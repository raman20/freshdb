# Production readiness audit

freshDB is **not production ready**. This file lists every known gap, by
severity, with its status. It is the source of truth for what "ready"
would mean; `docs/STATUS.md` tracks finished work items.

Written 2026-10-29 from a code read of `main` plus targeted probes
(each probe that found a bug is named). Update a row's status when the fix
lands, with the commit.

Severity, highest first:

- **S1 data loss / corruption**: acknowledged data can be lost, silently
  dropped, or the database can become unopenable.
- **S2 availability**: the database or server stops serving or stalls.
- **S3 operability**: operators cannot see, back up, restore or safely run it.
- **S4 security**: beyond what `docs/security.md` already lists.
- **S5 performance cliffs**: a workload shape makes it far slower or
  larger than the rest of the numbers suggest.
- **S6 compatibility**: upgrades, downgrades, API changes.

Status: **open**, **fixed** (commit), **mitigated** (partial fix, gap
named), **documented** (accepted limit, written down where users read).

## S1 — data loss / corruption

| id | gap | found by | status |
|---|---|---|---|
| S1.1 | A transaction whose frame exceeds `format.MaxFrameSize` (64 MiB) or 1M ops is written and acknowledged, but the decoder rejects it. Not the last segment: the DB no longer opens (`damaged segment … is not the tail`). Last segment: the next writable open "repairs" it, i.e. deletes the acknowledged commit. | probe: one 65 MiB `Put`, then reopen | **fixed** (item 13): `ErrTooLarge` before anything is written (`MaxTxBytes`, `MaxTxOps`); segments capped at 256 MiB; test `TestTooLargeTxRefused` |
| S1.2 | HTTP request bodies are read through `io.LimitReader(64 MiB)`: a larger value is silently truncated and stored. | code read (`server.body`) | **fixed**: `http.MaxBytesReader`, 413 (`--max-body`, default 96 MiB); `TestMaxBody` |
| S1.3 | Journal: `scan` stops at the first record that does not validate and treats the rest as a torn tail. A bit flip or bad sector in the middle hides every later record, and the next append overwrites them. | code read; probe: 1 flipped bit hid 3 of 4 objects | **fixed**: open tells torn tail / interrupted delete / corruption apart; corruption is `store.ErrCorrupt` (offset, key), file unchanged; `TestJournalMidFileCorruptionRefusesOpen` |
| S1.4 | Damage in the last segment of a log is "repaired" on a writable open (valid prefix kept, damaged copy in `quarantine/`, one log line). Every store's put is atomic (journal CRC per record, fs temp+link, S3), so such damage is corruption of acknowledged commits, not a torn write; it is dropped without failing. | code read (`repairTail`) | **fixed**: `ErrCorrupt` by default, segment untouched; repair only with `WithRepairTail` / `--repair-tail` |
| S1.5 | A segment header with an unknown version is reported as `ErrHeader`, the same as a torn header; on the last segment the writable open replaces it with an empty segment. A newer binary's segment would be deleted by an older one. | code read (`format.DecodeHeader`) | **fixed**: `format.ErrVersion`, never repaired; `TestNewerSegmentVersionRefused` |
| S1.6 | Journal `Get` reports a CRC mismatch as "not found", so a damaged object reads as missing (open fails with a misleading error; other callers may treat it as absent). | code read | **fixed**: `ErrCorrupt` for a live object failing its CRC; `TestJournalGetCorrupt` |
| S1.7 | Losing the newest segment object(s) of a log (deleted outside freshDB, restored from an older backup) is not detected: the log just ends earlier, and the next fence reuses the sequence number. | code read | **mitigated**: detected when the manifest's snapshot LSN or a child branch point lies past the end (`TestMissingNewestSegmentsDetected`); a log with no such reference still cannot tell |
| S1.8 | No power-loss test: crash tests kill the process (`kill -9`), which keeps the page cache. Unsynced writes reordering or vanishing is not exercised; fsync placement is argued in comments only. | review | **fixed for the journal store**: crash-point enumeration over traced writes/syncs (`store/powerloss_test.go`, design 12); fs:// and S3 not modelled |
| S1.9 | No full scrub: `verify` checks segments, chains, fencing and shred state, but not snapshots, bases, value chunks, ANN blobs or journal records that no index points to. | review | **fixed for local stores**: verify reads every object back and CRC-checks snapshots; on S3 only referenced objects (`scrubbed: false`) |
| S1.10 | Disk full (ENOSPC) on the journal/fs stores is not tested: whether a failed append leaves a record that later validates, and whether the writer recovers once space is freed. | review | fixed: fault-injected ENOSPC on journal and engine; halted writer stays halted until reopen (Compact used to revive it) — design 12 |
| S1.11 | Windows: directory fsync is a no-op (NTFS journals metadata); not power-loss tested. | code read | documented (README limits) |

## S2 — availability

| id | gap | status |
|---|---|---|
| S2.1 | One writer per database; no replicas, no HA. A writer crash means unavailability until a new process opens (fencing makes takeover safe, not automatic). | documented |
| S2.2 | Compaction and GC pause commits for their duration (0.87 s on the compaction bench; longer on big logs). | documented |
| S2.3 | `serve` shutdown: `ListenAndServe` returns as soon as `Shutdown` starts, so `db.Close` races in-flight requests; open SSE streams hold `Shutdown` for its whole 5 s timeout. | fixed (item 17): listen before open, drain (`/readyz` 503, streams `event: shutdown`, long polls return), wait for in-flight requests up to `--shutdown-timeout`, then close — design 16 |
| S2.4 | No `ReadTimeout`/`WriteTimeout`/`IdleTimeout` on the HTTP server (only `ReadHeaderTimeout`): slow clients hold goroutines. | fixed: per-request read (body) and write deadlines, renewed for streams/long polls, admin exempt; `IdleTimeout`; `TestTimeouts` |
| S2.5 | No limits on key, value or transaction size (see S1.1), number of ops, or JSON nesting in requests. | partly fixed: transaction size and ops (S1.1), request body (S1.2); key/name size: `MaxKeyBytes` 8 KiB (item 17); JSON nesting bounded by encoding/json (10 000) — fixed |
| S2.6 | Open reads every segment since the last base/snapshot; on S3 that is one GET per segment. | documented (benchmarks) |
| S2.7 | Two writers on one fs/journal directory: the second fences the first (by design); there is no lock-file message telling an operator which process holds it. | fixed: lease records (`lease/<epoch>.json`: version, command, pid, host); opener and fenced writer both name the other — design 16 |

## S3 — operability

| id | gap | status |
|---|---|---|
| S3.1 | No metrics (`/metrics`), no commit latency histograms, no cache/GC/compaction counters. | fixed: `GET /metrics` (Prometheus text, admin scope), `DB.Stats()` — design 14 |
| S3.2 | Logs are unstructured `log.Printf`; no request log, no slow-op log. | fixed: `--log-format json` (slog), `--access-log`, `--slow-op` (default 1 s) |
| S3.3 | `/healthz` only says the process is up; no `/readyz` that checks the store. | fixed: `/readyz` (closed / fenced / halted / store reachable; cached 1 s), `DB.Ping` |
| S3.4 | No pprof endpoint. | fixed: `--pprof`, admin scope, refused without auth |
| S3.5 | No backup or restore command; the documented method is copying the directory/bucket while stopped. No restore drill. | fixed: online `freshdb backup` (deletes held during the copy, incremental, digest record), `freshdb restore`, drill script + test — design 15 |
| S3.6 | Point-in-time recovery exists only as `branch create --at LSN` / `rewind`; no way to restore a database to a log position into a new location. | fixed: `restore --at LSN` (within the history the backup holds) |
| S3.7 | No long soak run; crash tests and the simulator run for seconds to minutes. | partly fixed (design 18): long deterministic simulator run recorded (bench/results/2026-10-29-soak/); 6 h real-time soak **in progress, result pending** — it ends ~02:00 IST 2026-10-30 and its last line (`SOAK OK:` / `SOAK FAIL:`) is in `/workspace/soak6h.out` on the test box (server log `/workspace/soak6h/server.log`); copy it to the results directory when it ends |

## S4 — security (beyond docs/security.md)

| id | gap | status |
|---|---|---|
| S4.1 | pprof/metrics must not leak to non-admin tokens once added. | fixed (item 15): admin scope; pprof refused without auth |
| S4.2 | No `govulncheck` in CI, no SBOM, release checksums not signed. | fixed in tooling: govulncheck in CI (0 reachable; 3 unreachable in x/net, x/crypto), SPDX SBOM and optional SSH signature in `scripts/release.sh`, release dry run with signature verify in CI. **No signing key published** |
| S4.5 | The CI cross-build loop ignored failures: Windows had not compiled since item 14 (`store.JournalVersion` in a unix-only file) while CI stayed green. | fixed: constant moved, loop fails on error |
| S4.3 | No threat model document; SECURITY.md has no response timeline. | fixed: docs/threat-model.md, timeline in SECURITY.md, docs/release-checklist.md |
| S4.4 | No per-token rate limits, no audit log of admin actions. | documented |

## S5 — performance cliffs

| id | gap | status |
|---|---|---|
| S5.1 | The key index (every key and version) lives in RAM: memory grows with keys, ~10× Pebble steady state (design 07). | open: disk-backed key index deferred (no < 1 h route that keeps the formats and tests honest); on the ROADMAP |
| S5.2 | HNSW build is single-threaded (16 min at 1M × 128-d); after a crash, vectors since the last clean close are re-inserted on open. | documented |
| S5.3 | The journal file only shrinks offline (`gc --shrink`). | documented |
| S5.4 | S3 request cost per commit and per open is not measured. | fixed (measured): docs/s3.md — 1 PUT per serial commit, 0.06 with 32 writers, ~2 GETs per segment at open |
| S5.5 | A snapshot is one object holding the whole key index; above 5 GiB a single S3 PUT fails (multipart is disabled). | open (not reached in tests; design 17) |

## S6 — compatibility

| id | gap | status |
|---|---|---|
| S6.1 | Version stamps exist for manifests (`format`) and segments, not for snapshots, bases, ANN blobs or the token file; no single "format version" a user can query. | fixed: `ErrVersion` for manifest, segment, base, journal; caches rebuilt; unversioned objects gated by the manifest format; `freshdb version` lists formats — design 13 |
| S6.2 | No golden files: an encoder change can alter the on-disk format without any test failing. | fixed: golden databases (v0.1.0 written by v0.1.0 code; main, main-compacted) with read and byte-identity tests |
| S6.3 | No written upgrade/downgrade policy; migration is tested from v0.1.0 directories only. | fixed: docs/compatibility.md (policy + measured matrix); found: v0.1.0 opens a journal directory as empty (documented, unfixable in v0.1.0) |
| S6.4 | No documented stable API surface, semver or deprecation policy. | fixed: docs/api-stability.md (surfaces labelled stable-track / experimental / internal); exported Go API snapshotted in `api/`, diffed in CI. HTTP/CLI not diffed |
| S6.5 | S3: only MinIO is tested; AWS S3 and R2 are untested (need credentials). Conditional-write behaviour of other S3-compatibles is unknown. | partly fixed: `freshdb s3check` probes an endpoint; fault injection against MinIO; tested-on matrix in docs/s3.md. AWS/R2/GCS still untested |

## Verdict per area (updated as items land)

| area | verdict |
|---|---|
| durability / corruption handling | journal store: corruption detected, never silently dropped; power-loss modelled. Disk full tested by fault injection (S1.10). Still open: fs:// and S3 not power-loss modelled, no salvage tool |
| formats / upgrades | stamps on every authoritative object, refusal tested, golden files for v0.1.0 and main, written policy. Not yet: frozen 1.0 policy, migration tooling for a future bump, old-binary column in CI |
| observability | metrics, readiness, structured/access/slow logs and pprof exist and are tested. Not yet: tracing, shipped dashboards/alert rules, a real Prometheus scrape in CI |
| backup / restore | online consistent backup, incremental, verified restore, PITR within retained history, drill (test + script). Not yet: S3-destination cost measured, scheduled backups, backup encryption beyond what the data already has |
| operational safety | graceful drain + shutdown, bind-before-open, per-request timeouts, key/body/tx limits, lease records naming writers, 409 for busy maintenance; tested with the real binary. Not yet: rate limits, global in-flight cap, request-level DB deadlines |
| soak / chaos | not ready |
| memory bound | not ready (RAM-bound by design today) |
| S3 | experimental. MinIO only (CI): fault injection (503, resets, lost replies) with no false fences or lost acks, request cost measured, `s3check` for other endpoints. Not yet: any real cloud provider, snapshots > 5 GiB |
| API stability / supply chain | policy written, Go API diff-gated, govulncheck + SBOM + checksums in CI. Not yet: 1.0 freeze, HTTP/CLI compatibility tests, a published signing key, reproducible builds |
