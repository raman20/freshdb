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
| S1.1 | A transaction whose frame exceeds `format.MaxFrameSize` (64 MiB) or 1M ops is written and acknowledged, but the decoder rejects it. Not the last segment: the DB no longer opens (`damaged segment … is not the tail`). Last segment: the next writable open "repairs" it, i.e. deletes the acknowledged commit. | probe: one 65 MiB `Put`, then reopen | open → item 13 |
| S1.2 | HTTP request bodies are read through `io.LimitReader(64 MiB)`: a larger value is silently truncated and stored. | code read (`server.body`) | open → item 13 |
| S1.3 | Journal: `scan` stops at the first record that does not validate and treats the rest as a torn tail. A bit flip or bad sector in the middle hides every later record, and the next append overwrites them. | code read | open → item 13 |
| S1.4 | Damage in the last segment of a log is "repaired" on a writable open (valid prefix kept, damaged copy in `quarantine/`, one log line). Every store's put is atomic (journal CRC per record, fs temp+link, S3), so such damage is corruption of acknowledged commits, not a torn write; it is dropped without failing. | code read (`repairTail`) | open → item 13 |
| S1.5 | A segment header with an unknown version is reported as `ErrHeader`, the same as a torn header; on the last segment the writable open replaces it with an empty segment. A newer binary's segment would be deleted by an older one. | code read (`format.DecodeHeader`) | open → items 13/14 |
| S1.6 | Journal `Get` reports a CRC mismatch as "not found", so a damaged object reads as missing (open fails with a misleading error; other callers may treat it as absent). | code read | open → item 13 |
| S1.7 | Losing the newest segment object(s) of a log (deleted outside freshDB, restored from an older backup) is not detected: the log just ends earlier, and the next fence reuses the sequence number. | code read | open → item 13 (cross-checks) |
| S1.8 | No power-loss test: crash tests kill the process (`kill -9`), which keeps the page cache. Unsynced writes reordering or vanishing is not exercised; fsync placement is argued in comments only. | review | open → item 13 |
| S1.9 | No full scrub: `verify` checks segments, chains, fencing and shred state, but not snapshots, bases, value chunks, ANN blobs or journal records that no index points to. | review | open → item 13 |
| S1.10 | Disk full (ENOSPC) on the journal/fs stores is not tested: whether a failed append leaves a record that later validates, and whether the writer recovers once space is freed. | review | open → item 17 |
| S1.11 | Windows: directory fsync is a no-op (NTFS journals metadata); not power-loss tested. | code read | documented (README limits) |

## S2 — availability

| id | gap | status |
|---|---|---|
| S2.1 | One writer per database; no replicas, no HA. A writer crash means unavailability until a new process opens (fencing makes takeover safe, not automatic). | documented |
| S2.2 | Compaction and GC pause commits for their duration (0.87 s on the compaction bench; longer on big logs). | documented |
| S2.3 | `serve` shutdown: `ListenAndServe` returns as soon as `Shutdown` starts, so `db.Close` races in-flight requests; open SSE streams hold `Shutdown` for its whole 5 s timeout. | open → item 17 |
| S2.4 | No `ReadTimeout`/`WriteTimeout`/`IdleTimeout` on the HTTP server (only `ReadHeaderTimeout`): slow clients hold goroutines. | open → item 17 |
| S2.5 | No limits on key, value or transaction size (see S1.1), number of ops, or JSON nesting in requests. | open → item 17 |
| S2.6 | Open reads every segment since the last base/snapshot; on S3 that is one GET per segment. | documented (benchmarks) |
| S2.7 | Two writers on one fs/journal directory: the second fences the first (by design); there is no lock-file message telling an operator which process holds it. | open → item 17 |

## S3 — operability

| id | gap | status |
|---|---|---|
| S3.1 | No metrics (`/metrics`), no commit latency histograms, no cache/GC/compaction counters. | open → item 15 |
| S3.2 | Logs are unstructured `log.Printf`; no request log, no slow-op log. | open → item 15 |
| S3.3 | `/healthz` only says the process is up; no `/readyz` that checks the store. | open → item 15 |
| S3.4 | No pprof endpoint. | open → item 15 |
| S3.5 | No backup or restore command; the documented method is copying the directory/bucket while stopped. No restore drill. | open → item 16 |
| S3.6 | Point-in-time recovery exists only as `branch create --at LSN` / `rewind`; no way to restore a database to a log position into a new location. | open → item 16 |
| S3.7 | No long soak run; crash tests and the simulator run for seconds to minutes. | open → item 18 |

## S4 — security (beyond docs/security.md)

| id | gap | status |
|---|---|---|
| S4.1 | pprof/metrics must not leak to non-admin tokens once added. | open → item 15 |
| S4.2 | No `govulncheck` in CI, no SBOM, release checksums not signed. | open → item 21 |
| S4.3 | No threat model document; SECURITY.md has no response timeline. | open → item 21 |
| S4.4 | No per-token rate limits, no audit log of admin actions. | documented |

## S5 — performance cliffs

| id | gap | status |
|---|---|---|
| S5.1 | The key index (every key and version) lives in RAM: memory grows with keys, ~10× Pebble steady state (design 07). | open → item 19 |
| S5.2 | HNSW build is single-threaded (16 min at 1M × 128-d); after a crash, vectors since the last clean close are re-inserted on open. | documented |
| S5.3 | The journal file only shrinks offline (`gc --shrink`). | documented |
| S5.4 | S3 request cost per commit and per open is not measured. | open → item 20 |

## S6 — compatibility

| id | gap | status |
|---|---|---|
| S6.1 | Version stamps exist for manifests (`format`) and segments, not for snapshots, bases, ANN blobs or the token file; no single "format version" a user can query. | open → item 14 |
| S6.2 | No golden files: an encoder change can alter the on-disk format without any test failing. | open → item 14 |
| S6.3 | No written upgrade/downgrade policy; migration is tested from v0.1.0 directories only. | open → item 14 |
| S6.4 | No documented stable API surface, semver or deprecation policy. | open → item 21 |
| S6.5 | S3: only MinIO is tested; AWS S3 and R2 are untested (need credentials). Conditional-write behaviour of other S3-compatibles is unknown. | open → item 20 (`s3check`) |

## Verdict per area (updated as items land)

| area | verdict |
|---|---|
| durability / corruption handling | not ready (S1.1–S1.10 open) |
| formats / upgrades | not ready |
| observability | not ready |
| backup / restore | not ready |
| operational safety | not ready |
| soak / chaos | not ready |
| memory bound | not ready (RAM-bound by design today) |
| S3 | MinIO only |
| API stability / supply chain | not ready |
