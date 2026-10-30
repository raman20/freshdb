# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Before 1.0, minor versions may
break APIs and the on-storage format.

## [Unreleased]

Work on the losses listed in `docs/benchmarks.md`; before/after numbers are
in its "Changes since v0.1.0" section.

### Changed (compaction pause)

- Compaction and GC delete due garbage before pausing the writers and
  after resuming them, instead of inside the pause. With ~2,700 objects
  deleted on a journal store, the commit pause went from 280-317 ms to
  11-14 ms (`BenchmarkCompactSweepPause`); the whole run takes ~15 % longer
  as deletes share the journal with commits. Branch operations still wait
  for the whole run.
- `freshdb_compaction_pause_seconds` (`Stats.CompactionPause`) now
  measures the pause itself; it used to time the whole run.
- A backup's deletion hold now waits for deletes already in flight
  (a sweep running alongside could otherwise remove an object the
  backup had just listed).
- Old manifests are deleted after the final sweep, so `KeepManifests`
  counts the sweep's own manifest.

### Fixed (component D review: documents)

- `Find` with an index now includes the transaction's own writes and is
  part of its read set (a concurrent matching insert conflicts), like the
  unindexed scan.
- `Find` compares numbers by exact value: a stored `1.0` matches `1`, and
  `GET /v1/docs/{c}?value=` no longer rounds large integers through
  float64. New `lens.DocField`.
- Shredding a subject no longer walks every indexed document.
- A document that is not a JSON object, or an unencodable `Find` value,
  is now `ErrBadValue` (HTTP 400; was a plain error, HTTP 500).

### Fixed (soak investigation, bench/results/2026-10-30-soak-investigation)

- A caller cancel (e.g. an HTTP client timing out on `/v1/compact`) during
  a manifest commit no longer halts the database until restart.
- Journal open scans through a window that stops at holes and no longer
  keeps a map of every deleted key: on the 6 h soak's database, open went
  from 54-59 s / 4.45 GB peak to 6-12 s / 2.4 GB.

### Added (component C review: topics)

- Topic retention: `CompactOptions.Topics` / `GCOptions.Topics`
  (`TopicRetention{MaxAge, MaxMessages, IgnoreGroups}`), `--topic-max-age`,
  `--topic-max-messages`, `--topic-ignore-groups` on `compact`, `gc` and
  `serve`, and the same query parameters on `/v1/compact` and `/v1/gc`.
  Compaction drops the oldest messages of each topic from the new base,
  never past the smallest committed cursor of a consumer group unless
  told to (model: `spec/FreshRetain.tla`). Offsets do not change;
  `TopicTx.Start` reports the first kept offset. Bases of trimmed topics
  use a new `TOPIC2` lens encoding; untrimmed ones are byte-identical to
  before. Off by default.

### Changed (component C review: topics)

- The soak driver (`bench/cmd/freshdb-soak`) pages topic reads through the
  1000-message HTTP cap instead of asking for `max=100000`.

- Shredding a subject touches only that subject's topic messages (an
  index in the topic lens) instead of scanning every message.
- HTTP `GET /v1/topics/{t}` without `max` (or with `max=0`) returns at
  most 1000 messages instead of the whole topic, and `.../next?max=0`
  fetches and acks at most 1000 instead of the whole topic; larger `max`
  values are capped at 1000.

### Fixed (component C review: topics)

- A topic snapshot or base with topics out of order or repeated, LSNs
  going back, or a redacted message with a value or no subject loaded
  silently (wrong offsets or reads); now `ErrBadSnapshot`.
- `Group.Ack` of an earlier message after a later one moved the cursor
  back (redelivery); it now never moves back (`Seek` still does). Acking
  a message of another topic moved this group's cursor; now `ErrBadName`.
- A group name containing NUL could address another group's cursor;
  now `ErrBadName` (HTTP 400). A cursor value that is not 8 bytes read as
  0 (redelivery from the start); now `ErrCorrupt`.

### Changed (component B review: KV)

- The KV lens keeps all but its newest versions in immutable compressed
  runs (front-coded keys, hash-indexed) instead of one map: live heap per
  key 172 → 30 B after a snapshot load at 10M keys (61–64 B right after
  ingest, with merge buffers), a 100-key prefix `Scan` no longer walks
  every key (866 ms → 0.05–0.07 ms at 10M), snapshot load 4–5.5× faster.
  Point `Get` is 1.1–1.3× slower and lens ingest 1.7–1.9× slower (merges);
  numbers and method in `docs/components/kv.md`. Snapshot bytes and
  digests are unchanged.

### Fixed (component B review: KV)

- A KV snapshot or base with a key lacking the space separator loaded and
  then panicked `Latest`; keys out of order or repeated silently replaced
  each other; versions out of order broke reads; non-canonical bools,
  nil-marked strings and a needless KV3 tag were accepted. All are now
  `ErrBadSnapshot` (snapshot: replay instead; base: `ErrCorrupt`). Found
  by the new `FuzzLoadKV`, which also checks accepted bytes re-encode
  identically.
- `DropSubject` (every shred, and every open after one) walked every key;
  it now rewrites only runs that hold the subject.

### Fixed (component A review: the log)

- S3 `Get` now takes the object size from the GET response itself (one
  request; it used to `Stat` after reading, which reported size 0 and made
  every S3 read fail with "short read"). A body cut short by the network is
  `store.ErrShortRead`, never a silently short object.
- A checksummed but inconsistent manifest (a null log entry, a parent
  cycle, a ref to a missing log) is `ErrCorrupt` at open; it used to panic
  or recurse without end (found by the new `FuzzOpenManifest`).
- `--repair-tail` no longer deletes and re-creates the damaged segment
  (a crash in between lost its valid prefix; a zombie writer could fill the
  freed slot). It writes a create-only repair note `log/<id>/<seq>.repair`
  and leaves the segment as is. Binaries before this refuse such a log with
  `ErrCorrupt` (they see a damaged non-tail segment); nothing is lost.
- A damaged tail was misread as mid-log damage when any other object sat
  under the log's prefix.
- An ambiguous create (412 whose read-back failed) was reported as fenced
  ("not committed"); it is now `ErrUnknownOutcome`. A manifest found below
  a newer one after its create is fenced and `ErrUnknownOutcome`.
- A failed open leaked the writer goroutines of branches opened before the
  failure and the value cache.
- Opening a filesystem store deleted every temp file, including another
  process's put in flight; only temp files older than an hour go now.
- Segment headers with non-zero reserved bytes are rejected.
- Recovery read the log as "every segment the LIST shows", so a stale
  LIST (S3 list-after-write lag) could shorten the log and fail open with
  "log ends before snapshot". Segments are now read by sequence number until
  one is absent; the LIST only checks for holes.
- Read faults: a silently short read could decode as a clean shorter
  segment (`store.ErrShortRead`, S3 size check); damage is believed only
  after 3 identical reads, and a well-formed object naming another key only
  after 6, else the read is retried (transient) instead of reported as
  corruption or repaired.
- A truncated `keys/_sign` seed panicked (ed25519); now `ErrCorrupt`.
- Snapshot v2 binds a snapshot to its log id, LSN and hash-chain head: a
  snapshot copied from another log or a diverged history was loaded.

### Performance
- Segment-index checkpoints (`log/<id>/<seq>.idx`, written with each
  snapshot): open recovers a log by reading only the segments after the
  newest checkpoint. Replay keeps 8 segment reads in flight
  (`WithReadAhead`). 100 segments at 2 ms per GET, with snapshots: open
  480 ms → 64-68 ms; without: ~500 → ~290 ms (docs/operations.md).
- `WithCommitPolicy(CommitPolicy{MaxDelay, TargetBytes})`: a group commit
  may wait to grow; off by default (docs/operations.md "Commit policy").
- Encoding a frame writes into the segment buffer (7 allocations to 0),
  the hash chain step no longer allocates, and decoding a segment interns
  space names (-32% allocations).

### Changed (breaking)
- Keys and topic/collection/subject names over 8 KiB (`MaxKeyBytes`) are
  refused with `ErrTooLarge` (413); existing larger keys stay readable.
- A damaged final log segment now makes open fail with `ErrCorrupt` instead
  of being truncated to its valid prefix. `WithRepairTail(true)` /
  `--repair-tail` keeps the valid prefix (now with a repair note, see above).

### Fixed
- Windows builds (broken since the format-version work: a constant lived
  in a unix-only file); the CI cross-build loop now fails on an error.
- Disk full: a commit's storage error now wraps its cause (`errors.Is(err,
  syscall.ENOSPC)`), and `Compact`/`GC` refuse to run on a writer halted by
  a storage error instead of silently replacing it (a commit after the halt
  could succeed without the reopen that learns the failed commit's outcome).
- Starting a compaction or GC while one runs returned HTTP 500; it is now
  `ErrBusy` / HTTP 409 (found by the soak harness).
- Backup: a data race on the report counters (found by the race suite).

### Added
- docs/api-stability.md (semver, stable-track vs experimental surfaces,
  deprecation), docs/threat-model.md, docs/release-checklist.md; SECURITY.md
  response timeline. CI: govulncheck, exported-API diff gate (`api/`),
  release dry run (SPDX SBOM, checksums, SSH signature verify).
- `freshdb s3check URL` (and `store.S3Check`): probes an S3 endpoint for
  the guarantees freshDB needs (create-only and concurrent creates, read-
  and list-after-write, ranged GET, delete visibility, large single PUT).
  S3 fault-injection and request-cost tests; docs/s3.md.
- Backup and restore: `freshdb backup DEST` / `POST /v1/backup` /
  `DB.Backup` (online, consistent, incremental, ends with a digest record),
  `freshdb restore BACKUP --db NEW [--at LSN]` / `Restore` (verified copy,
  point-in-time rewind), `scripts/restore-drill.sh`. `S3` stores gain
  `Keys` (recursive listing).
- Operational safety: `serve` drains on SIGTERM (`/readyz` 503, SSE
  `event: shutdown`, long polls return), waits for in-flight requests up
  to `--shutdown-timeout` and closes cleanly; binds before opening (a busy
  port no longer fences the running server); per-request
  `--read-timeout`/`--write-timeout`, `IdleTimeout`; `MaxKeyBytes` (8 KiB
  keys and names, `ErrTooLarge`); lease records (`WithHolder`,
  `lease/<epoch>.json`) so a takeover and a fenced writer name each other;
  `server.Options.Drain/ReadTimeout/WriteTimeout`.
- `bench/cmd/freshdb-soak`: real-time soak/chaos harness (mixed workload,
  periodic kill -9, concurrent compaction/GC/backup, invariant checker).
- Simulator: `-steps` / `FRESHDB_SIM_STEPS` and `FRESHDB_SIM_FIRST` for long
  sharded runs; after 100 failed recovery opens under a read-fault storm the
  storm passes (counted as `calm opens`) instead of failing the seed.
- Observability: `GET /metrics` (Prometheus text format, admin scope:
  commits, conflicts, errors, commit/segment/compaction latency histograms,
  value-cache hits, HTTP requests by route and code, runtime gauges),
  `GET /readyz` (closed / fenced / halted / store reachable),
  `--log-format json`, `--access-log`, `--slow-op` (default 1 s), `--pprof`
  (admin only, needs auth). Go API: `DB.Stats`, `DB.Healthy`, `DB.Ping`,
  `LatencyHistogram`; `server.Options` gains `Version`, `Logger`,
  `AccessLog`, `SlowOp`, `Pprof`.
- **Format versioning** (docs/compatibility.md, design 13). `ErrVersion`
  for a manifest, segment, base or journal file written by a newer freshdb
  (a newer base or journal used to be reported as corruption); `freshdb
  version` lists the formats; `verify` reports the manifest format. Golden
  databases under `testdata/golden` (one written by the v0.1.0 code) are
  read by every test run, and the current format is pinned byte for byte.
- **Durability hardening** (docs/design/12-durability.md). Transactions over
  64 MiB encoded or 2^20 ops fail with `ErrTooLarge` (`MaxTxBytes`,
  `MaxTxOps`) instead of being acknowledged and then breaking the next open;
  group-commit segments stop at 256 MiB. `serve --max-body` (default 96 MiB)
  answers 413 instead of truncating bodies at 64 MiB. The journal store
  tells a torn append, an interrupted delete and corruption apart on open;
  corruption is `store.ErrCorrupt` naming the offset and key (it used to hide
  every later record). A live journal object failing its CRC reads as
  `ErrCorrupt`, not "not found". A segment of an unknown version is
  `format.ErrVersion`. A log ending before its snapshot is `ErrCorrupt`.
  `verify` also CRC-checks snapshots and reads every object of a local store
  back (`scrubbed`, `objects`, `bytes`, `warnings`). Power-loss
  crash-point test for the journal (`store/powerloss_test.go`).
- **Scoped tokens, rotation, certificate reload, connection caps.**
  `freshdb serve --token-file F` takes lines `SCOPE TOKEN [name=N]
  [kv:PREFIX*|topic:T|docs:C|vectors:C ...]` with scopes read, write and
  admin (a bare `TOKEN` line is admin, as before); `/v1/tx` checks every
  op. The token file is re-read within 2 s of a change and the TLS
  certificate within 10 s, both on SIGHUP; a bad file keeps the old one,
  and an SSE stream of a removed token ends. `--max-streams` (1024) and
  `--max-waiters` (4096) cap SSE streams and long-polls (503 +
  `Retry-After`). Unknown token: 401; not allowed: 403. Go API:
  `server.Options{Auth, MaxStreams, MaxWaiters}`, `server.LoadTokenFile`,
  `server.LoadCerts`. Tokens in `--token-file` must now be at least 8
  characters.
- **GC with retention.** `(*DB).GC(ctx, GCOptions{KeepFor, KeepCommits,
  KeepReflog, KeepManifests, Grace})`, `freshdb gc [--keep-for 24h]
  [--keep-commits N] [--keep-reflog 64] [--keep-manifests 16] [--shrink]`,
  `POST /v1/gc` and `freshdb serve --gc-every D`. Compacts each log at a
  segment boundary that keeps the requested history window (time travel,
  rewind and branch points inside it still work), drops logs no branch or
  kept reflog entry can reach, prunes the reflog and deletes old manifests.
  Deleting manifests needed a new fencing rule (a writer re-lists after its
  create-only put and backs out if a newer manifest exists), checked in
  `spec/FreshGC.tla` with mutants that fail without it. `--shrink` rewrites
  the journal file offline. On `freshdb-bench gc` (20 churn cycles) disk was
  441 MiB with compact only, 38.5 MiB with GC, 32 MiB with GC + shrink.
- **`DeleteBranch`**, `freshdb branch delete NAME`,
  `DELETE /v1/branches/{b}` (main cannot be deleted; the log stays
  readable via the reflog until GC prunes it).
- `store.ShrinkJournal(dir)`; every journal now holds a shared flock on
  `journal.lock` for its lifetime so a shrink cannot run under it.
- **Compaction.** `(*DB).Compact`, `freshdb compact [--grace]`,
  `POST /v1/compact?grace=` and `freshdb serve --compact-every D
  --compact-grace G`. Each log gets a base image at its head (the latest
  live KV version per key; all topic messages, redacted ones as
  placeholders; large values in ~64 MiB chunk objects); one manifest
  commit drops the parents, and the old segments, snapshots and bases are
  deleted after a grace period (default 5 min via the API, 0 in the CLI).
  Commits pause for the duration. Modelled in `spec/FreshCompact.tla`.
  On the compaction bench (488 MiB written, 9.8 MiB live) disk fell from
  504 to 34 MiB and open from 1.6 s to 30 ms.
- **Opt-in HNSW vector index.** `(*DB).EnableANN(ctx, coll, ANNOptions)`,
  `freshdb vec ann COLL [--m --ef-construction --ef-search]`,
  `POST /v1/vectors/{c}/ann`. The declaration is stored in the manifest;
  the graph lives in RAM, is built from the lens on open (live branches
  only; time travel stays exact) and is excluded from the digest, so
  `lens == fold(log)` still holds. Search uses it from 4,096 vectors up and
  falls back to the exact scan if it returns fewer than k hits;
  `SearchExact` / `?exact=true` / `--exact` force the exact scan and
  `SearchANN` / `ef` set the beam. At 100k × 128-d (clustered) recall@10 is
  0.93 / 0.988 / 0.998 at ef 32 / 64 / 128 with 6.1k / 3.3k / 2.1k QPS on
  one client (exact: 910 QPS); build and reopen take ~23 s. At 1M recall
  is 0.67 / 0.80 / 0.92 (exact: 64 QPS, HNSW ef 128: 676) and build or
  reopen takes ~17 min, because the graph is not persisted.
- **HNSW graphs are persisted.** Saved on `Close` (`ann/<log>/…` objects,
  a cache outside the manifest and digests), validated node by node
  against the vectors on open, then caught up by inserting newer vectors;
  compaction hands graphs over in memory, shred deletes them, GC drops
  those of dropped logs. Modelled in `spec/FreshANN.tla`. Reopen: 100k
  23 s → 0.6 s, 1M ~17 min → 10 s; after a crash with 10k newer vectors
  at 1M, 22 s. Default `EfSearch` is now 128 (was 64): 1M recall@10 0.80
  → 0.92 (QPS c=1 1,252 → 725); a saved graph is kept when only EfSearch
  changes.
- **Server auth and TLS.** `freshdb serve --token T` (or `--token-file`,
  `FRESHDB_TOKEN`) requires `Authorization: Bearer T` on every request but
  `GET /healthz` (new, always open); `--tls-cert` / `--tls-key` serve HTTPS
  (TLS 1.2+). `serve` now **refuses a non-loopback `--listen` without a
  token** unless `--insecure-no-auth` is given. The CLI sends
  `--token` / `FRESHDB_TOKEN` with `--addr`. Go: `server.New(db,
  server.Options{Token})`. A token adds ~0.1 µs per in-process request.
- **Key provider.** `KeyProvider`, `WithKeyProvider`, `KeyFromEnv`,
  `KeyFromFile`, `ParseKey`; CLI `--kek-file` and `FRESHDB_KEK` (hex or
  base64). Open writes `keys/_kekcheck` and returns `ErrWrongKEK` for a
  wrong key, even on a database with no DEKs. Dev-key mode (no key
  supplied) is unchanged but now logs a warning once per process. No rekey.
- **Push consumers.** `(*DB).WaitHead(ctx, branch, seen)` blocks until a
  branch head moves (commit, rewind or promote), and `(*DB).Head(branch)`
  returns the current head. HTTP: `?wait=30s` (max 60 s) long-polls
  `GET /v1/topics/{t}` and `POST /v1/topics/{t}/groups/{g}/next`, and
  `GET /v1/topics/{t}/stream?from=` is a Server-Sent Events stream
  (`id:` = offset, resumes from `Last-Event-ID`, `: ping` every 15 s).
  `freshdb sub --follow` long-polls instead of sleeping 500 ms. TLA+ model
  `FreshNotify` checks there is no lost wake-up. At 1k msgs/s, e2e p99 is
  1–15 ms with `WaitHead` vs 99 ms when polling every 100 ms and 495 ms
  for the old 500 ms `--follow` sleep; p50 0.13 ms vs 0.24 ms for a
  200 µs spin.
- `ErrCompacted` (HTTP 410) for reads, branches and rewinds below a
  compacted log's base. Time travel below the base is gone by design.
- Manifest format 2 (written after the first compaction; v0.1.0 binaries
  cannot open a compacted database). KV snapshots use `KV3` only when a
  version points into a chunk object.

### Changed
- **Journal preallocation moved off the commit path.** The next 8 MiB
  chunk is zero-filled by a background goroutine in 256 KiB steps under the
  writer lock, each written out with `sync_file_range`. On the commit bench
  commits over 10 ms fell from 13–16 to 0–3 per 30k (c=1) and p99.9 at c=16
  from 15–17 ms to 2.6–9.1 ms; p99 is about 2× higher and p99.9 at c=1 went
  0.6 → 1.2 ms (`docs/design/09-journal-prealloc.md`).
- **TLA+ tools pinned to the stable v1.7.4 release.** `v1.8.0` is a
  prerelease whose jar is rebuilt nightly, so its checksum drifted between
  CI (cached) and fresh downloads. `spec/get_tla.sh` downloads and verifies
  the pinned jar for CI and local runs.
- **Memory (item 7).** The KV lens stores each version in 40 bytes
  (was 72) and a key's first version in the map slot itself; the log
  segment index keeps one chain hash (the tip's) instead of one per
  segment (64 → 24 B per segment). The default value cache is now 1/16 of
  the cgroup v2 memory limit (else MemTotal), clamped to 16–256 MiB (still
  256 MiB on machines with ≥ 4 GiB). YCSB 1M × 1 KiB peak RSS 2.05 →
  1.55 GiB, live heap 815 → 576 MiB, reads unchanged; see
  `docs/design/07-memory.md`. `freshdb-bench ycsb` records RSS/anon/file,
  Go heap and GC goal per phase, data-dir page cache, and gains
  `-memprofile` and `-compact-between`.
- **Memory: values are read from the log instead of being held in RAM.**
  The KV lens keeps values larger than `InlineMax` (default 64 B) as a
  position (offset, length, CRC-32C) in their immutable segment, and the
  log index keeps one small record per segment instead of every frame.
  Values are read with ranged reads (`pread` with an fd cache on local
  disk, `Range` GETs on S3) through a sharded LRU cache (`ValueCacheBytes`,
  default 64 MiB) and checked against the CRC (`ErrCorrupt` on mismatch).
  Peak RSS for the 1M × 1 KiB YCSB run fell from 7.1 GiB to about 1 GiB (1.06 and 0.99 GiB in two runs). Cache
  misses make point reads slower than in v0.1.0 (see the benchmarks).
- KV snapshots use a new `KV2` format that records the inline limit; `KV1`
  snapshots, or ones written with a different limit, are ignored and the
  log is replayed instead.
- **Vector search** stores unit vectors in one contiguous `float32` slab
  per collection and dimension, uses an unrolled dot product and a bounded
  top-k heap (no per-vector allocation), and splits large scans across
  cores. Results are unchanged: still exact, ordered by (score, id).
- **Hot-key transactions cannot starve.** Within a group commit,
  transactions that already lost conflicts are validated first; retries
  back off with jitter; and after 3 lost conflicts the transaction body
  runs on the branch's writer at the applied head, so it commits on that
  attempt. The log format and the committed operations are unchanged.
  Such a body must not block or call `Update` on the same DB. Several
  escalated transactions can share one group commit; one that conflicts
  with an earlier write in the batch runs in the next batch.
- The writer trims its conflict-history window in amortized steps instead
  of copying it on every commit.

- **New local directories use the journal layout on Unix.** A bare path or
  `file:///path` now opens an existing directory in the layout it already
  has (`store.OpenLocal` / `store.DetectLayout`), so v0.1.0 directories are
  unchanged. A new or empty directory gets the journal (`journal.fj`) on
  Linux/macOS/BSD and the files layout on Windows. `fs:///path` forces the
  files layout, and both explicit schemes refuse a directory holding the
  other layout.
- `DB.Close` now closes a store that `Open` created from a URL (journal
  file handles, FS read-handle cache).
- **Value cache moved off the Go heap and default raised to 256 MiB.** The
  cache is now 64 byte-ring shards in one anonymous `mmap` arena (heap on
  non-Unix), FIFO with a second chance for entries hit near eviction. The
  GC does not scan or count it, so RSS grows by about the cache size.
  C-only probe at 64 clients: 1.95M → 2.39M ops/s, peak RSS 897 MB →
  1.15 GB. `WithValueCacheBytes(n)` still sets the size; negative disables.
- Read path: no exclusive `DB` lock per read (atomic snapshot of branch
  writers), read-only transactions allocate no maps.

### Added
- `freshdb migrate [--to journal|files]` (`store.Migrate`): offline,
  crash-safe layout conversion. It copies, verifies byte-for-byte, fsyncs,
  renames into place and keeps `DIR.bak-<layout>`. Running it again after
  a crash finishes the job. The protocol is model-checked in
  `spec/FreshMigrate.tla` and tested by injecting a stop after every step
  and by SIGKILLing a migrating child process.
- `store.Walker` / `store.Keys` (enumerate every object of FS, Journal,
  Mem); `Journal.Sync`, `Journal.Close`, `FS.Close`.
- `freshdb-bench`: `freshdb-files` system (the v0.1.0 layout); `freshdb`
  now benchmarks the default layout.
- `WithInlineMax`, `WithValueCacheBytes` options.
- `store.RangeReader` / `store.GetRange` (ranged reads for FS, S3, Mem).
- **Journal store** (`journal:///path`, `store.NewJournal`, Unix
  only). Segments are records in one preallocated append-only file
  (`journal.fj`), so a durable commit is one `write` plus one `fdatasync`
  instead of file create + fsync + rename + directory fsync. Records carry
  a salted CRC-32C, which stops a torn tail or stale bytes from being read
  as data. Instances on one file coordinate with `flock`. Delete appends a
  tombstone and then punches a hole. It has its own conformance,
  reopen/torn-tail and kill -9 crash tests. It is now the default for new
  local directories on Unix (see Changed).
- `freshdb-bench`: `freshdb-journal` system for `ycsb` and `msg`.
- `freshdb-bench ycsb`: `-cpuprofile` (closed-loop phase only) and
  `-value-cache` (recorded in the result params).

### Fixed
- **Returned values no longer alias internal memory** (present since
  v0.1.0). `Get`, `Scan` and `Topic.Read` could return slices shared with
  the value cache or the in-memory lens, and `Put`/`Publish` kept the
  caller's slice; a caller writing into either could change what later
  reads returned. Values are now copied in both directions.
- The FS store's fd cache is sized from `RLIMIT_NOFILE` (clamped) through
  build-tagged helpers, so Windows builds again.

## [0.1.0] - 2026-10-27 — first public prototype (pre-release)

**Not for production use.** See the status section in the README.

### Added
- One append-only, hash-chained log on object storage (S3-compatible with
  `If-None-Match`, or a local directory). One immutable segment per group
  commit; create-only manifests; epoch fencing with no clocks.
- Lenses over the log: KV (MVCC history), topics with consumer groups,
  JSON documents with secondary indexes (can be added later), and vectors
  (cosine, brute force).
- Serializable single-writer transactions (OCC) across all lenses, including
  exactly-once "consume, write, ack" in one transaction.
- Branches (zero-copy), rewind with reflog, promote, and time travel to any LSN.
- Crypto-shredding per subject: per-subject AES-GCM keys, a shred marker, an
  audit entry and an ed25519-signed certificate. Shredded data is unreadable
  on all branches and under time travel. `verify` checks CRCs, hash chains,
  fencing and shred state.
- `freshdb` single binary: CLI and JSON/HTTP server (`freshdb serve`), configured
  by one URL.
- Correctness tooling:
  - TLA+ specs (FreshLog, FreshTxn, FreshBranch, FreshShred), checked by TLC.
  - A deterministic simulator with fault injection and a reference model.
  - rapid property tests and a kill -9 crash test.
  - Fuzzing of the decoders.
  - MinIO S3 tests.
- Agent demo (`examples/agent-demo.sh`) and Go API tour (`examples/tour`).
- Benchmark suite in [`bench/`](bench) (`freshdb-bench`): YCSB A/B/C/D/F
  against Badger, Pebble, bbolt and SQLite on the same box; streaming against
  embedded NATS JetStream; OCC contention; the multi-lens transaction; vector
  recall vs QPS; and S3 request counts and estimated request cost.
  Coordinated-omission-aware (HdrHistogram) latency. See
  [docs/benchmarks.md](docs/benchmarks.md).

### Changed
- KV/topic snapshots are now **off by default** (`WithSnapshotEvery(n)` turns
  them on). The benchmarks showed they are written synchronously inside the
  commit path and copy the whole lens. With 1M × 1 KB keys that is about 1 GB
  every 1,000 commits. They also don't speed up open yet, because recovery
  still reads every segment.

### Fixed during development (found by the tooling)
- TLC found that a slow opener could write an older-epoch fence after a newer
  one, which bricked the log on the next open. Recovery now stops with
  `ErrFenced` when it sees a newer epoch.
- Fuzzing found that the payload decoder accepted non-minimal varints. Payloads
  are now canonical.
- Property tests found that the vector lens kept empty collections after
  `DropSubject`. Its state is now canonical.
- The simulator found that a delayed DEK create could land after a shred.
  Open and `verify` now sweep such keys.

[Unreleased]: https://github.com/raman20/freshdb/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/raman20/freshdb/releases/tag/v0.1.0
