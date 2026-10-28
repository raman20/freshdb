# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Before 1.0, minor versions may
break APIs and the on-storage format.

## [Unreleased]

Work on the losses listed in `docs/benchmarks.md`; before/after numbers are
in its "Changes since v0.1.0" section.

### Changed
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

### Fixed
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
