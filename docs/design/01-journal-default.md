# Design 01: journal store as the default local layout

Status: implemented on `main` (see [STATUS](../STATUS.md)).

## Goal
A bare path or `file:///path` opens the journal store (one append-only
`journal.fj`, one `write` + `fdatasync` per commit) on Unix. v0.1.0
directories (one file per segment, "files" layout) keep working unchanged,
and `freshdb migrate` converts them. Windows and other non-Unix systems
keep the files layout.

Success metric: with the default URL, YCSB A c=1 and messaging p99 at
1k msgs/s match the `freshdb-journal` numbers in benchmarks.md (within run
noise), with no change for existing directories.

## Layout detection (`store.OpenLocal`)
1. `DIR/journal.fj` exists → journal.
2. Otherwise, if `DIR` holds any entry other than an empty `.tmp/`, it is a
   files-layout directory → files.
3. Otherwise (missing or empty) → journal where supported, else files.

Explicit schemes skip detection, but refuse a directory that already has
the other layout:
- `journal:///p` refuses a non-empty files-layout directory.
- `fs:///p` (files layout) refuses a directory that holds `journal.fj`.

**Invariant L1: a directory never holds two layouts.** Every way of opening
it either uses its existing layout or refuses. So no segment can be hidden
by opening the directory in the other layout.

## Migration (`freshdb migrate [-to journal|files] DIR`)
Offline: every process using `DIR` must be stopped. The files layout has no
lock to enforce this, so it is documented, not checked.

Steps:
1. `T = DIR.migrate-tmp`. Delete any old `T`; it is never trusted.
2. Copy every key of the source into a fresh store at `T` with
   `PutIfAbsent`.
3. Verify: the key sets are equal and every value is byte-equal. Then fsync
   the data and `T`'s parent directory.
4. Rename `T` → `DIR.migrated`. From here on there is a complete, verified
   copy.
5. Rename `DIR` → `DIR.bak-files` (or `.bak-journal`), then fsync the
   parent directory.
6. Rename `DIR.migrated` → `DIR`, then fsync the parent directory.

Resume after a crash: the command is idempotent and is decided by which
names exist.
- `DIR.migrated` exists and `DIR` exists → redo steps 5 and 6.
- `DIR.migrated` exists and `DIR` is missing → redo step 6.
- Otherwise → start from step 1.

The backup is never deleted automatically.

Invariants (model-checked in `spec/FreshMigrate.tla`):
- **M1:** after any prefix of steps and crashes, a complete copy of the
  data is reachable under `DIR` or `DIR.migrated`, and the original is
  reachable under `DIR` or the backup.
- **M2:** `DIR` never names an incomplete copy.
- **M3:** resuming always terminates with `DIR` in the target layout and
  the original in the backup.

## Failure modes
- Crash at any step: covered by M1–M3 and the resume rules.
- Disk full during the copy: the source is untouched; `T` is removed on
  the next run.
- Bit rot in the source: the journal checks record CRCs. Segment payloads
  carry their own checksums, which the DB checks when the log is opened.
  Migration copies bytes verbatim and compares them, so it neither hides
  nor adds corruption.
- Concurrent writer during migration: unsupported, and documented. A
  journal source is detected as busy if its `flock` cannot be taken.

## Test plan
- Unit tests:
  - detection, for each of rules 1–3, plus the explicit-scheme refusals;
  - Windows/other: the build-tagged default stays on files.
- Property test: random key/value sets survive files → journal → files
  migration byte-for-byte.
- Crash tests:
  - inject a failure after each migration step (a hook), then resume, then
    check the DB opens with identical log contents;
  - a kill -9 of a child `freshdb migrate`.
- DB level: the existing backend matrix runs on the default URL, and the
  root tests use the new default.
- Sim: unchanged (it runs on the Mem and Faulty stores).
- TLC: `FreshMigrate`.
- Benchmark: rerun YCSB A/B/C/F/D and msg with `-system freshdb`, now the
  default layout. Profile the journal run's peak RSS (1.81 GiB vs 0.99).

## Outcome (2026-10-28)

- YCSB with the default URL matches or beats the explicit `journal://` run
  in every cell (A c=1 43.6k vs 42.5k ops/s; full table in
  docs/benchmarks.md). Met.
- Streaming p99 at 1k msgs/s: not met as a stable number. Default and
  `journal://` back to back gave 43.1/25.2 ms vs 32.4/11.8 ms on the same
  code; a raw fdatasync probe shows ~1 s-periodic 30–75 ms disk stalls on
  this VM (`bench/results/2026-10-28-default-journal/ab-1k/`).
- Peak RSS 1.95 GiB (files layout 0.99). Profiled: 217 vs 141 B of heap
  per update, 64% in the KV lens's unbounded version history, and about
  2.8× more writes done by the journal run. Left for compaction/retention.
- Remaining: 8 MiB chunk growth (zero-fill + fsync under the write lock)
  is a ~15–40 ms stall every ~6 s at 1 MB/s; move it off the commit path.
