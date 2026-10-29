# Component A: the log

Review of the log, completed 2026-10-29 (IST). It covers segments, manifests
and epochs, recovery, group commit, repair notes, snapshots v2 and
segment-index checkpoints. Code: `format/`, `log.go`, `writer.go`,
`manifest.go`, `readstable.go`, `snapshot.go`, `checkpoint.go`, and the
log-facing parts of `store/` and `db.go`. This review does not claim the
log is production ready. "Verdict" below says what it does claim.

## Contract

1. **Durability.** A commit is acknowledged only after its segment's exact
   bytes have been read back from the store. Every acknowledged commit is
   in every later recovery, on every backend, under the fault model in
   docs/durability.md.
2. **Order and integrity.** LSNs are dense, and each frame names its
   predecessor's hash. Recovery reads segments by sequence number from the
   last verified point: the log start, a compaction base, or an intact
   checkpoint. It refuses with `ErrCorrupt` on:
   - a gap or a duplicate;
   - a broken chain;
   - a header seq or epoch that goes back;
   - damage below the tail with no matching repair note.
3. **Fencing.** A writer holds the newest manifest epoch, and only one
   writer at a time can. An older writer cannot append after a newer
   writer's fence segment (`ErrFenced`).
4. **Unknown outcomes.** When a PUT's result is lost and cannot be read
   back, the commit is `ErrUnknownOutcome`. It is never reported as
   fenced and never as failed.
5. **Damage is never data.** A damaged object or a truncated read makes
   the read return `ErrCorrupt`, `store.ErrShortRead`, or a retry. It
   never returns a silent prefix and never panics. A damaged tail is
   accepted only with `WithRepairTail`. Repair writes a create-only note
   and never rewrites the segment.
6. **Snapshots and checkpoints are caches.** A snapshot (v2) is bound to
   its log id, LSN and chain. A checkpoint is bound to its log id, start,
   last segment and that segment's first LSN, chain tip, timestamp and
   epoch. If either one disagrees with the log, it is ignored and a full
   scan runs. Neither is ever loaded with a wrong value.

## Defects found and fixed

Each fix has a test that fails without it.

| id | defect | fix (commit) | test |
|---|---|---|---|
| A-D1 | `--repair-tail` deleted the damaged segment and then re-created its prefix. A crash in between lost the valid prefix, and a zombie writer could fill the freed slot. | Create-only repair note `log/<id>/<seq>.repair` (6f3ee8f) | TestRepairTailNeverRewrites |
| A-D2 | The tail was taken as the last key listed, so any other object under `log/<id>/` moved it. | Tail by max parsed seq (6f3ee8f) | TestTailIgnoresOtherKeys |
| A-D3 | A 412 whose read-back failed was reported as `ErrFenced`. | `ErrUnknownOutcome` (6f3ee8f) | TestPutExactlyOnceOutcomes |
| A-D4 | A failed open leaked the goroutines it had started. | Teardown on every error path (6f3ee8f) | TestOpenFailureLeaksNothing |
| A-D5 | A manifest we created and then found below a newer one was treated as ours. | Unknown outcome (6f3ee8f) | TestManifestLostAfterPutIsUnknown |
| A-D6 | Opening an fs directory deleted every temp file, including a live writer's. | Only stale temp files are removed (857449e) | store TestNewFSKeepsFreshTmp |
| A-D7 | Non-zero reserved segment-header bytes were accepted. | Rejected, so a future use is never misread (3425af3) | format tests |
| A-D8 | A stale or partial LIST could shorten the log: acknowledged commits were not replayed. | Recovery reads segments by seq; `checkNoHole` (88c997e) | readstable_test, sim |
| A-D9 | One transient misread (bit flip, misdirected read) turned into a permanent `ErrCorrupt` or wrong data. | `getStable`: damage must repeat 3 times, a "valid but different" read 6 times (88c997e) | readstable_test, sim swarm read faults |
| A-D10 | `Store.Get` could return a silent prefix. | `store.ErrShortRead` plus a size check (c0624d0) | store tests, TestS3TruncatedGetIsAnError |
| A-D11 | A damaged keyring signing seed panicked (found by the rot atlas). | `ErrCorrupt` (32d2be6) | rot_atlas_test |
| A-D12 | Snapshots were not bound to their log, so a snapshot from another log or LSN could be loaded. | Snapshot v2 header with log id, LSN and chain (82c4503) | snapshot_test |
| A-D13 | S3 `Get` called `Stat` after reading and got size 0, so every S3 read failed with "short read". CI was red from c0624d0 to 68fbb8f. | Size comes from the GET response, one request (365c968) | s3 tests on MinIO, now also in the local full suite |
| A-D14 | A CRC-valid but inconsistent manifest panicked at open (null log entry) or recursed without end (parent cycle). Found by the new FuzzOpenManifest. | `Manifest.validate` (this change) | FuzzOpenManifest seeds |
| dev | The first checkpoint loader dereferenced nil on stable damage, because `getStable` returns stable damage as data. | Decode error propagated (never pushed) | TestCheckpointRejected |

Out of scope and noted for component F (keyring): `_sign` is silently
regenerated when it is missing, and swapping `_kek` gives a raw AES error
rather than `ErrWrongKEK`.

## Added this component

- **Storage fault model in the sim**, research pick 1. The sim injects:
  - bit flips, misdirected reads, short reads and stale LISTs on reads;
  - torn writes.

  Its oracle is "`ErrCorrupt` or the right data". Rot atlas: every live
  object is damaged in turn.
- **Group-commit budget**, pick 2 (29ae382). `CommitPolicy{MaxDelay,
  TargetBytes}`, off by default.
- **Segment-index checkpoints**, pick 3 (68fbb8f).
  - `log/<id>/<seq>.idx` is written with each snapshot.
  - Open reads one LIST, the checkpoint, its last segment and the tail
    after it.
  - Replay read-ahead (8 concurrent segment GETs, in-order delivery).

## TLA+ alignment

FreshLog.tla now models checkpoints:

- `Checkpoint(w)` records the writer's own last acknowledged commit.
- `Scan` may start its damage checks after any intact checkpoint.
- New invariant `CkptSound`: everything a checkpoint covers is present
  and either undamaged or noted.

TLC run, complete state space: 22,587,747 states (5,697,022 distinct),
depth 32, no error.

Mutant: a checkpoint allowed at any dense prefix, not only the writer's
own acknowledged commit. It fails `CkptSound`.

The header now says repair is note-based, not "in place". `Immutable` now
holds with no exception.

Not modelled:

- latent rot after a checkpoint was written (covered by read-time
  verification and `Verify`);
- read faults (sim only).

## Tests

| | before (2f7128f) | after |
|---|---:|---:|
| Test/Fuzz functions (root, format, store, sim) | 118 | 152 |
| Fuzzers on log decoders | 3 (segment, payload, snapshot) | 5 (+ checkpoint, + whole open from a fuzzed manifest) |

The fuzzers ran on this box as follows:

- FuzzOpenManifest: 3 min, 1.27 M execs, the panic above (A-D14) found
  and fixed, then clean.
- FuzzDecodeCheckpoint: run during development, clean.

Every decoder on the log path has a fuzzer:

- segment header and frames;
- payload;
- snapshot;
- checkpoint;
- manifest, through Open.

The base decoder is out of scope here and belongs to component B.

Other test kinds:

- **Property tests:** prop_test.go (rapid).
- **Deterministic sim with fault injection:**
  - 1,500 and 4,000 seeds with checkpoints: 0 corrupt-under-read-faults;
  - a 4-shard long sim run is pending (below).
- **Crash points:** crash_test.go and the power-loss model cover these:
  - every PUT in an open (manifest, fence, segment, note, snapshot,
    checkpoint) can be lost, delayed past a crash, duplicated, or torn;
  - the sim crashes at every step.

  Faulty now tears `.idx` objects too. A checkpoint PUT that is lost or
  torn is a cache miss: the checkpoint is ignored and open does a full
  scan.
- **Race detector:** the full suite runs `go test -race ./...` before every
  code push.

### Coverage

Method: `go test -coverpkg=./,./format,./store . ./format ./store`, without
S3 credentials. A block counts as covered if any of those test binaries ran
it. "+ sim" also merges `./sim`.

| file | before | after | after + sim |
|---|---:|---:|---:|
| log.go | 80.7% | 93.5% | 94.7% |
| checkpoint.go | (new) | 94.8% | 96.1% |
| readstable.go | (new) | 100% | 100% |
| snapshot.go | (new split) | 88.8% | 91.2% |
| manifest.go | 81.3% | 87.7% | 90.6% |
| writer.go | 92.1% | 91.5% | 93.5% |
| db.go | 90.0% | 93.1% | 94.6% |
| store/faulty.go | 0% | 46.7% | 100% |
| root package, all files | 85.5% | 87.7% | 90.4% |

Uncovered lines that remain, and why:

- **Error returns of store I/O.** Examples are a LIST failing in
  `checkNoHole`, a PUT of a repair note failing, and `ctx.Err()` in the
  retry loops. They pass the error through unchanged. The sim hits some of
  them; the rest need a fault at an exact call.
- **Defensive branches.** One example is `clone`'s nil-map fill, since
  decode already fills the maps.
- **Lines only the S3 binary reaches.** `store/s3.go` shows 0% because the
  S3 tests need MinIO. They run in CI and in the local full suite (s3
  step), but not under this coverage command.

### Mutation checks

Each check below was disabled (condition replaced with `false && …`), the
named test run, then the code restored. The recovery, `readSeg` and
`validate` mutants ran by script on 2026-10-29 at about 23:50 IST; the
checkpoint mutants ran by hand during development.

- Checkpoint checks: the tip compare, epoch compare, start compare,
  first-LSN compare, contiguity check, and propagation of the checkpoint
  decode error.
- Recovery checks: the epoch-regress, LSN-gap and chain-broken checks, and
  the store `ErrCorrupt` mapping (TestRecoverRefusesForgedSegments). The
  first LSN-gap test **survived** the removal of the LSN check, because the
  hash chain caught the gap one frame later. A second case now forges a gap
  with a consistent re-computed chain, and it kills the mutant.
- `Manifest.validate`: without it, the FuzzOpenManifest seeds panic.
- `readSeg`'s first-LSN check (TestLogReadPaths).
- Earlier in the component:
  - the `getStable` agreement counts;
  - the snapshot v2 binding fields;
  - the repair-note match.
- TLA mutants: `Superseded` (existing) and the any-prefix checkpoint (new).

## Benchmarks

The numbers were measured the same way before and after on a shared 8-core
box with other jobs running. Raw output is in bench/results/2026-10-29-*.

| benchmark | before | after |
|---|---|---|
| DecodeSegment | 620–810 µs, 3,208 allocs | 290–355 µs, 2,184 allocs |
| AppendFrame | 5.6–8.0 µs, 7 allocs | 450–540 ns, 0 allocs |
| ChainNext | 1.7–1.9 µs, 1 alloc | 1.7–2.3 µs, 0 allocs (within noise) |
| Open, 100 segments, 2 ms GET, no snapshot | 480–518 ms | 283–293 ms (read-ahead) |
| Open, 100 segments, 2 ms GET, SnapshotEvery 50 | 480 ms, 206.5 segment GETs | 64–68 ms, 106.5 segment GETs |
| Commit policy, 8 writers, 2 ms PUT | 1,766 txn/s, 0.25 PUT/txn | MaxDelay 1 ms: 2,246 txn/s, 0.13 PUT/txn (default unchanged) |
| PutSerial mem | 7.8–8.4 µs | 8.0–9.0 µs (noise) |
| PutParallel mem / fs | 3.8–4.5 / 10.2–12.4 µs | 4.0–4.5 / 9.2–14.3 µs (noise) |
| PutSerial fs | 333–370 µs | 428–575 µs, **unresolved**: measured during the 6 h soak, which loads the same disk. Rerun after the soak. |

S3 cost after 3,400 commits, opening: 808 GETs and 6 LISTs. A
single-request Get keeps one GET per object read (docs/s3.md).

## Residual risks

- **Lost newest objects are undetectable.** If a store loses the newest
  segments and nothing points past them, recovery sees a shorter log.
  Nothing points past them unless a snapshot, checkpoint, branch or
  manifest does. This is inherent to an append-only log with no external
  counter.
- **LIST is trusted for new keys above the tail.** Recovery reads by seq
  and stops at the first NotFound. It needs read-after-write for GET,
  which S3, R2 and GCS give today.
- **Read faults are injected only on `log/` and `manifest/` reads.**
  Snapshots, bases and the keyring are covered by the rot atlas, which
  damages objects at rest, not by transient read faults.
- **Checkpointed segments are not re-verified at open.** Rot in them is
  caught when the segment is read (frame CRC, the index's first-LSN
  check), by `Verify` (full scan plus checkpoint cross-check), or by a
  `fullScan` open. Until then, a value read from a rotted segment returns
  `ErrCorrupt`, never wrong data.
- **Checkpoint size grows with the log.** It is 24 B per segment and
  written with each snapshot, about 2.4 MB at 100k segments. Compaction
  resets it.
- **Open is still O(log) when vector or document-index lenses are in
  use.** They replay every segment because they have no snapshot yet.
  This is planned for components D and E.
- The fs serial-put number above is still unresolved, and S3 is tested
  only on MinIO.
- The 6 h soak (SOAK OK, 2026-10-30 02:09 IST) and the 4-shard long sim
  (600 seeds × 2 000 steps, all OK) finished after this review; results
  in bench/results/2026-10-29-soak. Server RSS and disk grew during the
  soak; that is not root-caused.

## Verdict

The log meets its contract above under the tested fault model: the sim,
TLC and the fuzzers found nothing open. Defects A-D1 to A-D14 are fixed,
each with a regression test.

The component is **done for this review** with these open items:

- the fs put rerun.

This is not a claim of production readiness. The residual risks above
stand.
