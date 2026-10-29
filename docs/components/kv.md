# Component B: the KV lens

Review of the KV lens, completed 2026-10-30 (IST). Code: `lens/kv.go`,
`lens/kvrun.go`, the decoder helpers in `lens/lens.go`, and the KV
callers in the root package (`tx.go`, compaction, shredding). This
review does not claim the KV lens is production ready; "Verdict" says
what it does claim.

## Contract

1. **Fold.** The lens is a pure fold of the log. `Get(space, key, at)`
   returns the newest version with LSN ≤ `at`; a delete version reads as
   absent (`Version{}, false`). `Scan(space, prefix, at)` returns the same
   for every key with that prefix, in key order. `LastWrite` is the
   newest LSN that wrote the key.
2. **Deterministic state.** `Snapshot()` bytes and `Digest()` depend only
   on the applied entries, never on the in-memory layout (hot set, runs,
   merges in flight). The format ("KV2", or "KV3" when any version has a
   chunk) is unchanged by this review.
3. **Strict loading.** `LoadKV` (snapshot) and `LoadKVBase` (compaction
   base) accept only bytes `Snapshot()` could have written, apart from a
   base's inline threshold. Anything else is `ErrBadSnapshot`: a snapshot
   is then ignored and the log replayed; a base is `ErrCorrupt`. Loading
   never panics.
4. **Shredding.** `DropSubject(s)` redacts every version tagged with
   subject `s`, in every run, before it returns.
5. **Concurrency.** `Apply`, `DropSubject`, `SetLatest` run under the
   view's write lock; `Get`, `Scan`, `Latest`, `Snapshot` under its read
   lock and never mutate the lens. The lens is pure (no goroutines, no
   sync), per the lens import rule.
6. **Values are not shared out.** `Version.Val` may alias an immutable
   run buffer; every caller copies it before it leaves the package
   (`tx.open` `bytes.Clone`, compaction `Clone`, doc/vector decode).

## Defects found and fixed

- **B-D1, decoder accepted malformed snapshots and bases** (found by the
  new `FuzzLoadKV` within seconds). The old decoder accepted, among
  others:
  - a key without the `\x00` space separator: loaded, then `Latest`
    panicked (slice bounds);
  - keys out of order or repeated: later ones silently replaced earlier
    ones, so a damaged base changed data instead of being refused;
  - versions out of (LSN, Op) order: `Get` binary search returned wrong
    versions;
  - a version count of 0;
  - a bool byte > 1, a nil-marked (MaxUint64) string, a "KV3" tag with no
    chunk.

  The new decoder also refuses a chunk > MaxUint32 and trailing bytes.

  All are now `ErrBadSnapshot`. Note one entry may write the same key
  twice with the same LSN and ascending Op; that stays legal and tested.
  Regression: `TestLoadKVRejectsMalformed` (18 cases; 7 were accepted by
  the old code) and the fuzzer's re-encode check.
- **B-D2, `DropSubject` was O(all keys)** on every shred and on every open
  after one. It now rewrites only runs whose per-run subject count is
  non-zero, plus the hot set.
- **B-D3, prefix `Scan` was O(all keys)**: it walked the whole map and
  sorted the matches (866 ms–1.19 s per 100-key scan at 10M keys). Now
  a binary search per run and on the hot set's sorted keys.
- **B-D4, memory**: one Go map entry plus a slice per key cost
  172–200 B/key of heap at 1M–10M keys (research item 4).

## Design (research item 4: interim compressed key index)

- **Hot set**: the newest ≤ 65,536 versions in a map, with its keys kept
  sorted (`keys`) plus up to 1,024 unsorted new keys (`recent`) merged in
  when full.
- **Runs**: when the hot set is full it is frozen into an immutable run.
  A run is one byte buffer of entries in key order, front-coded against
  the first key of each 16-entry block. An entry is the key suffix and a
  body of versions (uvarint LSN, Op, a flags byte, then only the fields
  that are set). LSNs are absolute, so merging two runs' bodies for one
  key is concatenation.
- **Index**: each run has an open-addressing slot table (load ~0.77,
  multiply-shift index). A slot holds 8 hash bits, the block start and
  the entry's offset in its block; an offset ≥ 65,535 falls back to a
  seek from the block start. `Get` checks the hot set, then runs newest
  first, with every run's first slot loaded up front so the cache misses
  overlap.
- **Merging**: runs are merged pairwise, incrementally (16 keys per op
  applied), several disjoint pairs at once, when the older run is at
  most 3× the newer. Beyond 24 runs a merge in progress completes at
  once. A run is capped at 2^39 bytes (the slot's block-start field);
  loading a larger snapshot splits it into disjoint runs.
- **Model**: the old map implementation is kept as a test model
  (`kvmodel_test.go`); rapid checks that the run lens refines it.

## TLA+ alignment

There is no KV-specific spec. `FreshTxn.tla` uses the KV lens only as
"the fold of a log prefix" for snapshot reads and "does any entry after
snap write k" for conflicts; both are contract item 1 and unchanged.
The new representation is checked against the map model by property
tests (below), not by TLC. Conflict detection does not call
`LastWrite` (it tracks writes in `writer.go`); `LastWrite` is kept and
tested.

## Tests

| | before (00ecf41) | after |
|---|---:|---:|
| Test/Fuzz functions in `lens` | 13 | 23 |
| Fuzzers on KV decoders | 0 | 1 (`FuzzLoadKV`, snapshot and base) |

- `FuzzLoadKV`: strict and base loads; anything accepted must re-encode
  to identical bytes and survive Latest/Get/LastWrite/Scan/DropSubject/
  Digest. 5 min, 24.8 M execs, clean after B-D1.
- `TestKVRunsRefineMap` (rapid, 2,000 checks in development, 100 in CI):
  random ops, random tiny hot/recent/run-size limits so freezes, merges
  and run splits happen every few ops, shreds, snapshot reload, merge
  work; after each step every Get/Scan/LastWrite/Latest/Snapshot equals
  the map model, and hot-set invariants hold.
- `TestKVSetLatestRefineMap`, `TestKVRunsBounded` (200k keys, ≤ 7 runs),
  `TestKVRunBigBlock` (40 KiB values: offset fallback),
  `TestKVRunsForcedMerge`, `TestKVManyVersions`, `TestKVLatestError`,
  `TestKVRunOddVersions`, `TestLoadKVRejectsMalformed`.
- Race: the full suite runs `-race` over the root package, whose
  concurrent read/write tests exercise the lens under the view lock.
- Crash points: the lens holds no durable state; the log's crash tests
  (component A) recover it by snapshot load or replay, both covered.
- Mutation: 20 hand-made mutants of kv.go/kvrun.go/lens.go decoder
  lines. First pass 17 killed; the 3 survivors (hot counter, a position
  without checksum, the subject liveness check) led to `checkHot` and
  `TestKVRunOddVersions` and are now killed: 20/20.
  bench/results/2026-10-30-kv/mutation.txt.
- Coverage (`-coverpkg=./,./lens`, root + lens tests): kv.go 94.3 % →
  99.0 % from lens tests alone (100 % with the root tests, which cover
  `Ref` and `LoadKVBase`), kvrun.go (new) 100 %, the decoder in lens.go
  84.6 % → 100 %. The review's coverage pass added four malformed-input
  cases (truncated u64, value or version count past the end, chunk over
  32 bits) and removed one unreachable branch in `SetLatest` (an
  in-place replace never grows the hot set, so no freeze can be due).

## Benchmarks

`lens/kv_bench_test.go`, same code before (old map lens) and after:
keys `user%010d` shuffled, 8 puts per entry, 1 KiB values stored by
reference (as the root package does above the inline limit). 1M keys
(`-count 3`) and 10M keys (`FRESHDB_KV_KEYS=10000000`). Shared 8-core
box with the 6 h soak running throughout, so expect ±20 % noise. Raw
output: bench/results/2026-10-30-kv.

| benchmark | 1M before | 1M after | 10M before | 10M after |
|---|---|---|---|---|
| ingest (µs/key) | 1.07–1.50 | 2.04–2.41 | 1.53 | 2.60–2.90 |
| heap after ingest (B/key) | 200 | 60.5 | 172 | 63.6 |
| heap after snapshot load (B/key) | 200 | 30.6 | 172 | 30.2 |
| Get (ns) | 200–240 | 257–268 (3 runs) | 302–346 | 397–403 (4 runs) |
| Scan, 100 keys | 76.6–79.5 ms | 39–42 µs | 866 ms–1.19 s | 51–65 µs |
| snapshot load | 1.09–1.16 s | 0.24–0.33 s | 13.5 s | 2.38–2.45 s |

Tradeoffs, stated plainly:

- **Get is 1.1–1.3× slower** (one hash probe per run instead of one map
  probe). End-to-end `Get` is dominated by the HTTP/tx path; the root
  benches are recorded separately below.
- **Lens ingest is 1.7–1.9× slower** (merge work, write amplification of
  each version is rewritten once per
  merge level, a few times at 10M keys). Commit latency is dominated
  by the log PUT/fsync, hundreds of µs.
- **Heap after ingest is ~2× the loaded heap**: hot set plus merge output
  buffers in flight. Both are bounded (hot ≤ 65,536 versions; a merge
  output ≤ the two inputs).

Root benchmarks (`bench_test.go`, 10k keys, all in the hot set, so
both versions use a map here), `-count 3`, before then after:

| benchmark | before | after |
|---|---|---|
| `DB.Get`, mem store | 207–241 µs | 294–357 µs |
| interleaved rerun, 3 pairs | 327–671 µs | 396–976 µs |
| PutSerial mem / fs | 7.6–8.9 µs / 0.57–1.19 ms | 8.1–9.2 µs / 0.66–0.72 ms |
| OpenRecover10k, 2 ms GET, no snapshot / snapshot | 282–289 / 98–105 ms | 286–292 / 104–105 ms |

The `DB.Get` gap is load noise from the soak, not the lens: a CPU profile
of the after binary puts 95 % of `Get` in `store.Mem.Get` copying the
segment (`memmove`) plus GC, and no KV lens frame reaches the 0.5 %
cutoff. The fs numbers share the disk with the soak. A rerun on a quiet
box is still owed.

## Residual risks

- **Merge pauses.** A merge normally advances 16 keys per op; when runs
  exceed 24 one completes at once, a pause proportional to its size
  (not observed in the tests above: max 7 runs at 200k keys and 4 at
  10M).
- **A hot-set scan filters up to 1,024 unsorted keys** per call.
- **Full snapshot / digest is still O(N)** (merged iteration over all
  runs). Unchanged from before; snapshots run off the commit path.
- **Values by reference stay in the log**; this review shrinks the index,
  not the values. Inline values (≤ the inline threshold) are kept in runs.
- **No KV-specific TLA+ spec**; refinement is checked by rapid against
  the map model only.

## Found outside this component

- **`TestMetricsCounters` flaked in CI** (`freshdb_conflicts_total` 2,
  want 1; 13 of 300 local `-race` runs, and 8 of 300 on the code before
  this review, so not caused by it). A transaction that loses to a commit
  still in the same group commit can retry before the winner is visible
  and lose again. That is serializable, but `conflictBackoff`'s comment
  ("the retry already reads the winner's commit") is wrong in that case.
  The test now asserts ≥ 1; the retry behaviour goes to component F
  (transactions).

## Verdict

The KV lens meets the contract above under the tests listed: the
fuzzer, the refinement property and 20/20 mutants found nothing open.
B-D1 to B-D4 are fixed with regression tests or benchmarks. The
component is **done for this review**, trading a 1.1–1.3× slower point
`Get` and 1.7–1.9× slower lens ingest for 5.7× less heap after load,
4–5.5× faster snapshot load and prefix scans that no longer scale with
the key count. This is not a claim of production readiness.
