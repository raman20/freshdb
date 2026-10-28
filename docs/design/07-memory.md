# 07 — Memory: where freshDB's RSS goes, and what we cut

Status: implemented on `main` (item 7). Numbers: `bench/results/2026-10-28-memory/`.

## Question

YCSB 1M × 1 KiB (default URL = journal layout, fsync on, workloads
A,B,C,F,D at 1/8/64 clients) peaked at ~2.1 GiB RSS; the v0.1.0 report
quotes Pebble at 0.73 GiB. Where does freshDB's memory go, which parts can
shrink without losing the read-path gains, and is the Pebble comparison
fair?

## How it was measured

`freshdb-bench ycsb` now samples every 200 ms and at the end of the load
and of each workload (`memory` in the result JSON):
- **RSS split** from `/proc/self/status`: `RssAnon` (process-private
  memory: Go heap, the value-cache arena) vs `RssFile` (mapped files: the
  binary). freshDB and Pebble both read data files with `pread`, so their
  data in the OS page cache is in *neither* RSS.
- **Go runtime** (`runtime/metrics`): memory the runtime holds minus what
  it released, live heap after a forced GC, and the GC goal.
- **Off-heap anon** ≈ `RssAnon` − Go-resident: freshDB's mmap'd value-cache
  arena (nothing else is off-heap; no cgo).
- **Heap profiles** (`-memprofile`) after the load and each workload.
- **Page cache** of the data directory at the end (`mincore`), reported
  separately: shared, reclaimable, and not charged to the process.

## Where the 2.0 GiB goes (before, end of run, commit f5c1c22)

| part | MiB | what it is |
|---|---|---|
| KV lens: versions + map | 485 | one 72-byte `Version` per write, kept forever (MVCC history: time travel needs it), in a `[]Version` per key (a separate allocation even for one version) |
| journal object index | 94 + 39 | one map entry per segment (≈ per commit batch) plus its `log/<id>/<seq>.seg` key string |
| log segment index | 90 | one 64-byte `segIdx` per segment, 32 bytes of it a chain hash |
| map key strings | 36 | `kv\0user…` per key |
| value-cache index, misc | ~25 | |
| **live heap** | **815** | grows with history: 259 after the load, 424 after A, 527 after B, 721 after F, 815 after D |
| GC headroom | ~960 | Go's default `GOGC=100` lets the heap grow to ~2× live before collecting; garbage + free spans not yet returned |
| value-cache arena | 247 | off-heap, ≤ 256 MiB by default, fully touched by the read workloads |
| binary, other file-backed | 21 | |
| **RSS** | **2,046** | anon 2,025 |

In plain words: about half of RSS is the garbage collector's slack, a
quarter is the index of every version and every commit ever made (it
grows for as long as the database is not compacted), an eighth is the
value cache we chose to keep, and the rest is small.

No vectors are loaded in YCSB, so HNSW/vector slabs are 0 here (the HNSW
item measured 107 MiB heap at 100k × 128-d and 1,088 MiB at 1M).

## Pebble, measured the same way (same box, same run)

Pebble's 0.70 GiB peak is **only during the bulk load** (its batches and
the harness's value copies on the Go heap). From workload A on, Pebble's
RSS is **83–86 MiB** (anon ~63 MiB): an 8 MiB block cache (the default the
bench uses), memtables and little else. Its index and data live in sstables
and are read through the OS page cache, which is not in RSS.

So the fair comparison is not 2.1 vs 0.73 GiB. In steady state Pebble's
process holds ~0.09 GiB and freshDB holds ~2 GiB; freshDB's design keeps
every key, every version position and every segment in RAM. That gap is
structural, not a tuning problem.

## Changes

1. **Packed versions (40 bytes, was 72).** The lens keeps `ver{lsn, off,
   len, op, sum, chunk, del, *extra}`; the rare subject and inline value
   move behind one pointer. A key's first version is stored in the map
   slot itself, so single-version keys allocate nothing beyond the map.
   `lens.Version` (the API) and the snapshot encoding are unchanged.
2. **Segment index 24 bytes, was 64.** The chain hash after each segment
   is the next segment's first `Prev`, so only the tip's is kept;
   `chainAt` on a segment's last LSN reads the next segment (branch
   creation and shred certificates only, never the read path).
3. **Value cache sized from memory.** Default = 1/16 of the cgroup v2
   limit (else MemTotal), clamped to 16–256 MiB. On this 16 GiB box it is
   still 256 MiB; in a 1 GiB container 64 MiB.
4. **GC tuning is documented, not hard-coded.** `GOMEMLIMIT` (and `GOGC`)
   are process settings; a library must not set them. The curve below
   shows what they buy and cost.
5. **Bench:** the memory sampler, `-memprofile`, page-cache size,
   `-compact-between` (run `Compact` after each workload, as `serve
   --compact-every` would).

Not done, and why: an arena/open-addressing key table (keys are 36 MiB of
815; not worth a new hash table yet); buffer pools for per-read copies (the
copies are short-lived garbage, which the GC limit already bounds; the
profile shows no retained read buffers); journal readahead (pread, no mmap:
nothing in RSS to fix).

No TLA+ model: these are representation changes, not new protocol steps.
The chain-hash change is checked by a test that folds the hash chain over
every frame and compares `chainAt` at every LSN (it fails if the boundary
case is wrong), plus reopen of branches at every LSN.
