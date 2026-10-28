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

## Where the RSS goes (end of run, default settings)

| part | before | after | what it is |
|---|---|---|---|
| KV lens: versions + map | 485 | 313 | one `Version` per write, kept until `Compact` (MVCC history: time travel needs it). Before: 72 B each, in a `[]Version` per key (a separate allocation even for one version) |
| journal object index | 94 + 40 | 83 + 36 | one map entry per segment (≈ per commit batch) plus its `log/<id>/<seq>.seg` key string |
| log segment index | 90 | 39 | one `segIdx` per segment; before 64 B, 32 of them a chain hash |
| map key strings | 36 | 45 | `kv\0user…` per key (sampled profile; same keys) |
| value-cache index | 10 | 9 | |
| unattributed (profile vs runtime count) | ~60 | ~50 | |
| **live heap** | **815** | **576** | grows with history. Before: 259 after the load, 424 after A, 527 after B, 721 after F, 815 after D. After: 262, 349, 406, 540, 577 |
| GC headroom | ~960 | ~700 | Go's default `GOGC=100` lets the heap grow to ~2× live before collecting; garbage + free spans not yet returned |
| value-cache arena | 247 | 249 | off-heap, ≤ 256 MiB by default, fully touched by the read workloads |
| binary, other file-backed | 21 | 21 | |
| **peak RSS** | **2,046** | **1,549** | anon 2,025 → 1,527 |

("before" = commit f5c1c22; "after" = this item with defaults. MiB; heap
parts from `-memprofile` at the end of the run.)

In plain words: about half of RSS is the garbage collector's slack, a
quarter is the index of every version and every commit ever made (it
grows for as long as the database is not compacted), an eighth is the
value cache we chose to keep, and the rest is small.

No vectors are loaded in YCSB, so HNSW/vector slabs are 0 here (the HNSW
item measured 107 MiB heap at 100k × 128-d and 1,088 MiB at 1M).

## Pebble, measured the same way (same box, same run)

Pebble's peak (0.67 and 0.75 GiB in two runs today; 0.73 in the v0.1.0
report) is **only during the bulk load** (its batches and the harness's
value copies on the Go heap). From workload A on, Pebble's RSS is
**83–88 MiB** (anon 61–66 MiB): an 8 MiB block cache (the default the
bench uses), memtables and little else. Its index and data live in sstables
and are read through the OS page cache, which is not in RSS.

Page cache for the data directory at the end of the run (`mincore`; shared,
reclaimable, charged to neither RSS): Pebble 1,636 MiB, freshDB
4,269–5,424 MiB. freshDB's journal is append-only until `Compact`, so it
holds every version's bytes (~5 GiB here); Pebble compacts in the
background.

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

## Trade-off curve (after this item; one run per row, same box, same day)

YCSB 1M × 1 KiB, default URL, fsync on; closed-loop ops/s at 64 clients.
Peak RSS is over the whole run; "steady" is RSS after GC at the end.

| config | peak RSS | anon | steady | live heap | C c=64 | B c=64 | D c=64 | F c=64 |
|---|---|---|---|---|---|---|---|---|
| before (f5c1c22, defaults) | 2,046 | 2,025 | 2,019 | 815 | 1.65M | 907k | 881k | 116k |
| after, defaults | 1,549 | 1,527 | 1,500 | 576 | 1.67M | 950k | 927k | 110k |
| `GOMEMLIMIT=1200MiB` | 1,450 | 1,429 | 1,392 | 587 | 1.77M | 876k | 816k | 125k |
| `GOMEMLIMIT=1000MiB`, cache 128 MiB | 1,121 | 1,101 | 1,074 | 582 | 1.41M | 928k | 815k | 105k |
| `GOMEMLIMIT=800MiB` | 1,058 | 1,038 | 1,019 | 578 | 1.48M | 888k | 599k | 103k |
| `GOMEMLIMIT=600MiB`, cache 64 MiB | 703 | 682 | 651 | 529 | 1.10M | 703k | 169k | 65k |
| `-compact-between` | 2,074 | 2,053 | 1,196 | 272 | 1.56M | 852k | 600k | 103k |
| `-compact-between`, `GOMEMLIMIT=800MiB` | 1,201 | 1,180 | 926 | 282 | 1.86M | 912k | 931k | 118k |
| Pebble (defaults, 8 MiB block cache) | 771 (load) | 750 | 83 | 2 | 231k | 167k | 315k | 69k |

How to read it:
- The code changes alone: peak RSS −24% (2,046 → 1,549 MiB), live heap
  −29%, no throughput lost (all cells within run-to-run noise).
- `GOMEMLIMIT` trades GC CPU for RSS. Down to ~800 MiB (≈ 1.4× live
  heap + arena) reads hold up; D (read-latest with inserts) drops to
  599k. At 600 MiB with a 64 MiB cache the GC runs almost continuously:
  RSS matches Pebble's load peak but D falls to 169k and C to 1.10M.
  `GOMEMLIMIT` does not cover the cache arena (it is not Go memory), so
  RSS ≈ limit + cache.
- Compaction is what bounds the live heap (272–283 MiB instead of 576 and
  rising). Alone, its transient garbage pushes the peak up; with
  `GOMEMLIMIT=800MiB` it gave the best row: 1.20 GiB peak, 0.93 GiB
  steady, no throughput loss.
- C c=64 is noisy on this VM: 1.10–1.86M today across rows, and the
  2.24M of the read-path run was not reproduced even by "before" (1.65M).
  Treat differences under ~20% as noise.

Recommendation (in `docs/operations.md`): for a memory-bounded server set
`GOMEMLIMIT` to ~1.5× the live heap plus the value cache, and run
`serve --compact-every`.

Not done, and why: an arena/open-addressing key table (keys are 36 MiB of
815; not worth a new hash table yet); buffer pools for per-read copies (the
copies are short-lived garbage, which the GC limit already bounds; the
profile shows no retained read buffers); journal readahead (pread, no mmap:
nothing in RSS to fix).

No TLA+ model: these are representation changes, not new protocol steps.
The chain-hash change is checked by a test that folds the hash chain over
every frame and compares `chainAt` at every LSN (it fails if the boundary
case is wrong), plus reopen of branches at every LSN.

## Conclusion: the fair comparison with Pebble

- Peak vs peak (what the v0.1.0 table compares): freshDB 1.55 GiB with
  defaults (was 2.05), 1.06 GiB with `GOMEMLIMIT=800MiB`, 0.69 GiB only by
  giving up most of D/F throughput; Pebble 0.67–0.75 GiB, all of it in the
  bulk load.
- Steady state (the number that matters for a long-running process):
  freshDB 0.93–1.5 GiB anon vs Pebble ~0.06 GiB anon. Pebble keeps its
  index and data on disk and leans on the OS page cache (1.6 GiB here,
  reclaimable); freshDB keeps every key and version position in RAM by
  design. That 10–20× gap is structural, and only a disk-resident index
  (larger-than-RAM data, not on the roadmap for this item) would close it.
- freshDB still does 7× Pebble's C reads and 3× its D at 64 clients in the
  same runs, which is what the RAM buys.

## Not built: a disk-backed key index (what closing the gap would take)

The remaining steady-state gap to Pebble (~0.9–1.5 GiB vs ~0.09 GiB anon)
is the in-RAM index: one map entry per key (key string + 40 B per
version), one journal-index entry and one `segIdx` per segment. Moving it
to disk is a storage-engine change, not a tuning step, so it was not
started in this item (estimate: weeks, not under an hour). What it would
take:

1. **Split hot and cold.** Keep in RAM only keys written since the last
   compaction (a memtable-like delta map, as today). Everything folded into
   a compaction base image moves to an on-disk sorted index.
2. **On-disk index format.** The base image already holds the latest live
   version per key in key order. Add an index block per base: sorted
   (key, lsn, offset, len, crc) entries in ~4 KiB pages, a sparse in-RAM
   fence array (first key per page: ~1M keys / ~60 per page ≈ 17k fences,
   < 1 MiB) and a per-base Bloom filter (~10 bits/key ≈ 1.2 MiB per 1M
   keys). A point read then costs delta-map lookup → Bloom → one page
   `pread` (usually page-cache hot) → value `pread`.
3. **History.** Time travel (`AsOf`, branches at old LSNs) needs older
   versions. Either keep them only until compaction (today's semantics
   after `Compact` already drop them) or write a second on-disk
   (key, lsn) index per base; the RAM map would then never hold history.
4. **Segment indexes.** The journal object map and `segIdx` (≈ 120 MiB
   at 1M keys after this item) shrink to the segments after the last base;
   older ones are reachable through the base's index.
5. **Correctness work.** Recovery must rebuild the delta map from segments
   after the newest base and trust the base index only if its checksum and
   manifest commit match; this changes the open path and the compaction
   commit, so it needs a TLA+ extension of the compaction model
   (index published atomically with the base) and crash tests.
6. **Cost.** Reads that miss the delta map pay an extra page lookup, so C
   would fall from the in-RAM 1.6–1.9M ops/s toward Pebble's range on
   cold pages; with the page cache warm it should stay well above it.
   Expected steady anon: delta map + fences + Blooms + value cache, i.e.
   tens of MiB plus the cache, at the price of the page cache (which is
   reclaimable and what Pebble already relies on).

Cheaper partial steps, in order: run `Compact` regularly (already halves
the live heap here); let the latest version live in RAM and page out
history only (steps 3–4); then the full index (steps 1–2, 5).
