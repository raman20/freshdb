# 02 — Point-read path (item 2)

## Goal
Win back point-read throughput lost when values moved out of RAM into the
log (YCSB C at 64 clients: v0.1.0 2.92M ops/s; files layout 1.21M; default
journal 1.51M in the full A,B,C,F,D run), without raising memory by default
more than the value cache size, and without changing on-disk formats.

## Where the time goes (CPU profile, C-only, c=64, default journal)
`freshdb-bench ycsb -workloads C -clients 64 -cpuprofile` (closed loop only):
- `pread` syscalls on cache misses: ~15% of CPU;
- value cache `get`/`put` (one mutex per shard, LRU list moves): ~20%;
- RWMutex reader-count atomics (`sync/atomic.Int32.Add`): ~11%;
- `DB.branchWriter`: takes the global `db.mu` **exclusively** on every read: ~6%;
- per-read `Tx` allocation (two maps, a closure) and a second map lookup
  in the journal (`live` + `GetRange`).

## Changes (each measured separately)
1. `branchWriter` reads the branch map under a read lock (or an atomic
   snapshot), not the exclusive `db.mu`.
2. `DB.Get` / read-only views avoid allocating per-Tx maps until a write
   or a read-set entry is needed.
3. Value cache: more shards, read hits without moving list nodes (CLOCK
   reference bit), so a hit takes no exclusive lock. **Tried; no measurable
   gain over the LRU (C c=64 1.66M vs 1.63M). Replaced by 3b.**
3b. Off-heap ring cache: 64 shards, each a byte ring sliced from one
   anonymous `mmap` arena (heap fallback on other OSes); index
   `map[uint64]uint64` (hash of log+seq+off → logical position); FIFO
   overwrite with a second chance (a hit in the oldest quarter re-puts the
   entry); stale index entries swept every quarter ring. The cached bytes
   are invisible to the GC, so RSS grows by the cache size, not 2–3× it.
4. Journal reads: one index lookup per read; optionally serve reads from a
   read-only `mmap` of the journal (no syscall per miss). **Not done:** the
   second `live()` check is required by the cross-instance contract
   (delete by another instance ⇒ `ErrNotFound`); mmap deferred (SIGBUS on
   truncation, RSS accounting). Only the `live()` buffer moved to the stack.
5. Default cache size: revisit only after 1–4, with RSS measured.
   **Raised 64 → 256 MiB** once the cache moved off-heap (numbers below).
6. Found while profiling (a bug since v0.1.0): `Get`/`Scan`/`Topic.Read`
   returned slices aliasing the cache or the in-memory lens, and
   `Put`/`Publish` kept the caller's buffer. A caller writing into a
   returned or passed slice could change what later readers saw. Values
   are now copied both ways (R6).

## Invariants
- R1: a read at LSN `at` returns exactly the value the lens fold gives at
  `at` (snapshot semantics unchanged; existing tests + sim digests).
- R2: every value read from storage is CRC-checked against the lens before
  it is returned or cached (`ErrCorrupt` otherwise).
- R3: the cache never returns bytes for a different position (key =
  log, segment, offset; positions are immutable).
- R4: shredded subjects stay unreadable (cache holds ciphertext only).
- R5 (mmap): reads never see bytes past the durable end the index knows;
  a hole punched by delete reads as zeros and fails CRC, never panics.

## Failure modes
- Racing `Close` with readers (mmap unmapped under a reader) → readers
  must hold a reference; unmap only after the last reader.
- File growth: mapping must be extended (remap) before reading a new
  region; fall back to `pread` when outside the mapping.
- Cache stampede under concurrency: duplicate loads are allowed (same
  bytes), only one entry stored.

## Test plan
- Unit: cache (hit/miss/eviction/byte bound/concurrent), journal mmap
  reads across growth, reads after Close error cleanly.
- Property: random ops on DB; every Get equals a model map (existing
  model tests), with tiny cache sizes to force misses and evictions.
- Sim: existing deterministic sim (digests unchanged).
- Crash: existing kill -9 journal tests read through the new path.
- Race detector on the store and root packages.

## Success metric
YCSB C at 64 clients with the default URL in the full A,B,C,F,D run, same
command as `2026-10-28-default-journal`, before 1.51M ops/s → after,
plus peak RSS (must not rise by more than the cache-size change).

## Outcome
C-only probe (`-workloads C -clients 1,8,64 -duration 8s`, default URL,
1M × 1 KiB, same box, one run each; ops/s at c=1 / c=8 / c=64, peak RSS):

| variant | c=1 | c=8 | c=64 | peak RSS |
|---|---|---|---|---|
| a7e6cbb (64 MiB LRU, values aliased) | 424k | 1.87M | 1.95M | 897 MB |
| + copies (R6) only | 334k | 1.48M | 1.63M | – |
| + alloc cuts, `branchWriter` snapshot | 400k | 1.56M | 1.86M | 1.02 GB |
| + off-heap ring, 64 MiB | 437k | 1.44M | 1.94M | 882 MB |
| + off-heap ring, 256 MiB (new default) | 543k | 1.93M | 2.39M | 1.15 GB |
| for contrast: on-heap CLOCK, 256 MiB | 494k | 2.11M | 2.58M | 1.57 GB |

The copies cost ~16% on hits; the allocation cuts and the ring win it
back at equal cache size; the extra 192 MiB of cache costs ~+266 MB RSS
off-heap versus ~+550 MB on-heap. Success metric, full A,B,C,F,D default-URL run
(`bench/results/2026-10-28-read-path/`, same command as
`2026-10-28-default-journal`): C c=64 1.51M → 2.24M ops/s, peak RSS
1.95 → 2.12 GiB (+0.17 GiB for +192 MiB of cache): met. Remaining on this path: RWMutex reader-count atomics
(~14% CPU at c=64), the second `pread` per miss, and GC from per-read
copies.
