# 09 — Journal preallocation off the commit path

## Problem

The journal file grows in 8 MiB chunks. Before this change the commit
whose record crossed the end of the file zero-filled the next 8 MiB and ran
a full `fsync`, holding the writer lock. On the commit bench (4 KiB puts,
30k commits, ext4 on a VM disk) that showed up as one 10–65 ms commit per
chunk at c=1 and, at c=16, a p99.9 of 16–19 ms (every waiting client
stalls together).

Zero-filling is kept on purpose: commits that overwrite already-written
blocks need no metadata change, so `fdatasync` stays a data flush.
`fallocate` alone (unwritten extents) was measured here as an alternative:
c=1 p50 went from 37 to 140 µs and throughput from ~19k to ~6k commits/s,
because every commit then converts an extent.

## Design

- After each append, if the end is within 4 MiB of the file size and no
  growth is running, start one background goroutine that extends the file
  to the next 8 MiB boundary.
- It zero-fills in 256 KiB steps. Each step takes the writer locks (the
  in-process mutex and the `flock`, so other processes are excluded too),
  reads the current size and writes at that size, then unlocks. Records are
  only ever written below the size (an append extends synchronously first
  if it would pass it), so a step never touches a record.
- After each step, unlocked, `sync_file_range(WAIT_BEFORE|WRITE|WAIT_AFTER)`
  writes that step out, so a concurrent commit's `fdatasync` never has more
  than one step of dirty zeros to flush. One `fsync` at the end makes the
  size durable.
- The synchronous path stays as a fallback when appends outrun the grower.
- `Close` stops the grower and waits for it.

Safety (spec/FreshGrow.tla, small): writers append at `end` under the lock
and extend themselves if needed; growers write a zero block at the current
size under the lock. Invariant: no record block is overwritten, every block
below `end` is a record. The mutant where a grower reads the size, releases
the lock and writes later violates it in 4 steps. Crash: zeros past the end
are what a torn or empty tail already looks like (CRC fails, ignored).

## Results

`freshdb-bench commit` (30k commits of one 4 KiB put, journal on ext4, VM
disk), before and after binaries interleaved, 3 runs each. Ranges over
the runs. Raw data: `bench/results/2026-10-29-commit/`.

| | c=1 before | c=1 after | c=16 before | c=16 after |
|---|---|---|---|---|
| commits/s | 18.5–20.9k | 18.2–19.8k | 35.2–39.0k | 34.6–36.8k |
| p50 | 36–38 µs | 36–37 µs | 244–253 µs | 257–261 µs |
| p99 | 224–287 µs | 474–535 µs | 1.00–1.27 ms | 1.71–2.07 ms |
| p99.9 | 544–643 µs | 1.10–1.32 ms | 15.4–17.0 ms | 2.6–9.1 ms |
| max | 15.8–26.9 ms | 6.8–13.3 ms | 15.9–25.8 ms | 3.3–10.7 ms |
| commits > 10 ms | 13–16 | 0–3 | 207–224 | 0–16 |

The per-chunk stall is gone (commits over 10 ms: 13–16 per run → 0–3 at
c=1; p99.9 at c=16 down 2–6×). The price is the background writeback
competing with commits: p99 roughly doubles at both concurrencies and
p99.9 at c=1 doubles (0.6 → 1.2 ms); throughput is within noise to ~5%
lower. Variants tried on the same box: one 8 MiB step in the background
(still 16 stalls: the next `fdatasync` flushes the 8 MiB), steps without
`sync_file_range` (c=1 p99.9 3.4 ms), 64 KiB and 1 MiB steps and growing
a whole chunk ahead (within noise of the chosen 256 KiB / 4 MiB).

## Limits

- The zeros still have to reach the disk: the writeback now overlaps
  commits instead of blocking one, which costs some throughput and p99 at
  c=1 (see results).
- Each step changes the file size, so the next `fdatasync` journals a size
  update (32 small metadata commits per chunk instead of one).
- macOS/BSD have no `sync_file_range`; the steps are not written out early
  there.
