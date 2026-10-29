# Soak growth investigation (2026-10-30)

Subject: the 6 h soak of 2026-10-29 (bench/results/2026-10-29-soak) passed
every check, but server RSS went 405 MiB -> 3.3-4.5 GiB (peak 6.06 GiB),
disk 2.6 -> 9.5 GiB, the driver's average rate 21.2k ops/s (first 30 s)
-> 2.6k ops/s (whole run), with 5,823 "slow request" warnings.

Inputs: the soak's own database (`/workspace/soak6h/db`, a 244 GB sparse
journal, 9.3 GB allocated) and server log, copied before every probe.
All times IST. Box: 8 vCPU, 15 GiB RAM, shared disk.

## Proven (measured)

| finding | evidence |
|---|---|
| **Disk** is journal dead space, not live data | offline `freshdb gc --keep-for 0s --grace 0s --shrink` on a copy: 40.8 M dead records; allocated 8.57 GB -> 0.45 GB, apparent 244 GB -> 0.45 GB (6 min 54 s). Live data is the 448 MB base (mostly the 11.37 M topic messages). Each deleted object leaves its header+key and the partial blocks around its punched data; only the offline shrink reclaims them (S5.3) |
| **RSS**: live heap is the topic lens, not a leak | heap profile after a forced GC (`/debug/pprof/heap?gc=1`) of a server on a copy: ~1.09 GB live, almost all `lens.Topic` (11.37 M messages, ~95 B each). The soak sets no topic retention, so topics only grow |
| **RSS peaks**: the journal open alone peaked at 4.45 GB | `VmHWM` of a process doing only `store.OpenLocal` + `OpenStore` on the final database: 4.45 GB (old code). Journal recovery kept a map of every deleted key (tens of millions) besides the live index. Every kill -9 restart paid this |
| **Open time** grew with dead records | old code: store open 54-59 s warm, 89-97 s right after a copy; DB open on top 2.1-2.6 s. CPU profile: 46 % read syscalls (two preads per record), 21 % map inserts |
| **Slow requests are compaction/GC pauses** | 4,784 of the 4,809 non-maintenance slow requests (KV 3,431, /v1/tx 1,036, topics 342) ended during a compaction or GC (±1 s). Compaction and GC pause every writer for their duration: 572 compactions and 188 GCs were themselves slow (compact p50 7.0 s, max 51 s; GC p50 8.2 s). Maintenance wall time was 990-1,320 s per hour, i.e. writers paused ~30-37 % of every hour, with p50 7.1 s in hour 1 rising to 9.4 s in hour 6 |
| **21k ops/s is not the steady rate** | 21.2k/s is the first 30 s (no compaction, restart or check yet). Per 20-50 min window the rate was 5.5k/s in the first 20 min, then 1.7k-3.8k/s |
| **A client timeout on /v1/compact halted the DB** | a 30 s client timeout during a 42 s compaction cancelled the manifest put; the outcome was unknown and the DB halted until restart. Reproduced in `TestCompactCallerCancelDoesNotHalt`; fixed in f0b1838 |

## Fixed here

- f0b1838: a caller cancel no longer halts the DB (manifest put and the
  post-commit reload/sweep run on `context.WithoutCancel`, put bounded to
  2 min; a ctx already done before the put commits nothing).
- Journal recovery (this commit): reads through a 256 KiB window that stops
  at the next hole (`SEEK_HOLE`), instead of two preads per record, and
  tracks deletes only for keys with a damaged record before them (the only
  use of that map). Same records, same decisions; tests:
  `TestJournalScanWindow` (window edges, records larger than the window,
  dead records between live ones) plus the existing corruption, torn-erase
  and power-loss tests. Mutation: 3 of 4 mutants killed; the survivor
  (`m < n` short-read check) is masked by the size check before every body
  read.

| measurement | before | after |
|---|---|---|
| final 6 h database, store open, warm | 54.2 / 58.8 s | 6.2 / 7.4 s (12.4 s first run) |
| same, right after copying the file | 89.4 / 96.9 s | 23.3 s |
| same, process peak RSS (store + DB open) | 4.45 GB | 2.37-2.47 GB (the rest is the DB open: topic lens) |
| `BenchmarkJournalOpen` dead=200 (100k objects, 90 % deleted) | 157-181 ms, 28.2 MB, 571k allocs | 27.7-31.0 ms, 13.4 MB, 390k allocs |
| `BenchmarkJournalOpen` dead=64KiB (dead data punched to holes) | 544-593 ms, 763 MB (every live body allocated) | 322-382 ms, 13.7 MB |

## Hypothesized (not yet proven)

- Throughput fell from 5.5k/s (first 20 min) to ~2k/s while the
  maintenance share stayed ~1/3: likely restart downtime growing with the
  journal open time (134 restarts; the driver waits at most 30 s for
  `/readyz`, so each soak open was under 30 s, yet a copy of the final
  database took 54-59 s warm: unexplained, the soak's page cache is the
  first guess), and full checks and backups reading ever larger topics. To confirm with the instrumented
  soak (per-restart open time, profiles every 3 min).
- Compaction p50 was already 7 s in the first hour, when topics were small:
  the cost is not only O(topic messages). Candidates: per-object fsync and
  hole punching in the sweep, base write on the journal. To profile.
- The 6.06 GiB peak: the journal-open peak (4.45 GB at the end) plus GOGC
  headroom; a compaction's transient copies (whole-topic encode, ~450 MB
  base buffer) are a second candidate.

## Confirmation soaks (2026-10-30 03:53-04:52 IST)

Two 25 min soaks, seed 7, binaries at 7549a9e + the soak driver's
"server ready in" log line, heap/goroutine/metrics snapshots every 3 min:
`base2` (no topic retention, as the 6 h run) and `ret2`
(`--topic-max-messages=10000`). **Caveat:** the box also ran two full
test suites and the component D benches during them; and 25 min cannot
show whether a 6 h run stays flat. Both: **SOAK OK** (every full check and
every restored backup passed).

| | 6 h run at 26 min (old binary) | base2 (25 min) | ret2 (25 min, retention) |
|---|---|---|---|
| ops, rate (cumulative) | 9.04 M, 5.7k/s | 10.07 M, 6.6k/s | 12.31 M, 8.1k/s |
| first 30 s | 21k/s | 21.1k/s | 13.9k/s |
| max server RSS | 596 MiB (at 22 min) | 837 MiB | 412 MiB |
| topic heap at the last profile | - | 285 MB (`Topic.add` + `SetAll`) | 9 MB |
| restart → ready (17 restarts) | not logged (≤ 30 s by the driver) | p50 3.65 s, max 14.2 s | p50 2.84 s, max 6.2 s |
| database on disk at the end (allocated) | - | 1.8 GB | 1.9 GB |
| slow requests (> 1 s) | - | 514 | 455 |

What this confirms or refutes:

- **Retention bounds memory (S5.6): confirmed.** Same workload, half the
  peak RSS; the topic lens went from the largest heap item to 9 MB.
- **Restart open time:** confirmed short at this size (p50 3-4 s, max
  14 s). It still grows with dead records (S2.9); a 6 h database was not
  reopened under the new binary in a soak.
- **The 21k/s start is a cold-start burst** (empty database, before the
  first compaction, check and backup), in both binaries. Not a regression
  signal by itself.
- **Rate decline, "growing checks" hypothesis: supported in one window,
  not quantified over 6 h.** base2 04:08-04:16: the driver completed only
  ~50k ops in 8 min while the server, per its own metrics, ran 36k commits
  at 1.7 ms mean commit latency and logged one slow request after
  04:08:11: the server was mostly idle. The driver was inside `backupCheck` (backup 51.7 s,
  then restore, open, verify and full check of the copy, sequential in
  the main loop, with the workers' histories pinned, i.e. unpruned) the
  whole time. The harness, not the database, throttled throughput there.
- **Compaction already slow in hour 1: one cause observed, share not
  measured.** Two goroutine dumps (04:15-04:16; that process paused writers
  14.4 s over 3 compactions) show the sweep deleting garbage one object at a time
  (`deleteGarbage` → `Journal.Delete`: an appended tombstone + fdatasync,
  then erase + punch + fdatasync, per object) while every writer waits,
  interleaved with the backup's deferred deletes for the journal's write
  lock. 135 µs per delete on a quiet small journal. Candidate fixes: a
  batched journal delete (one fsync pair per batch) or sweeping outside
  the writer pause. Not done yet.
- **The 6 GiB peak: not reproduced** (max 837 MiB in 25 min); still a
  hypothesis.
- **Measurement artefact found:** a host snapshotter on this box drops
  full, non-sparse copies named `journal.fj.box-store-snap-*` next to the
  journal for minutes at a time; the driver's `disk` column counted them
  (13-22 GB for a 1.8 GB database). The driver now skips them. The 6 h
  run's interim disk numbers may include such copies; its final 9.5 GiB
  was confirmed separately as the journal itself (S5.3).

## Not fixed (documented)

- Topics without retention grow RAM and every compaction's cost without
  bound: set `--topic-max-age` / `--topic-max-messages` (component C).
- The journal reclaims dead records only offline (`gc --shrink`).
- Compaction and GC pause all writers (S2.2).

Files: `journal_open_{before,after}.txt` (go test -bench, quiet box).
