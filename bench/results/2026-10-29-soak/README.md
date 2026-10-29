# Soak / long simulator (item 18): both passed

## 6 h real-time soak: SOAK OK

`freshdb-soak -duration 6h -seed 7`, 8 workers, ext4 VM disk, loopback,
started 2026-10-29 20:00 IST, ended 2026-10-30 02:09 IST (the final full
check runs after the 6 h). Full output: [`soak6h.out`](soak6h.out).

```
SOAK OK: 6h2m6s elapsed, 56806351 ops (2615/s), 1067 unknown-outcome/errors, 134 kill -9 restarts,
216 full checks, 574 compactions, 271 GCs, 82 backups (82 restored+checked), max server RSS 6060 MiB
```

Every full invariant check (acknowledged writes present, no phantom or
double-applied writes, model fold equal to the lenses) and every restored
backup passed across 134 kill -9 restarts with compaction, GC and backups
running concurrently. The 1067 "unknown-outcome/errors" are requests cut
by the kill -9 restarts; the checker resolves each one against the
recovered state. The server log has 5823 "slow request" warnings and no
error, panic or fatal line.

What this run does **not** show (honest notes):

- **Throughput is not a performance number.** The box ran the full test
  suite, fuzzers, the long simulator and benchmarks during the soak. The
  ops rate per 30 min window fell from ~21k/s (first minute) and 5.2k/s
  (first 30 min) to 1.7–2.8k/s for the rest.
- **Server RSS and disk grew over the run**: server RSS 0.6 GiB at 30 min,
  1.3 GiB at 2 h, 3.3–4.5 GiB from 3.5 h on (max 6060 MiB); disk 2.5 GiB
  at 1 h to 9.5 GiB at 6 h. RSS is a sawtooth across restarts, and the
  driver's own RSS (up to 2.9 GiB, HWM 4.4 GiB) fell back to 55 MiB later:
  that part is transient (its bounded model pins values while backup
  checks run), not a leak. The server-side growth was not root-caused in
  this run; GC ran with history windows, and topics/history stay in RAM
  until compaction (docs/PRODUCTION_READINESS.md S5.1). A longer soak with
  heap profiles is needed before calling memory bounded.
- One machine, local disk, one seed; no S3, no network partitions.

Status lines every ~10 min at the start:

```
status: 30s elapsed, 637906 ops (21171/s), 0 unknown-outcome/errors, 0 kill -9 restarts, 0 full checks, 1 compactions, 0 GCs, 0 backups (0 restored+checked), max server RSS 405 MiB; server RSS 405 MiB, disk 1128 MiB, commit p99 (since start) ≤0.0025s
status: 1h26m10s elapsed, 20255391 ops (3918/s), 340 unknown-outcome/errors, 44 kill -9 restarts, 80 full checks, 205 compactions, 75 GCs, 36 backups (36 restored+checked), max server RSS 2383 MiB; server RSS 1100 MiB, disk 3384 MiB, commit p99 (since start) ≤0.005s
```

## Long deterministic simulator: all OK

4 shards × 150 seeds (100000–100599) × 2000 steps, started 2026-10-29
20:17 IST, finished 22:27–22:51 IST (7817–9067 s per shard):

| shard | acked txns | crashes | takeovers | shreds | rewinds | compactions (gc) | injected store faults |
|---|---|---|---|---|---|---|---|
| 0 | 50,952 | 37,939 | 15,035 | 13,695 | 8,139 | 8,421 (4,473) | 142,704 |
| 1 | 50,024 | 37,962 | 15,093 | 13,419 | 8,261 | 8,388 (4,354) | 133,155 |
| 2 | 51,216 | 37,648 | 14,891 | 13,315 | 8,043 | 8,245 (4,368) | 139,256 |
| 3 | 50,016 | 37,958 | 15,170 | 13,317 | 8,435 | 8,311 (4,392) | 134,754 |

Total: 600 seeds, 202,208 acknowledged transactions, 151,507 crashes,
549,869 injected store faults, no violation. A first attempt (4 × 500
seeds) stopped on storm seeds 100004, 100504, 101004, 101502 with "could
not open after 200 attempts": a harness liveness limit under read-fault
storms (design 18), fixed before this rerun; no safety violation was
reported.
