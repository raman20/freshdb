# Soak / long simulator (item 18) — partial, results pending

6 h real-time soak: `freshdb-soak -duration 6h -seed 7`, started 2026-10-29 20:00 IST, 8 workers, ext4 VM disk, loopback. Final line will be in /workspace/soak6h.out on the test box (SOAK OK / SOAK FAIL). Status lines so far (every ~10 min):

```
status: 30s elapsed, 637906 ops (21171/s), 0 unknown-outcome/errors, 0 kill -9 restarts, 0 full checks, 1 compactions, 0 GCs, 0 backups (0 restored+checked), max server RSS 405 MiB; server RSS 405 MiB, disk 1128 MiB, commit p99 (since start) ≤0.0025s
status: 16m53s elapsed, 7046612 ops (6959/s), 102 unknown-outcome/errors, 10 kill -9 restarts, 17 full checks, 34 compactions, 12 GCs, 7 backups (7 restored+checked), max server RSS 589 MiB; server RSS 426 MiB, disk 14676 MiB, commit p99 (since start) n/a
status: 32m39s elapsed, 10565343 ops (5394/s), 159 unknown-outcome/errors, 18 kill -9 restarts, 32 full checks, 75 compactions, 27 GCs, 14 backups (14 restored+checked), max server RSS 1297 MiB; server RSS 666 MiB, disk 22091 MiB, commit p99 (since start) ≤0.005s
status: 50m20s elapsed, 14075390 ops (4661/s), 227 unknown-outcome/errors, 27 kill -9 restarts, 47 full checks, 120 compactions, 43 GCs, 20 backups (20 restored+checked), max server RSS 1346 MiB; server RSS 654 MiB, disk 2413 MiB, commit p99 (since start) ≤0.01s
status: 1h6m36s elapsed, 17124741 ops (4285/s), 274 unknown-outcome/errors, 35 kill -9 restarts, 62 full checks, 162 compactions, 58 GCs, 27 backups (27 restored+checked), max server RSS 1650 MiB; server RSS 960 MiB, disk 2787 MiB, commit p99 (since start) ≤0.025s
status: 1h26m10s elapsed, 20255391 ops (3918/s), 340 unknown-outcome/errors, 44 kill -9 restarts, 80 full checks, 205 compactions, 75 GCs, 36 backups (36 restored+checked), max server RSS 2383 MiB; server RSS 1100 MiB, disk 3384 MiB, commit p99 (since start) ≤0.005s
```

Long simulator: 4 shards × 150 seeds (100000–100599) × 2000 steps, started 20:17 IST; output /workspace/simlong/shard*.out, pending. First attempt (4 × 500 seeds) failed on storm seeds 100004, 100504, 101004, 101502 with 'could not open after 200 attempts' (harness liveness, see design 18); no safety violation was reported.
