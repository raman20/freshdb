# freshdb-bench

The benchmark suite for freshDB. It is a separate Go module, so the baselines
(Badger, Pebble, bbolt, SQLite and NATS) never become dependencies of freshDB
itself.

Methodology, hardware and caveats are in
[docs/benchmarks.md](../docs/benchmarks.md). Results are in
[results/](results): one directory per run, with raw JSON and a generated
`results.md`.

```bash
cd bench
scripts/smoke.sh                  # every suite at tiny size, about 1-2 minutes
scripts/run-all.sh                # the full suite, about 1.5-2 hours
go run ./cmd/freshdb-bench report -dir results/<date>   # rebuild results.md
```

| Subcommand | What it measures |
|---|---|
| `calib` | The harness noise floor on this machine: timer lateness, and an open-loop no-op at 20k/s |
| `ycsb` | YCSB A/B/C/D/F, scrambled zipfian (θ=0.99), 1 KB values, closed loop (max) and open loop (at 0.5× max), for `-system freshdb\|badger\|pebble\|bbolt\|sqlite` |
| `msg` | Streaming, OpenMessaging-style: end-to-end latency at fixed publish rates, consumer groups, and max throughput, for `-system freshdb\|nats` |
| `txn` | OCC transfers on hot keys (commit/abort rates, plus a sum invariant), and the multi-lens consume, write and ack transaction (with an exactly-once check) |
| `vector` | recall@10 vs QPS on synthetic clustered vectors against a brute-force flat loop |
| `s3` | Commit latency, batch size, group commit, requests per commit, **estimated** request cost, and cold open vs segment count against an S3 endpoint (`-url` or `FRESHDB_BENCH_S3`) |
| `report` | Merges the `*.json` in a directory into `results.md` (every run, no filtering) |

Run `freshdb-bench <subcommand> -h` for flags. Every result file records the
exact command, the seed, durations, the Go version, CPU, kernel, disk, the
freshDB commit and the load average at start.
