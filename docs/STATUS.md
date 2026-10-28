# Status

Single source of truth for the post-v0.1.0 work items. One line per finished
item: item | commit | metric (from result files) | remaining.

| item | commit | metric | remaining |
|---|---|---|---|
| 1. journal default + migrate | 6e7b172 (+docs) | default URL YCSB A c=1 43.6k ops/s (journal 42.5k, v0.1.0 3.7k); C c=64 1.51M; peak RSS 1.95 GiB; msg p99@1k 11.8–43 ms, disk-noise bound | 8 MiB chunk growth stall on commit path; RSS bound needs item 3 |
