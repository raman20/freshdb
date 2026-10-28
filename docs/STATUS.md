# Status

Single source of truth for the post-v0.1.0 work items. One line per finished
item: item | commit | metric (from result files) | remaining.

| item | commit | metric | remaining |
|---|---|---|---|
| 1. journal default + migrate | 6e7b172 (+docs) | default URL YCSB A c=1 43.6k ops/s (journal 42.5k, v0.1.0 3.7k); C c=64 1.51M; peak RSS 1.95 GiB; msg p99@1k 11.8–43 ms, disk-noise bound | 8 MiB chunk growth stall on commit path; RSS bound needs item 3 |
| 2. read path / value cache | e12e909 (+docs) | default URL YCSB C c=64 1.51M → 2.24M ops/s, c=1 302k → 426k; B c=64 1.00M → 1.15M; peak RSS 1.95 → 2.12 GiB (cache 64 → 256 MiB, off-heap); aliasing bug fixed | RWMutex reader atomics, 2nd pread per miss, per-read copy GC; v0.1.0 C was 2.92M (all values in RAM) |
