# Status

Single source of truth for the post-v0.1.0 work items. One line per finished
item: item | commit | metric (from result files) | remaining.

| item | commit | metric | remaining |
|---|---|---|---|
| 1. journal default + migrate | 6e7b172 (+docs) | default URL YCSB A c=1 43.6k ops/s (journal 42.5k, v0.1.0 3.7k); C c=64 1.51M; peak RSS 1.95 GiB; msg p99@1k 11.8–43 ms, disk-noise bound | 8 MiB chunk growth stall on commit path; RSS bound needs item 3 |
| 2. read path / value cache | e12e909 (+docs) | default URL YCSB C c=64 1.51M → 2.24M ops/s, c=1 302k → 426k; B c=64 1.00M → 1.15M; peak RSS 1.95 → 2.12 GiB (cache 64 → 256 MiB, off-heap); aliasing bug fixed | RWMutex reader atomics, 2nd pread per miss, per-read copy GC; v0.1.0 C was 2.92M (all values in RAM) |
| 3. compaction | b873ae3, 09773ea (+docs) | 9.8 MiB live of 488 MiB written: journal disk 504 → 34 MiB (file size unchanged, holes), open 1,626 → 30 ms, heap 54 → 2.8 MiB; fs 5,004 files → 9; compact 0.87 s | manifests/fences never collected; commits pause during compact; no retention windows; journal file never shrinks |
| 4. HNSW (opt-in ANN) | a96d780, ec468d5 (+docs) | 128-d clustered, recall@10 / QPS c=1 at ef 64: 100k 0.988 / 3.3k (exact 910), 1M 0.804 / 1.1k (ef 128: 0.919 / 676; exact 64); build=reopen 23 s at 100k, ~17 min at 1M | graph not persisted (rebuilt on open); single-threaded insert; 1M needs ef/efC tuning |
| 5. auth/TLS + key provider | 09001a3 | bearer token +0.06–0.1 µs on a ~2 µs in-process GET; non-loopback bind without token refused; wrong KEK → ErrWrongKEK at open (keys/_kekcheck); TLS 1.2+ | one shared token, no scopes/mTLS/cert reload; no rekey (dev-key DBs stay dev); no built-in KMS client; TLS cost not measured |
