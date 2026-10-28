# Roadmap

freshDB is **pre-1.0**. This roadmap is a statement of intent, not a promise.
There are no dates. Items move when there is evidence (users, benchmarks,
bug reports). Everything must respect the [design principles](docs/design-principles.md).

## Now — v0.1.x: make the prototype honest and usable

- [x] One log; KV, topics, consumer groups, documents, vectors as lenses
- [x] Serializable single-writer transactions; exactly-once consume + write + ack
- [x] Object storage with create-only fencing (S3/MinIO, local dir)
- [x] Branches, rewind, promote, time travel
- [x] Crypto-shred with signed certificates; `verify`
- [x] TLA+ specs + TLC in CI; deterministic simulator; property tests; fuzzing
- [x] Journal layout (one append-only file per local DB) as the default on
  Unix, with `freshdb migrate` for v0.1.0 directories (unreleased)
- [ ] Test on AWS S3 and Cloudflare R2 (only MinIO is tested today)
- [ ] `--kek` / `FRESHDB_KEK` for the CLI and server (the Go API has `WithKEK`)
- [ ] Prebuilt binaries for every release, and checksums

## Next — v0.2–v0.4: make it last

- **Compaction follow-ups.** Compaction (base images + garbage with a
  grace period) is on `main`. Still open: collecting old manifests and
  fences (needs a fencing argument in TLA+), compacting without pausing
  commits, shrinking the journal file itself, retention policies (keep N
  hours of time travel), and topic retention.
- **Persisted lens snapshots for all lenses.** Today only KV and topics
  checkpoint, and recovery still scans every segment. The goal is open time
  proportional to the tail, not to history.
- **Disk-backed / mmap lenses.** Lens state is entirely in RAM today, which caps
  database size at memory.
- **ANN vector index** (e.g. HNSW or IVF) as a lens, while keeping the
  `lens == fold(log)` property. Today's vector search is exact brute force.
- **KMS key support.** KEK held in AWS KMS / GCP KMS / Vault, never in the
  bucket, plus clear guidance on object versioning for `keys/`.
- **Auth and TLS** for `freshdb serve` (token auth first).
- **Read replicas without Raft.** Processes that tail the log from object
  storage and serve reads at a known LSN. They need no consensus, because the
  log on S3 is already the source of truth.

## Later — exploratory

- **SQL layer** outside the core (see "What we refuse to build"). Likely
  read-only, over lenses or exports.
- **Lakehouse export:** an Iceberg/Parquet export lens keyed by LSN range, so
  history is queryable from DuckDB/Spark.
- **Replication / multi-writer.** Only if a design keeps the single-log
  simplicity. Sharding by database comes first.
- A public lens plugin API.

## Good first issues

These are well-scoped, need no deep knowledge of the protocol, and have clear
tests. Comment on (or open) an issue before starting, so work is not duplicated.

1. **`freshdb version` in the HTTP API.** Add `GET /v1/version`, with a test in
   `server/server_test.go`.
2. **`--kek` flag / `FRESHDB_KEK` env** for CLI and server, passed to
   `WithKEK`. Hex or base64, with clear errors.
3. **`freshdb scan --keys-only`** and `--limit`.
4. **Health endpoint** `GET /healthz` for `freshdb serve` (no DB call). Useful
   behind load balancers.
5. **`freshdb log --follow`** that tails new entries, like `sub --follow`.
6. **Better CLI errors for bad URLs.** Example: `s3://` without a bucket
   should name the problem and show an example URL.
7. **Docs: a deployment guide for Cloudflare R2.** Try it, write down the
   exact URL and settings, and report whether the S3 tests pass against it.
8. **Benchmark: commit latency on real S3.** Add an opt-in benchmark
   (`FRESHDB_BENCH_S3`) that reports p50/p99 commit latency, and document the
   numbers with the region and instance used.
