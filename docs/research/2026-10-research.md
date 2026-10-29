# freshDB research scan: October 2026

Status: research note, not a design. Written 2026-10-29 (IST). Nothing in it is
implemented. It is not committed; the owner decides whether it goes in.
This note is written to be safe to read without the code open.

**Scope.** These are ideas from 2023–2026 papers, engineering blogs and repos
that bear on freshDB's known weaknesses (docs/PRODUCTION_READINESS.md S2.1,
S2.2, S2.6, S5.1, S5.2; ROADMAP "Next"). I opened every source cited here this
session, apart from a few secondary links (SlateDB checkpoint pages, the
LazyLog PDF, the AWS S3 multi-writer and S3 Vectors blogs, the Antithesis
docs, the Filtered-DiskANN PDF, the Iceberg spec). Those are standard locations
but were not re-fetched. Where a number comes from a vendor's own blog, it is marked
**(vendor)**: those are claims, not independent measurements. Effort estimates
assume one engineer who knows the codebase. They are guesses, ±50%.

**Ground truth about freshDB used here** (from the repo docs, `main` @ 2f7128f):
one writer per DB; OCC that tracks keys, scanned prefixes and cursors
(tx.go:40); group commit = one create-only segment object per batch; the
journal store is the local default; every key, version and segment is indexed
in RAM (design 07: ~2 GiB vs Pebble's ~0.09 GiB steady state at 1M×1 KiB);
large values live in segments and are read on demand (already WiscKey-style
key/value separation); KV and topic checkpoints are optional and the other
lenses replay; HNSW is saved only on clean close (design 10); compaction pauses
commits (S2.2); the simulator's `store.Faulty` models failed, lost, delayed,
duplicated and torn writes.

---

## 1. Ranked shortlist

Ranking = (fit to a weakness freshDB already names) × (strength of evidence) ÷
(cost and risk). Order of the "harden the log first" plan is in §3.

| # | Idea | Component | Evidence | Effort |
|---|---|---|---|---|
| 1 | TigerBeetle-style explicit storage fault model in the simulator | log, store, sim | proven in production (TigerBeetle, FDB) | 1–2 wk |
| 2 | Size- and latency-aware group commit tuned to the object store's request curve | log, S3 | paper (Milliscale 2026) + vendor data | 1 wk |
| 3 | Lens checkpoints for every lens, so open time scales with the tail (includes periodic HNSW save) | log, all lenses | standard technique (ERMIA, SlateDB) | 2–4 wk |
| 4 | Disk/object-backed key index: an LSM of immutable key→version-position runs with in-RAM fences and filters | KV, memory | proven (WiscKey, SlateDB, Pebble); newer variants are papers | 6–10 wk (2–3 wk interim) |
| 5 | Read replicas and warm standby by tailing the log (object store as source of truth, no Raft) | log, HA | proven (SlateDB DbReader, Neon, Litestream) | 2–3 wk |
| 6 | Fast-commit tier: WAL on S3 Express One Zone (optionally a 2-of-3 bucket quorum), sealed into S3 Standard segments | log, S3 | vendor-proven (WarpStream); paper (Milliscale); quorum part unproven for freshDB | 4–6 wk + TLA+ |
| 7 | RaBitQ 1-bit codes + rerank for exact and ANN vector search | vectors, memory | paper (SIGMOD'24) + production (Elastic BBQ, turbopuffer) | 1–2 wk |
| 8 | Partition (IVF/SPFresh-style) vector index stored as immutable objects, as the object-storage ANN path | vectors, S3 | paper (SOSP'23) + production (turbopuffer) | 6–8 wk |
| 9 | Filtered vector search driven by doc indexes (pre-filter bitmaps; ACORN-style for HNSW) | vectors, docs | paper (SIGMOD'24) | 2–3 wk |
| 10 | Aria-style deterministic reordering inside a commit batch to cut OCC aborts | txns, writer | paper (VLDB'20) | 2–3 wk + TLA+ |
| 11 | Non-blocking compaction (build bases off the writer, swap via manifest) | log, KV, topics | proven (SlateDB, every LSM) | 3–4 wk |
| 12 | Lakehouse export: compaction also emits topic/doc bases as Parquet + Iceberg v3 metadata | topics, docs | proven (Redpanda Iceberg Topics) | 3–5 wk |

### 1. Explicit storage fault model in the simulator (TigerBeetle/VOPR style)

**What it is.** TigerBeetle writes down which faults the disk may produce
(silent corruption, misdirected I/O, latent sector errors, gray failure) and
makes the simulator inject all of them on both reads and writes. Its VOPR
picks read and write fault rates of 0–10%, misdirect rates of 0–10% and crash
rates of 80–100% per seed, and keeps a "fault atlas" so it never corrupts the
only copy of something when that would make recovery impossible by design.
"Protocol-aware recovery" (FAST'18) adds the point that recovery has to know
which records may already be committed. A corrupt record that might be
acknowledged must never be silently truncated.
- TigerBeetle safety doc (fault rates per year, PAR, O_DIRECT): https://docs.tigerbeetle.com/concepts/safety/
- VOPR parameters: https://github.com/tigerbeetle/tigerbeetle/blob/main/src/vopr.zig
- Architecture / PAR NACK protocol: https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/ARCHITECTURE.md
- Alagappan et al., Protocol-Aware Recovery, FAST'18: https://www.usenix.org/conference/fast18/presentation/alagappan
- FoundationDB SIGMOD'21 (simulation as the primary test method): https://www.foundationdb.org/files/fdb-paper.pdf
- Go 1.25 `testing/synctest` GA (fake clock "bubbles"): https://go.dev/doc/go1.25 , https://go.dev/blog/testing-time

**Mapping.** `store.Faulty` and `sim/`. freshDB already made the
PAR-compatible call (S1.3/S1.4: mid-log damage is `ErrCorrupt`, never
repaired). But the simulator does not inject **read-path** faults: a bit flip
on GET, a stale or short LIST, a ranged GET that returns the wrong range, a
misdirected journal read. It also leaves out an object that was durable and
later rots (S1.7: lost newest segments). Those are exactly the faults that
`verify`, the value cache CRC and the open path claim to handle.

**Benefit.** It turns the S1 claims ("detected, never silently dropped") into
properties checked on every seed. It is the cheapest way to find the next
S1.3-class bug. Go's `synctest` gives deterministic time for the writer's
batching, backoff and notify paths without rewriting them against a fake
clock interface.

**Cost/risk.** Low. The main risk is a flood of "expected" failures. That
needs an oracle: a fault the model allows must give `ErrCorrupt` or a correct
answer, never a wrong answer.

**Sketch.** (a) Add a `ReadFault` mode to `Faulty`: flip a bit, truncate,
return a neighbouring object, or return a stale LIST that omits the newest k
keys. (b) Add post-ack rot: after an ack, corrupt or delete a random
committed object with a small probability, and track it in a fault atlas
like VOPR's. (c) Oracle: every read returns the model's value or a typed
error, and Verify reports every rotted object. (d) Swarm testing: per seed,
switch fault classes on and off (VOPR picks rates per seed). (e) Move the
writer's timers under `synctest` in unit tests. (f) Run nightly with a seed
corpus. **1–2 weeks.**

### 2. Size- and latency-aware group commit

**What it is.** Milliscale (arXiv, March 2026) measured S3 Express One Zone
append latency on EC2. It stays flat at about 8 ms from 128 to 512 KB and
grows linearly above about 1 MB. A GET is about 5 ms up to 256 KB. Their
best-tail configuration is a shared 1 MB buffer per two threads, and they
track commit dependencies per record. That cut p99.9 commit latency by about
52–62% against naive S3 Express use, and reached gp3-like p99.99. WarpStream
(vendor) reports the same trade-off: batch timeout from 250 ms down to 25 ms,
with more PUT cost.
- Milliscale: https://arxiv.org/html/2603.02108v1 (code: https://github.com/sfu-dis/milliscale)
- WarpStream low-latency doc (cost arithmetic, S3 Express PUTs ~1/5 the price): https://docs.warpstream.com/warpstream/kafka/advanced-agent-deployment-options/low-latency-clusters
- S3 Express price cut, April 2025 (PUT $0.00113/1k, GET $0.00003/1k): https://aws.amazon.com/blogs/aws/up-to-85-price-reductions-for-amazon-s3-express-one-zone/

**Mapping.** The writer's batching (`writer.go`). docs/s3.md measured 1 PUT
per serial commit and 0.06 PUTs per commit with 32 writers. freshDB is
already single-buffer group commit, so Milliscale's main gain (fewer
decentralized log buffers) does not apply. The transferable part is the
**policy**: close a batch at whichever comes first of (a) the store's
"free-size" knee, about 256 KB–1 MB on S3 Express, or (b) a latency budget.
Learn the knee per store at open (`s3check` already issues PUTs and could
time them).

**Benefit.** Fewer PUTs per commit at the same latency, and a documented
knob. Small but cheap. It is also a prerequisite for #6.

**Cost/risk.** Low. The flush timing must stay deterministic in the
simulator: inject the clock.

**Sketch.** `WithCommitPolicy{MaxDelay, TargetBytes}`, with defaults per
store type (journal: flush now; S3: 512 KB or 10 ms). Expose batch-size and
PUT-latency histograms through `/metrics`. Benchmark against docs/s3.md.
**About 1 week.**

### 3. Checkpoints for every lens; open time ∝ tail; periodic HNSW save

**What it is.** A standard memory-optimized engine checkpoint (ERMIA, as
used by Milliscale §4.4): an MVCC snapshot at timestamp t is written without
blocking, then the log before it can be skipped at recovery. SlateDB has
explicit checkpoints that readers and clones pin.
- Milliscale §4.4 (ERMIA checkpointing): https://arxiv.org/html/2603.02108v1
- SlateDB checkpoints and clones: https://slatedb.io/docs/design/checkpoints/ , https://slatedb.io/rfcs/0004-checkpoints/

**Mapping.** ROADMAP "Persisted lens snapshots for all lenses", S2.6 (open
reads every segment), S5.2 (a crash re-inserts every vector since the last
clean close). Today only KV and topics checkpoint, and only when turned on.

**Benefit.** Bounded open and takeover time, which #5 needs for failover. It
also removes the HNSW crash cliff by saving the graph blob every N inserts or
T minutes, with the same create-only `ann/` objects keyed by LSN.

**Cost/risk.** Medium. Checkpoints must stay *optional caches*: the lens
digest has to equal the fold. The test is the existing purity test with
"checkpoint at a random LSN, reopen, compare digests", run in the simulator.
Shred must invalidate them; design 10 already does this for ANN blobs.

**Sketch.** A common `lens.Checkpointer` interface (encode at LSN, decode,
digest). The writer schedules it off the commit path from a `ViewAt(lsn)`.
The manifest is not involved: the newest valid checkpoint ≤ head is found by
LIST, as for `ann/`. **2–4 weeks.**

### 4. Object-backed key index: LSM of key-index runs (the S5.1 fix)

**What it is.** freshDB already separates values from keys, as WiscKey does:
values stay in log segments and the lens holds positions. What WiscKey and
every LSM do next is keep the **key → position** map itself in sorted,
immutable runs on storage. RAM then holds only a small memtable of recent
keys, per-run fence pointers (one key per block) and per-run filters. SlateDB
is this design on object storage, with a single writer and fencing. Newer
work shows how to shrink the RAM side further:
- WiscKey (FAST'16): https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu
- SlateDB (object-store LSM, single writer, fencing, readers; v0.17 Sep 2026; distributed compaction RFC-0025): https://slatedb.io/docs/get-started/introduction/ , https://slatedb.io/docs/design/overview/ , https://github.com/slatedb/slatedb/releases/tag/v0.15.0
- SplinterDB + maplets (SIGMOD'23): compact, mergeable per-run metadata, so metadata can be compacted eagerly while data is compacted lazily: https://dl.acm.org/doi/10.1145/3588726
- F2 (PVLDB 18, 2025): a two-level hash index for cold keys (a small RAM table of chunk pointers, with chunks on disk) plus a hot in-memory log: https://www.vldb.org/pvldb/vol18/p4910-kanellis.pdf
- Bf-Tree (PVLDB 17, 2024): variable-size mini-pages cache single records, not whole pages: https://vldb.org/pvldb/vol17/p3442-hao.pdf
- SuRF / Fast Succinct Trie (SIGMOD'18), about 10 bits per trie node, supports range filters: https://db.cs.cmu.edu/papers/2018/mod601-zhangA-hm.pdf

**Mapping.** The KV lens (`lens/kv.go`), the journal object index and the
segment index (design 07 table). The runs can *be* the KV lens checkpoint
(#3). A run written at LSN L is the fold of the log up to L, restricted to
key positions, so purity is kept.

**Benefit.** RAM grows with the *hot* key set and the filter size, not with
every key and version. Typical LSM filters cost about 10 bits per key. Fence
pointers cost about one key per 4–64 KB block. My estimate, not measured:
for 1M keys that is low tens of MiB of index RAM, against design 07's 313 MiB
of versions and map. Open time drops to the tail (#3).

**Cost/risk.** High. This is the largest item. Reads of cold keys gain one or
more storage round trips: about 5 ms on S3 Express, ~10–100 ms on S3 Standard
(Milliscale Fig. 2; turbopuffer architecture doc). It needs a local
disk/NVMe cache for runs when the store is S3. Time travel (`ViewAt`) must
search runs with version history, so runs keep all versions above the
compaction base. Compaction of runs is new background work and must not
pause commits (#11).

**Interim (2–3 weeks).** Keep the index in RAM but **compress cold
history**. Freeze key ranges older than the last checkpoint into immutable,
prefix-compressed sorted blocks (front coding; or an FST for the key part),
and keep a mutable map only for keys written since. Gain is likely 3–5× on
the version/map line of design 07. That is a guess: measure it with the
10M-key benchmark the ROADMAP names. It needs no new I/O path.

**Full sketch (6–10 weeks).** (1) Run format: sorted (key, lsn, pos, flags)
blocks of 16–64 KB, a block index and a filter (bloom or ribbon; SuRF if
range-filtering matters), CRC per block. (2) Write runs as create-only
objects from a frozen memtable. A manifest entry lists the runs for each log
(the existing create-only manifest is the commit point). (3) The read path
checks the memtable, then the runs newest-first. Filters live in RAM; blocks
go through the existing value-cache arena. (4) Background merge of runs
(size-tiered first), swapped in by a manifest bump with the existing garbage
grace period. (5) TLA+: extend FreshCompact with run swap. (6) The 10M-key
benchmark is the acceptance test.

### 5. Read replicas and warm standby by tailing the log (no Raft)

**What it is.** If the object store is the source of truth and the writer
is fenced, replicas need no consensus. They LIST and GET new segments and
fold them. SlateDB ships this as `DbReader`, which can follow the latest
manifest or pin a checkpoint for GC safety. Neon's pageservers consume WAL
that safekeepers made durable. Litestream 0.5.8 (Feb 2026) added
S3-conditional-write leases for failover.
- SlateDB readers: https://slatedb.io/docs/design/readers/
- Neon architecture: https://neon.com/docs/introduction/architecture-overview
- Litestream leasing (v0.5.8): https://github.com/benbjohnson/litestream/releases/tag/v0.5.8 , https://fly.io/blog/litestream-revamped/
- Aurora DSQL: storage replicas apply a journal of *committed* transactions with no 2PC or consensus: https://brooker.co.za/blog/2024/12/05/inside-dsql-writes/

**Mapping.** ROADMAP "Read replicas without Raft". S2.1 (no HA) becomes
*fast failover*. A standby tails the log, and on writer loss it opens
(fence = create manifest N+1). Its outage is about one fence plus the
un-tailed suffix, not a full replay.

**Benefit.** Read scale-out and an HA story without a new consensus
protocol. It reuses the proven fencing argument (spec/FreshLog.tla).

**Cost/risk.** Medium. A replica must never read a segment the GC deleted,
so it needs a pin: SlateDB uses checkpoints, and freshDB's grace period plus
lease records are a start. It must also never expose a branch state that a
rewind later moved. Takeover still needs an external decision about who
opens; a lease object with a TTL works, as in Litestream.

**Sketch.** `OpenReader(url, at)`: poll LIST (or `WaitHead` over HTTP) and
fold. `serve --follow`. A pin object `pin/<reader>` with an expiry that GC
respects. TLA+: add a reader to FreshGC. **2–3 weeks**, plus 1 week for
standby promotion.

### 6. Fast-commit tier: S3 Express One Zone WAL, sealed into S3 Standard

**What it is.** Land each commit batch in a low-latency, single-AZ bucket
first, and later "seal" batches into normal segments in a regional bucket.
WarpStream (vendor) does this: about 20 ms to write "a quorum of S3EOZ
buckets", p50 under 35 ms and p99 under 50 ms produce latency with Lightning
Topics. Milliscale replicates appends to a list of buckets and commits on
"all or a majority". S3 conditional writes now work on directory buckets:
`If-None-Match` since Aug 2024 and `If-Match` CAS since Nov 2024.
- WarpStream Lightning Topics / LazyLog (Feb 2026): https://www.warpstream.com/blog/the-art-of-being-lazy-log-lower-latency-and-higher-availability-with-delayed-sequencing
- LazyLog (SOSP'24): https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf
- S3 conditional writes: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html , https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/
- S3 Express durability is single-AZ: "designed for 11 nines" within the AZ, but data can be lost if the AZ is destroyed: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.md
- S3 Express append (10,000 parts per object, each billed as a PUT): https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html ; critique: https://blog.astradot.com/s3-express-append-has-issues/
- EBS durability, for comparison: gp3 99.8–99.9%, io2 Block Express 99.999%: https://docs.aws.amazon.com/ebs/latest/userguide/ebs-volume-types.html

**Mapping.** The store layer and the writer. It answers the "S3 latency
forces batched commits" weakness.

**Benefit.** Commit latency drops from roughly 100 ms-class to about 10 ms
class on AWS, with object-store operability: no EBS, multi-attach not needed.

**Cost/risk.** **High, and the quorum part is unproven for freshDB.**
- One S3 Express bucket only: simple, since it is the same create-only
  primitive, but an AZ loss can lose acknowledged commits. That is a
  different durability promise and must be opt-in and documented.
- A 2-of-3 bucket quorum across AZs: a create-only object per (log, seq) in
  each bucket, committed iff one writer won a majority. This is a
  Disk-Paxos-like register built from three CAS registers. Two fenced
  writers can split the buckets 1-1-1, and recovery with one bucket down
  can be ambiguous. It is the same class of race TLC already found once in
  freshDB's open path (architecture.md step 4). It **must** be model-checked
  before code.
- Cost: every commit becomes 2–3 PUTs plus a later seal. Use #2's arithmetic.

Not recommended: S3 Express *append* as the log format. The 10,000-part
limit and the fact that each append is billed as a PUT remove most of the
benefit (astradot), and freshDB's one-object-per-batch is already the
simpler shape.

**Sketch.** (1) TLA+ first (`FreshQuorum.tla`): writers with epochs, three
create-only stores, lost responses, a bucket outage, a seal step, and the
invariant that an acked transaction is never lost and never doubled. (2) A
`store.Quorum` wrapper exposing `PutIfAbsent` with majority semantics, plus
read repair. (3) A sealer that copies sealed ranges into regular segments in
the regional bucket and garbage-collects the WAL, reusing compaction's
grace-period garbage. **4–6 weeks including the model.** Do it only after
#1–#5.

### 7. RaBitQ codes + rerank for vector search

**What it is.** RaBitQ (SIGMOD'24) stores D bits per D-dimensional vector,
using a random rotation and a sign bit per dimension. It gives an unbiased
distance estimator with an error bound of O(1/√D). Extended RaBitQ (SIGMOD'25)
generalizes this to B bits per dimension. Search uses the estimate plus the
error bound to pick candidates, then reranks them with full-precision
vectors. Production adopters include Elastic (BBQ, a RaBitQ-derived variant)
and turbopuffer ANN v3 (vendor: "<1% of vectors need reranking").
- RaBitQ: https://arxiv.org/abs/2405.12497 ; Extended RaBitQ: https://dl.acm.org/doi/10.1145/3725413
- Elastic BBQ: https://www.elastic.co/search-labs/blog/better-binary-quantization-lucene-elasticsearch
- turbopuffer ANN v3 (May 2026): https://turbopuffer.com/blog/ann-v3

**Mapping.** The vector lens (`lens/vector.go`: float32 slab, exact parallel
scan) and HNSW (`lens/hnsw.go`: distance calls during search).

**Benefit.** A 1-bit code slab is 32× smaller than float32, so exact search
becomes "scan codes, rerank the top few %", bounded by memory bandwidth. The
codes are also the natural payload for #8. The HNSW graph can navigate with
the codes and rerank at the end.

**Cost/risk.** Low to medium. Recall drops at low dimension because the
bound is O(1/√D). Keep exact search as the reference, as design 04 does. The
rotation must be deterministic, seeded from collection metadata, to keep
fold purity. The codes are a derived cache, outside the digest.

**Sketch.** Per collection, opt in with `Quant: "rabitq1"`. Keep a code slab
next to the float slab. `Search` runs the code scan, takes the top
max(k·r, bound-filtered set), then does an exact rerank. Report recall
against `SearchExact` in the benchmarks. **1–2 weeks.**

### 8. Partition (IVF/SPFresh-style) vector index on immutable objects

**What it is.** SPFresh (SOSP'23) is a centroid/partition index that updates
in place. Its LIRE protocol splits and merges partitions locally and
reassigns boundary vectors, so it never rebuilds the whole index.
turbopuffer runs a hierarchical SPFresh derivative on object storage. Cold
queries cost 3–4 round trips, because the tree depth bounds the round trips,
whereas graph indexes walk the graph serially (vendor, but the reasoning is
sound). For disk-resident graphs, see FreshDiskANN and LSM-VEC (2025, LSM
storage for an HNSW-style graph).
- SPFresh: https://www.microsoft.com/en-us/research/wp-content/uploads/2023/08/SPFresh_SOSP.pdf
- turbopuffer architecture: https://turbopuffer.com/docs/architecture
- FreshDiskANN: https://arxiv.org/abs/2105.09613 ; LSM-VEC: https://arxiv.org/abs/2505.17152
- AWS S3 Vectors GA (Dec 2025; sub-second cold, about 100 ms warm): https://aws.amazon.com/blogs/aws/amazon-s3-vectors-now-generally-available-with-increased-scale-and-performance/

**Mapping.** It replaces "HNSW in RAM, rebuilt or saved whole" (S5.2,
roadmap "disk-backed graphs") for large collections. A partition is an
immutable object: centroid, ids, RaBitQ codes and optionally float vectors.
Inserts go to the partition of the nearest centroid. Splits and merges are
deterministic functions of the log (the fold), so the index can be rebuilt
from the log and is cached as objects.

**Benefit.** Vector collections larger than RAM, cheap persistence (only
changed partitions are rewritten), and no full-graph rebuild after a crash.

**Cost/risk.** Medium to high. Recall depends on centroid quality and the
probe count. Deterministic k-means needs a seeded RNG, and its results
differ across compaction replay orders, as with HNSW today: accept this
(it is a cache) or canonicalize the order. Write amplification comes from
splits.

**Sketch.** Phase A: a flat IVF (√N centroids, codes from #7) built at
compaction, with an append-only delta list for new vectors that is searched
exactly (turbopuffer does the same for "unindexed WAL"). Phase B: LIRE-style
local split/merge. **6–8 weeks.** Keep HNSW for in-RAM collections.

### 9. Filtered vector search via doc indexes

**What it is.** ACORN (SIGMOD'24) runs HNSW search over the subgraph of
nodes that satisfy an arbitrary predicate. It uses denser graph construction
(ACORN-γ) or two-hop expansion (ACORN-1), and falls back to pre-filtering
for very selective predicates. Filtered-DiskANN (WWW'23) is the label-aware
alternative.
- ACORN: https://arxiv.org/abs/2403.04871 ; Filtered-DiskANN: https://harsha-simhadri.org/pubs/Filtered-DiskANN23.pdf

**Mapping.** The docs lens (secondary indexes) combined with the vector lens.
Cross-lens queries are where "one log, many lenses" can beat glued systems,
because both lenses are read at one LSN and consistency is free.

**Benefit.** Correct top-k under filters. Post-filtering an ANN top-k
silently returns fewer or worse results.

**Cost/risk.** Low to medium. A planner rule: below selectivity s, scan the
filtered ids exactly (with #7 codes); above it, do a predicate-aware graph or
partition search.

**Sketch.** `VecTx.Search(q, k, Filter{index, value})`. Build the filtered
id bitmap from the doc index at the transaction snapshot, then pick a
strategy by bitmap cardinality. ACORN-1 expansion in `hnsw.go`. Test recall
against exact filtered search. **2–3 weeks.**

### 10. Deterministic reordering inside a commit batch (Aria)

**What it is.** Aria (VLDB'20) runs a batch against one snapshot and then
decides commits deterministically. Its reordering rule allows read-after-write
(RAW) dependencies when there is no write-after-read (WAR) conflict, so it
aborts fewer transactions than plain OCC. This fits a single writer that
already validates a whole group-commit batch.
- Aria: https://vldb.org/pvldb/vol13/p2047-lu.pdf
- Aurora DSQL OCC (commit-time adjudication only, MVCC reads): https://brooker.co.za/blog/2024/12/05/inside-dsql-writes/

**Mapping.** The writer's validation loop (architecture.md "Writer"). Today
conflicts inside a batch move a transaction to the next batch, and after 3
losses the body runs on the writer.

**Benefit.** Fewer aborts and retries on hot keys, so better p99 under
contention. Modest: only worth it if benchmarks show conflict retries in the
tail.

**Cost/risk.** Medium. Serializability proofs get subtle, so it must be added
to FreshTxn.tla (`ExactlyOnce`, `AtomicCursor`) before code. Measure first:
put abort/retry counts in `/metrics` if they are not there yet.

**Sketch.** Build each batch's read/write sets, order the batch by Aria's
rule instead of arrival order, keep the escalation path, and run TLC with
the new order. **2–3 weeks.**

### 11. Non-blocking compaction

**What it is.** Build base images from a pinned `ViewAt(L)` off the writer
goroutine, then commit with one create-only manifest that also replays the
segments after L. Every LSM works this way, and SlateDB now even offloads
compaction to separate workers (RFC-0025, v0.15).
- SlateDB v0.15 release notes (distributed compaction phase 3): https://github.com/slatedb/slatedb/releases/tag/v0.15.0

**Mapping.** S2.2 (compaction pauses commits for 0.87 s and more) and the
ROADMAP's "compacting without pausing commits". It is a prerequisite for the
run merges in #4.

**Cost/risk.** Medium: FreshCompact.tla must model concurrent commits during
the build. **3–4 weeks.**

### 12. Lakehouse export (Parquet / Iceberg v3)

**What it is.** Streaming systems now expose topics as Iceberg tables
directly (Redpanda Iceberg Topics GA, 25.1). Iceberg v3 (ratified May 2025)
adds deletion vectors, row lineage and VARIANT. DuckLake 1.0 (Apr 2026)
keeps the catalog in a SQL database. Lance and Vortex target random access.
- Redpanda Iceberg Topics: https://www.redpanda.com/blog/redpanda-25-1-iceberg-topics-ga
- Iceberg v3 vote: https://www.mail-archive.com/dev@iceberg.apache.org/msg10228.html ; spec: https://iceberg.apache.org/spec/
- DuckLake 1.0: https://ducklake.select/2026/04/13/ducklake-10/
- Lance (adaptive structural encodings, arXiv 2025): https://arxiv.org/html/2504.15247 ; Vortex at LF AI & Data: https://lfaidata.foundation/press-release/2025/08/06/lf-ai-data-foundation-hosts-vortex-project-to-power-high-performance-data-access-for-ai-and-analytics/

**Mapping.** Compaction already writes base images of topics and KV at a
known LSN. Also writing them as Parquet with Iceberg metadata, as a
*derived* export, gives analytics for free.

**Cost/risk.** Medium. Crypto-shredding interacts with it: an exported
plaintext table defeats shred unless export is per-subject encrypted or
regenerated after a shred. That needs a design call. It does not help any
known weakness, so it comes last. **3–5 weeks.**

---

## 2. Considered and rejected (for now)

| Idea | Why not (now) |
|---|---|
| **Raft/Paxos replication of the log** (Neon safekeepers style: https://github.com/neondatabase/neon/blob/main/docs/safekeeper-protocol.md) | freshDB's durability already comes from the object store plus create-only fencing. Raft adds a second source of truth and a new protocol to model. #5 (tailing) and #6 (bucket quorum) get HA and latency with less. Revisit only if sub-ms commits on local disks become a goal. |
| **Multi-writer per database** via S3 `If-Match` CAS on a shared manifest | Per-object CAS gives optimistic coordination of rare metadata updates (AWS blog: https://aws.amazon.com/blogs/storage/building-multi-writer-applications-on-amazon-s3-using-native-controls/), not a fast shared log. WarpStream needs a central sequencer for exactly this reason. Partition across databases instead, as SlateDB suggests. |
| **Learned indexes** for the key index | A 2025 evaluation in LSM systems: gains shrink once prediction reaches block granularity, scans erase them, and updates hurt robustness (https://arxiv.org/abs/2506.08671 ; robustness study https://dl.acm.org/doi/10.1145/3749188). Fence pointers plus filters are simpler and proven. |
| **Custom Bε-tree / Bf-Tree** in place of an LSM of runs | Both assume in-place page updates on a local block device. freshDB's storage is immutable objects, which LSM runs match. Bf-Tree's mini-page idea can inform the *cache* later. |
| **S3 Express append as the log format** | 10,000 parts per object, each append billed as a PUT, and the reported consistency pitfalls before If-Match (https://blog.astradot.com/s3-express-append-has-issues/). One object per batch is simpler and already proven in freshDB. |
| **LazyLog / Lightning-style "ack before sequencing"** | freshDB's single writer assigns LSNs locally, so sequencing is free. The win in WarpStream comes from removing a *remote* sequencer hop. It would also weaken external consistency (WarpStream's own caveat). |
| **DiskANN-style graph directly on S3** | A cold graph walk needs serial round trips. Partition indexes bound them (turbopuffer doc). Use graphs only when cached in RAM or on NVMe. |
| **Delegating vectors to AWS S3 Vectors** | It breaks "lens = pure fold(log)", branches, time travel and shredding, and ties freshDB to AWS. Useful as a latency baseline only. |
| **CRDT core / local-first sync** (Automerge 3, July 2025, >10× less memory: https://automerge.org/blog/automerge-3/ ; Graft: https://graft.rs/docs/about/) | It is a different consistency model: merge, not serializable OCC. freshDB's branches already allow "fork, work offline, promote". A future "sync a branch to the edge" feature could borrow Graft's pull-only-changed-pages idea, but it is not on the critical path. |
| **Prolly-tree / content-addressed bases for branch diff and merge** (Dolt: https://www.dolthub.com/docs/architecture/storage-engine/prolly-tree/) | Nice for cell-level merge. freshDB has no merge feature, and its bases would need re-chunking. Park until merge is a requirement. |
| **Antithesis** (deterministic hypervisor: https://antithesis.com/docs/introduction/how_antithesis_works/) | It is a commercial service. freshDB's own simulator plus #1 covers the same ground for an embedded Go engine. Consider it later for `serve` plus a real S3 client end to end. Turso uses both its own DST and Antithesis (https://github.com/tursodatabase/turso/blob/main/testing/simulator/README.md). |
| **Adopting Kafka diskless (KIP-1150) designs wholesale** | KIP-1150 (accepted 2 Mar 2026) is only a motivational umbrella. The implementation KIPs 1163 and 1164 were still under discussion when I read them (https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150%3A+Diskless+Topics). Nothing to copy yet beyond what WarpStream documents. |
| **Becoming a broader multi-model / SQL engine** | History is poor. Fauna shut down its service on 30 May 2025 (https://fauna.com/blog/the-future-of-fauna). Multi-model systems tend to lose to specialists on each workload. freshDB's defensible niche is *cross-lens atomicity on one log* (exactly-once consume+write+ack, filtered vector search at one LSN), not breadth. Add lenses only when a cross-lens guarantee needs them. |

Also noted, no action: Bufstream's Jepsen report (Nov 2024) found lost
committed transaction writes and spurious offsets in a Kafka-on-object-storage
system (https://jepsen.io/analyses/bufstream-0.1.0). It is a reminder that
#1 and TLA+ belong before the latency features, not after. OceanBase Bacchus
(PVLDB 19) and the VLDB Journal "OLTP in the cloud" study
(https://link.springer.com/article/10.1007/s00778-025-00913-z) are useful
background on cost models. I did not mine them for specific techniques.

---

## 3. Recommended sequencing

This follows the plan "harden the log first, then KV, topics, docs, vectors,
txns, one at a time". Each step ends with its TLA+ or simulator gate and a
benchmark row, as items do today.

**Phase 0 — log (now).**
1. #1 storage fault model: read faults, post-ack rot, swarm, `synctest`. Gate: N nightly seeds clean.
2. #2 commit policy (bytes/latency knee, metrics).
3. #3 checkpoint framework, used first by the log (open ∝ tail) and HNSW periodic save, which removes the S5.2 crash cliff early at low cost.
4. #11 non-blocking compaction (FreshCompact extended). It unblocks #4.
5. #5 read replicas and warm standby (FreshGC + reader pin).
6. #6 fast-commit tier: *model only* in this phase (`FreshQuorum.tla`). Implement after Phase 1, if AWS latency is a real user ask.

**Phase 1 — KV.** #4 interim (compressed cold history, measured with the
10M-key benchmark), then the full run-based key index on the #3 and #11
machinery.

**Phase 2 — topics.** Topic retention plus checkpoints via #3. Replicas (#5)
serve consumers. The #12 export of topic bases is optional and goes here
only if analytics is requested.

**Phase 3 — docs.** Doc index checkpoints (#3), and build the filter bitmaps
that #9 needs.

**Phase 4 — vectors.** #7 RaBitQ (cheap, immediate memory and bandwidth
gain), then #9 filtered search, then #8 partition index for larger-than-RAM
collections.

**Phase 5 — txns.** Measure abort and retry tails first. Do #10 Aria
reordering only if they matter, with FreshTxn updated first.

**What I am least sure of.**
- #4's memory gains are an estimate until the 10M-key benchmark runs.
- #6's quorum protocol is my own sketch, not a published design for this
  setting. WarpStream does not document its quorum rule. Treat it as
  research.
- Vendor latency numbers (WarpStream, turbopuffer) are self-reported.
  Milliscale's were measured on one EC2 instance type, in one region, in
  early 2026.
- RaBitQ's recall advantage is weakest at low dimension. Verify on
  freshDB's own benchmark vectors.
