# Component E: vectors (exact + HNSW)

Review started 2026-10-30 (IST). Code: `VecTx` in `tx.go` (Put, Delete,
Search, SearchExact, SearchANN), `EnableANN` in `branch.go`, `ann.go`
(graph save/load), the vector lens `lens/vector.go`, the graph
`lens/hnsw.go`, its blob format `lens/hnsw_persist.go`, the HTTP handlers
(`/v1/vectors`, `/v1/ann`) in `server/server.go`. **Status: review done;
verdict at the end.** Nothing here claims vectors are production ready.
Next steps from the research picks are in docs/design/19-vectors-next.md
(design only).

## Contract (as now implemented)

1. A vector is a non-empty float32 array with finite, not-all-zero
   components, stored under (collection, id) in space `vec:<coll>`;
   anything else is `ErrBadValue` (HTTP 400). Search compares unit
   vectors (cosine); only vectors of the query's dimension are candidates.
2. `Search(q, k)` returns at most k hits, best first, ties by id, at the
   transaction's snapshot **including its own writes**. `k` larger than
   the candidate count is fine (capped); `ef` must be 0..`MaxEf` (4096).
   A NaN or infinite query is `ErrBadValue`; a zero query finds nothing.
3. Without `EnableANN`, or below `ANNExactBelow` (4096) vectors, search
   is exact. Above it the HNSW graph answers (approximate) and the exact
   scan takes over if the graph returns fewer than k hits.
   `SearchExact` always scans.
4. In a read-write transaction `Search` reads the whole collection for
   conflict checking: a concurrent write to it makes the commit retry.
5. The graph is a cache: not in the digest, saved on clean `Close`,
   validated against the current vectors (hash) on restore, rebuilt if
   unusable. Identical vectors share one node.

## Defects found and fixed

| id | defect | evidence | fix | test |
|---|---|---|---|---|
| E-D1 | `k` and `ef` were unbounded: `newVisited` looped / allocated by them, reachable from HTTP (`k=1e12`, `ef` huge) | code read + probe | `k` capped at candidates (overflow-safe with own writes), `ef` ≤ `MaxEf`, lens caps both by slab size | `TestVectorHugeK` (k = 2^40 and MaxInt in-tx), `TestANNHugeKEf`, `TestHTTPAPI` |
| E-D2 | `ANNOptions` unbounded (`M` 100000 → huge link lists per node) | code read | `M` ≤ `MaxM` (128), ef fields ≤ `MaxEf`, no negatives | `TestVectorBadInput`, `TestHTTPAPI` |
| E-D3 | Bad vectors (empty, NaN, Inf) and bad queries were plain errors (HTTP 500); a zero vector was accepted and then silently dropped by the lens | probe | `ErrBadValue` (400) for all | `TestVectorBadInput`, `TestHTTPAPI` |
| E-D4 | `Search` in a read-write tx ignored the tx's own puts/deletes and was not in the read set (write skew under `Update`) | probe; same shape as D-D1/D-D2 | overlay local writes (scored with the lens's `Dot`), collection prefix read | `TestVectorSearchProperty` (rapid: plain vs ANN DB, in-tx and committed, vs a model), `TestVectorSearchConflicts` |
| E-D5 | Swap-delete left a deleted vector's components and subject in the slab's spare capacity (data meant to be gone after a shred stayed in RAM) | code read | clear the vacated slot | `TestVectorDeleteLeavesNoResidue` |
| E-D6 | `DecodeANN` panicked on crafted blobs (huge counts → makeslice), and restore kept self / repeated / over-cap links | `FuzzDecodeANN` (new) | bound every count by the blob size and `MaxInt32`; restore drops links a correct save never writes | `TestDecodeANNHostile`, fuzz regression seed, `FuzzDecodeANN` |
| E-D7 | `knn` documented "up to k" hits but returns up to max(ef, k) | code read | doc fixed (callers already merged to k) | — |
| E-D8 | More than 2·M identical vectors made an HNSW island: each copy's neighbor list fills with other copies, and no search reaches them | `TestHNSWReachability` (new) | identical vectors share one node (representative + sorted copy list); copies rejoin by hash on restore; `CheckANN` checks the copy bookkeeping | `TestHNSWReachability`, `TestHNSWDuplicates`, `TestCheckANNCatches` |

E-D8 before/after (`TestHNSWReachability`, 2-d vectors, 50 queries):

| data | unreachable nodes before → after | recall@10 before → after |
|---|---|---|
| 5,000 vectors over 35 directions, M 4 | 4,990 → 0 | 0.050 → 1.000 |
| same, M 16 | 4,966 → 0 | 0.122 → 1.000 |
| same with 15,000 puts/deletes of churn | 2,604 of 2,666 → 0 | 0.254 → 1.000 |
| random / clustered 32-d, 10 % exact copies, 2-d random | 0 → 0 | 0.996–1.000 (unchanged) |

Tried first and dropped: making the neighbor heuristic accept ties
(`>=`): unreachable 4,966 → 4,896, no real gain. Near-duplicates (equal
up to rounding, not bit-identical) are separate nodes; a cluster of more
than 2·M of those can still form an island (not measured).

Also checked, no defect: shred order across dimensions (graphs are per
slab, so order is irrelevant; a sort added on suspicion was removed).

## Measurements (2026-10-30, 8-core box, in-memory store, quiet box)

Before = 4cb70d6, after = this change; 5 runs (root) / 3 runs (lens).

| bench | before | after |
|---|---|---|
| `BenchmarkVectorSearch10k` (exact, View) | 369–385 µs | 362–395 µs |
| `BenchmarkVectorSearch100k` (lens exact scan) | 3.80–4.04 ms | 3.71–3.78 ms |
| `BenchmarkVectorSearchANN/view` (20k × 128, HNSW) | 453–474 µs | 432–450 µs |
| `…/update` (read-write tx, no writes) | 446–511 µs | 437–473 µs |
| `…/update+put` (own write overlaid) | 446–466 µs | 437–467 µs |
| `BenchmarkHNSWBuild` (20k × 128, default M 16 / efC 100) | 17.1–17.4 s | 17.8–18.4 s (**+4 %**) |

Search: ranges overlap, no measurable change; the overlay costs
O(own writes) and one prefix read. Build is ~4 % slower: insertion now
searches all levels before linking (to detect a duplicate with the
level-0 result), keeping each level's candidate list alive at once. The
graph produced for distinct vectors is the same as before (the searches
read only the existing graph either way; checked: identical `MarshalANN`
bytes before and after for 3,000 random 16-d puts and deletes).

Note the ANN path at 20k × 128 (~450 µs) is slower than the exact scan of
10k (~370 µs): the scalar `dot` is ~130 ns per 128-d pair and ef 128 ×
M0 32 ≈ 2–3k distance calls per query. HNSW pays off only at larger n
(and the exact scan is parallel; the graph search is not).

## Tests, fuzzing, mutation, coverage

- Unit / property: `vec_tx_test.go` (new): rapid property, plain and
  ANN databases against a model top-k, in read-only and read-write
  transactions, with deletes and shreds and `CheckANN`; conflicts;
  bad input; huge k. Lens: `TestHNSWReachability`, `TestHNSWDuplicates`
  (copies survive representative deletes and restore identically),
  `TestCheckANNCatches` (7 kinds of broken bookkeeping detected),
  existing recall/churn/persist tests. Server: `TestHTTPAPI` 400 cases,
  huge k, put/delete/search.
- Deterministic simulation (`sim/`): already writes and deletes vectors
  under store faults, crashes, takeovers, shreds and compaction, checks
  `SearchExact` against the model fold and `CheckANN` after every
  recovery, and counts ANN restores. Unchanged by this review.
- Fuzz: `FuzzDecodeANN` (new; fixes up the CRC so the structure is
  reached; a decoded graph restored onto random vectors must pass
  `CheckANN` and answer): found E-D6, then 7.3 M execs / 60 s clean after
  the duplicate change. `FuzzDecodeVec` (new).
- Mutation (hand mutants applied in place, file restored after each):
  overlay prefix read, overlay filter, overlay puts, kk bound, k cap,
  ef bound, query finite check, zero-vector check, `MaxM` bound, slab
  clear, subject clear, restore link sanitize, lens k cap, duplicate
  join, representative promotion slot, moved-representative fix, copy
  hits per node, copy attach on restore, `CheckANN` count check:
  **19 killed** (the k cap after adding the in-tx `MaxInt` case; the
  count check after `TestCheckANNCatches`). Equivalent: promoting the
  last copy instead of the first (any copy is a valid representative;
  the first is chosen for determinism). One dead line removed (`remove`
  never sees a node with copies).
- Coverage: `lens/vector.go` 94.7 %, `lens/hnsw.go` 99.2 %,
  `lens/hnsw_persist.go` 92.5 % (lens suite); `VecTx` methods 100 %,
  `search` 97.7 %, `ann.go` 71–100 % per function, server vector
  handlers 71–100 %. Uncovered, justified: `lensView` errors in `search`
  (closed DB, covered for other tx methods); `ann.go` store-error
  branches of save/load/drop (store faults are exercised by the
  simulator's ANN restores, not per branch); `vecDel`'s error return.

## TLA+ alignment

`spec/FreshANN.tla` models graphs as sets of (id, version) nodes and
restore as "keep a node only if it matches the current vector, then
insert the rest", plus the zombie fence and shred rules. Sharing a node
between identical vectors does not change what restore may keep: the
representative is still validated by id and vector hash, and copies are
rebuilt only from current vectors (hash + bitwise equality), never from
the blob. The in-tx overlay and read-set rule (E-D4) is the same
predicate-read mechanism as `Scan` and `Find`; the OCC part (component F)
covers it. No spec change.

## Gaps (honest)

- Crash cliff (S5.2): the graph is saved only on clean `Close`; after a
  crash every vector since then is re-inserted (~0.9 ms per 128-d vector
  here). Periodic checkpoints are designed (19, #3), not built.
- `EnableANN` builds every graph while holding the writer lock (17–18 s
  per 20k × 128 here): writers stall for the build. Single-threaded.
- `Search` conflicts on any write to the collection (coarse; correct).
- No filtered search (19, #9), no quantization (19, #7), no SIMD `dot`.
- Near-duplicate clusters larger than 2·M can still be unreachable.
- Vectors and graphs live in RAM; the vector lens is rebuilt by replay
  on open.

## Verdict

Correct for what it promises after E-D1..E-D8: searches agree with an
exact model (approximate only where documented), including the
transaction's own writes; bad input is a 400; hostile graph blobs and
huge k/ef do not crash; duplicates no longer hide from the graph. Not
production ready as a vector database: the crash cliff, the writer stall
on `EnableANN`, and single-threaded unquantized search are documented
limits, not fixed.
