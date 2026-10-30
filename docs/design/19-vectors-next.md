# 19 — Vectors: next steps from the research picks (#3, #7, #9)

Status: **design only; none of this is implemented.** Written at the end
of the component E review (docs/components/vectors.md), 2026-10-30.
Source: docs/research/2026-10-research.md items 3, 7 and 9. Effort
figures are the research note's estimates, not measured.

## Where vectors stand

- Exact search: a float32 slab per (collection, dimension), parallel
  scan. 100k × 128-d top-10: 3.7–3.9 ms on this 8-core box.
- HNSW (opt-in, `EnableANN`): a cache outside the digest, saved only on
  a clean `Close` (design 10). 20k × 128-d with default parameters
  (M 16, efConstruction 100) builds in 17–18 s, single-threaded, and the
  build holds the writer lock when `EnableANN` is called.
- After a crash every vector since the last clean close is re-inserted
  on open (S5.2): about 0.9 ms per 128-d vector here, so ~15 min per 1M.

## #3 Periodic HNSW checkpoints (removes the crash cliff)

Goal: open time after a crash proportional to the vectors written since
the last checkpoint, not since the last clean close.

- Reuse the design 10 objects (`ann/<log>/<lsn>-<epoch>-<part>of<n>`,
  create-only, hash-validated on restore). Restore already handles a
  snapshot older than the head (stale nodes removed, the rest inserted),
  so a checkpoint is the same blob written at a non-final LSN.
- Trigger: every N inserted vectors or T minutes, whichever first; off
  the commit path. The writer takes the view's read lock only for the
  encode (`MarshalANN` is O(nodes + links)); the put and the deletion of
  older snapshots happen without locks.
- Safety: the zombie fence and shred rules of FreshANN apply unchanged
  (a shred deletes all snapshots and stops saves until a rebuild). The
  spec's Save action already allows a save at any time by the owner; a
  model check with saves interleaved with Put/Del/Crash is the first task.
- Test: simulator crash at a random step after a checkpoint, reopen, and
  check `CheckANN`, `ANNLoadStats` (inserted ≈ vectors since the
  checkpoint) and search against the model.
- Cost to watch: encode time and the RLock it holds (measure at 100k and
  1M); object churn on S3 (one blob per checkpoint, older ones deleted).

## #7 RaBitQ 1-bit codes + rerank

Goal: exact-search bandwidth and memory down ~32× before the rerank.

- Opt-in per collection: `ANNOptions{Quant: "rabitq1"}` (stored in the
  manifest like the ANN declaration). Codes are a derived cache next to
  the float slab, never in the digest.
- Rotation: a deterministic random orthogonal matrix seeded from the
  collection name and dimension (fold purity: same log ⇒ same codes).
- Search: scan codes, keep max(k·r, everything inside the estimator's
  error bound), rerank with float32, return exact scores. `SearchExact`
  stays float-only and is the recall reference in tests and benches.
- Risks: recall at low dimension (bound O(1/√D)); the rotation costs
  O(D²) per vector put. A first prototype must report recall@10 and
  time against `SearchExact` at 128-d and 768-d before any API is added.

## #9 Filtered search via doc indexes

Goal: correct top-k under a predicate (post-filtering an ANN top-k
returns fewer or worse hits).

- API: `VecTx.SearchFilter(q, k, Filter{Coll, Field, Value})` reading
  the doc index (`lens/docindex.go`) at the same view, so both lenses are
  at one LSN. In a read-write transaction the filter's collection and the
  vector collection both go in the read set, and the transaction's own
  doc and vector writes are overlaid (as `Find` and `Search` do now).
- Plan by selectivity s = |filtered ids| / |vectors|: below a threshold
  (start at 5 %, to be measured), exact scan of the filtered ids; above
  it, HNSW search that only reports filtered nodes and expands two hops
  through non-matching ones (ACORN-1), falling back to the exact
  filtered scan if fewer than k hits come back (the same rule `Search`
  uses today).
- Test: rapid property against an exact filtered model, including own
  writes and shreds; recall vs exact for s in {0.1 %, 1 %, 10 %, 50 %}.

## Order

#3 first (correctness of open time after a crash; smallest change, reuses
design 10), then #9 (an API users ask for; exact path is easy to make
correct), then #7 (a performance feature that needs a prototype first).
