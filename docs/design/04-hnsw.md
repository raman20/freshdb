# 04 — HNSW vector index (item 4)

## Goal
Sub-linear approximate top-k for large vector collections, opt-in per
collection, without weakening any existing guarantee. Exact brute force
stays the default, the fallback, and the reference for recall.

## Shape
- Pure Go, no dependencies. Standard HNSW (Malkov & Yashunin): layered
  proximity graphs, greedy descent from a top-level entry point, beam
  search (`ef`) at layer 0, a new node's neighbors chosen by the paper's
  heuristic (keeps diverse neighbors), max `M` links above layer 0 and
  `2M` at 0. When an existing node's list overflows it is re-pruned with
  the same heuristic (see the outcome: keeping only the closest was
  faster but stranded clusters).
- One graph per (collection, dimension) slab, inside the vector lens.
  Nodes point at slab slots (the slab's swap-delete updates the mapping),
  so vectors are not copied: memory is the links (~`2M`+ int32 per node).
- **Deterministic**: a node's level comes from a hash of its id, not
  from a global RNG, and nodes are inserted in log order; the same history
  gives the same graph. Results may still differ between two histories
  with the same final vectors (e.g. after compaction, which replays in a
  different order). The graph is a cache: the lens digest covers the
  vectors only, exactly as before.
- **Deletes** (delete, overwrite, shred) unlink the node immediately and
  repair each former neighbor by re-selecting from its own and the deleted
  node's links. Nothing deleted or shredded stays in the graph or can be
  returned; slots are freed as today.

## Opt-in and API
- `db.EnableANN(ctx, coll, ANNOptions{M, EfConstruction, EfSearch})`
  records the declaration in the manifest (like `RegisterIndex`), builds
  graphs in every live branch view, and every later open builds them after
  replay. Time-travel views (`ViewAt` below the head, `ViewLog`) never
  build a graph: they scan exactly.
- `VecTx.Search` uses the graph when the collection has it and the slab
  holds at least `ExactBelow` (default 4,096) vectors, else exact.
  `VecTx.SearchExact` always scans; `SearchANN(q, k, ef)` sets the beam.
  HTTP: `{"exact": true, "ef": N}` on search and `POST /v1/vectors/{c}/ann`;
  CLI: `freshdb vec ann COLL` and `vec search --exact --ef N`.
- If a graph search returns fewer than k hits while the slab has ≥ k
  vectors, the exact scan answers instead.

## Costs (accepted, documented)
- The graph is rebuilt on every open by replay (not persisted); building
  costs O(n · ef · log n) distance computations, so opening a large
  ANN collection is much slower than without it.
- Inserts pay graph maintenance on the commit/apply path.
- Recall < 1; it is measured, not promised.

## TLA+
No new storage or concurrency protocol: the declaration is a manifest
field committed like `Indexes`, and the graph is in-memory state mutated
under the view's existing lock. No model is added; the safety property
(never return an id that is not live in the lens) is a property test.

## Test plan
- Unit: graph invariants after churn (slot/node maps agree, degree ≤ max,
  no self links, live entry point, deleted nodes hold no links); same
  history builds the identical graph; the digest is unchanged by ANN; a
  shredded vector's bytes are zeroed.
- Randomized churn (8 seeds × 3,000 puts/overwrites/deletes/shreds, M=4):
  every ANN hit is live; with ef = n, recall@10 ≥ 0.9 against exact.
- Recall test: 10k random 32-d vectors, recall@10 ≥ 0.9 at default ef
  (measured 0.979); every hit carries the exact score.
- Sim: unchanged (the lens digest excludes the graph); DB test with
  reopen, compaction, shred.
- Bench: recall@10 vs exact and QPS at 100k and 1M, plus build time and
  open time.

## Outcome

Measured 2026-10-28 (`bench/results/2026-10-28-hnsw/`, 128-d clustered,
M 16, efConstruction 100): at 100k, recall@10 0.930 / 0.988 / 0.998 at ef
32 / 64 / 128 with 6.1k / 3.3k / 2.1k QPS on one client vs 910 exact; at 1M,
0.669 / 0.804 / 0.919 with 2.1k / 1.1k / 676 QPS vs 64 exact. Build and
reopen: 23 s at 100k, ~17 min at 1M (heap 1.1 GiB).

Overflow pruning changed during the work: keeping only the closest
neighbors was 2.7× faster to build but stranded clusters (100k recall
0.72–0.74 regardless of ef), so overflowing lists are now pruned with the
same diversity heuristic, without top-up.

Remaining: the graph is rebuilt on every open (persist it with snapshots);
insert is single-threaded; 1M needs larger ef/efConstruction than the
defaults for recall ≥ 0.95, not measured.
