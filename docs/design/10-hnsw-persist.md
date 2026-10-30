# 10 — Persisting HNSW graphs

## Problem

The opt-in HNSW graph (design 04) lived only in RAM and was rebuilt by
inserting every vector on every open: 23 s at 100k × 128-d and about
17 min at 1M (item 4 numbers). Every compaction that commits also reloads
every branch from its base, so it paid the same rebuild.

## Design

The graph stays a cache. It is not in the manifest, not in any digest, and
never needed for correctness; losing it costs a rebuild.

- **Format** (`lens/hnsw_persist.go`). One blob per live view: magic,
  graph count, then per (collection, dimension) the parameters, node count,
  top level and entry, and per node its id, a 64-bit FNV hash of its unit
  vector and its links per level (links to deleted nodes are dropped).
  CRC32 over the whole blob. No vector components are stored.
- **Objects**. `ann/<log>/<lsn>-<epoch>-<part>of<n>`, 64 MiB parts, written
  create-only. `lsn` is the log head when saved, `epoch` the opener's.
- **Save** on `Close`, per writer that is not failed or crashed, after it
  has drained. Older snapshots of the log are deleted after a complete
  save. A failed or partial save deletes its own parts.
- **Load** in `openWritable`: the newest complete snapshot with
  `lsn ≤ head` is decoded (bad magic, CRC, bounds or a link to a node that
  lacks that level → ignored, rebuild). Per graph with the same normalized
  parameters, each saved node is kept only if its id is in the view, it
  has at least one level, and the hash of the view's vector matches (the
  level count is not checked against the id since 2026-10-30: identical
  vectors share a node, and a copy that replaces a deleted representative
  keeps the node's levels). Copies are not saved; each vector without a
  node first joins a kept node with the same hash and identical vector. Other nodes are removed with the normal delete repair,
  then dead links are swept, the entry is re-picked if needed, and every
  vector without a node is inserted (catch-up for commits after the save).
  Graphs whose parameters differ are rebuilt.
- **Compaction** reloads views from their bases; it now hands the old
  views' graphs to the reload in memory (same encode/decode path, same
  validation) instead of rebuilding.
- **Shred** deletes every `ann/` object of every log (live and pending
  garbage) before returning, and marks the DB tainted: no graph built
  before the shred is saved or handed over. The link structure of such a
  graph was chosen with the shredded vectors present; the next open or
  compaction rebuilds from scratch, which clears the mark.
- **Zombies**. An opener that was fenced but never noticed (no commit since
  the takeover) still runs `Close`. It checks for the fence segment at its
  next sequence before and after the put and deletes its snapshot if it
  finds one. A fence before the put is seen by the second check; a fence
  after it happens before the new owner can shred, and that shred's sweep
  deletes the snapshot. The remaining gap: a zombie that crashes between
  its put and its re-check leaves a snapshot (hashes, no vectors) until the
  owner's next save deletes older snapshots of that log.
- **GC** deletes the `ann/` objects of a log it drops.

## Formal model

`spec/FreshANN.tla`: two ids × three versions; owner put / delete / shred /
save / crash / open-with-restore; one takeover whose zombie saves with the
two fence checks. Invariants: `GraphCurrent` (after any restore the graph
holds exactly the current (id, version) vectors) and `ShredClean` (no saved
graph holds a shredded vector, except a zombie's in the step before its own
re-check deletes it). 982,783 states, 222,948 distinct, depth 23, no error.
Mutants: no re-check → `ShredClean` violated in 7 steps; restore without the
vector check → `GraphCurrent` violated in 6 steps.

## Tests

- `lens`: restore onto the same vectors is byte-identical; onto a later
  state (overwrites, deletes, shreds, a new dimension) it keeps the
  invariants (`CheckANN`) and recall ≥ 0.95 at ef 200; flipped or truncated
  blobs are refused; other parameters rebuild.
- `freshdb`: close/reopen gives identical answers with every node reused; a
  crash after 2k more writes restores 5000, removes 1000 stale, inserts
  2000; compaction hand-over; shred deletes and stops saves; a dropped
  branch's snapshot goes with GC; a corrupt snapshot rebuilds; a fenced
  zombie's Close leaves nothing.
- Simulation: transactions now also put and delete vectors (40 ids, ANN on,
  M 2–5), a third of crashes are clean closes (which save through the
  faulty store), and every check compares the vector lens to the model and
  runs `CheckANN`. 2000 seeds per suite run.

## Results

`freshdb-bench vector`, 128-d clustered vectors, M 16, c=1, same box as
item 4. Results: `bench/results/2026-10-29-hnsw-persist/`.

| N | build | close (save) | reopen before → after | reopen after a crash, N newer vectors |
|---|---|---|---|---|
| 100k | 18.2 s | 0.1 s | 23 s → 0.6 s (heap 99 MiB) | 0.9 s (1,000 newer) |
| 1M, efC 100 | 976 s | 2.2 s | ~17 min (1,037 s) → 10.1 s (heap 1,047 MiB) | 22.4 s (10,000 newer) |
| 1M, efC 200 | 1,479 s | 2.0 s | — → 8.9 s | 25.0 s (10,000 newer) |

Recall@10 / QPS c=1 at 1M (100 queries):

| efC | ef 64 | ef 128 | ef 256 |
|---|---|---|---|
| 100 | 0.804 / 1,252 | 0.919 / 725 | 0.962 / 481 |
| 200 | 0.810 / 1,566 | 0.925 / 820 | 0.982 / 475 |

At 100k (efC 100): ef 32 0.929 / 8.4k, ef 64 0.989 / 5.7k, ef 128 0.997
/ 4.4k, ef 256 0.999 / 2.9k QPS (exact: 1.0 / 1.4k).

Recall after reopen equals recall after build (same graph). efC 200 costs
+52% build time for +0.006 recall at ef 64/128, so the efC default stays
100. The default EfSearch is now **128** (was 64): at 1M recall 0.804 →
0.919 for 1,252 → 725 QPS; at 100k 0.989 → 0.997 for 5.7k → 4.4k QPS.
EfSearch does not shape the graph, so a saved graph is kept when only
EfSearch changes.

Parallel insert was not done: `dot` is already ~39% of the build profile,
and insert order must stay deterministic so that the same history builds
the same graph (the simulator and byte-identity tests rely on it).

## Limits

- Saved only on clean `Close`: after a crash the last saved graph is
  caught up by inserting everything newer, which is as slow per vector as
  before.
- Saving holds the view read lock (commits wait) for the encode: at Close
  that does not matter; there is no periodic save yet.
- One blob in RAM while saving or loading (≈ ids + 8 B + links per node).
