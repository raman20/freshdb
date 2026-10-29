# Component D: documents and secondary indexes (in progress)

Review started 2026-10-30 (IST). Code: `DocTx` in `tx.go` (Put, Delete,
Get, Find), `RegisterIndex` in `branch.go`, the index lens
`lens/docindex.go`, the HTTP handlers (`/v1/docs`, `/v1/indexes`) in
`server/server.go`. **Status: first fixes landed; the full bar (benches,
mutation run, fuzzers for every decoder, TLA+ note, coverage
justification, verdict) is still to do.** Nothing here claims
documents are production ready.

## Contract (as now implemented)

1. A document is a JSON object stored under (collection, id) in space
   `doc:<coll>`; `Put` refuses anything else.
2. `Find(field, value)` returns the sorted ids whose top-level `field`
   equals `value`, at the transaction's snapshot **including its own
   writes**, and gives the same answer with or without an index.
3. Equality is JSON equality: objects compare with sorted keys, numbers
   by exact decimal value (`1`, `1.0`, `10e-1` equal; integers past 2^53
   are not rounded together). A literal whose exponent does not fit an
   int64 only equals itself.
4. In a read-write transaction `Find` reads the whole collection for
   conflict checking, indexed or not: a concurrent write to the
   collection makes the commit retry (no phantoms; coarse).
5. An index can be declared at any time; it is built by replaying the
   log (base included) and kept by every view; shredded documents leave it.

## Defects found and fixed

| id | defect | evidence | fix | test |
|---|---|---|---|---|
| D-D1 | Indexed `Find` in a read-write tx ignored the tx's own writes; unindexed `Find` saw them, so the answer depended on whether an index existed | probe: put b then Find → `[a]` indexed, `[a b]` unindexed | overlay the tx's local writes on the index result | `TestDocFindIndexedMatchesScan`, `TestDocFindProperty` (rapid, indexed vs unindexed DB over puts, deletes, subjects, shreds) |
| D-D2 | Indexed `Find` was not in the read set: a concurrent insert matching the predicate did not conflict (write skew; `Update` promises serializable) | probe: 1 attempt indexed vs 2 unindexed | register the collection as a prefix read, as the scan path does | `TestDocFindIndexedMatchesScan` (phantom case) |
| D-D3 | Numbers compared by their literal: a stored `1.0` was not found by `1`; the HTTP handler parsed the query as float64, so `12345678901234567891` matched `…890` | probe + `TestDocFindNumbers` | `CanonicalJSON` rewrites numbers to an exact canonical decimal (linear, no big-number parsing, no exponent DoS); the handler decodes with `UseNumber` | `TestCanonicalJSONNumbers`, `FuzzCanonNumber` (canonical equality ⇔ exact rational equality; 4.6 M execs, 60 s), `TestDocFindNumbers`, server test |
| D-D4 | `DocIndex.DropSubject` walked every document of the collection for each shred | code read (same shape as topics C-D4) | subject → ids map | rapid property (shreds, indexed vs scan) |

Also checked, no defect: an index declared after a compaction, and
rebuilt on reopen, sees every document (`TestDocIndexAfterCompactAndReopen`);
a read-only snapshot's `Find` does not see later commits (both paths).

## Open (next steps of this review)

- `RegisterIndex` holds `db.mu` and the view lock while it replays the
  whole log: writers stall for the replay. Measure; consider building
  off-lock and catching up.
- Memory per indexed document (two maps plus the subject map), and the
  `Find` cost without an index (decodes every document): benchmarks.
- Conflict granularity of `Find` is the whole collection (contract 4).
- Mutation run on `lens/docindex.go` and the `Find` overlay; coverage of
  `CanonicalJSON` error paths (unreachable after a successful Marshal).
