# Component D: documents and secondary indexes

Review started 2026-10-30 (IST). Code: `DocTx` in `tx.go` (Put, Delete,
Get, Find), `RegisterIndex` in `branch.go`, the index lens
`lens/docindex.go`, the HTTP handlers (`/v1/docs`, `/v1/indexes`) in
`server/server.go`. **Status: review done; verdict at the end.** Nothing here claims
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
| D-D5 | `PUT /v1/docs` with a JSON non-object (`[1,2]`) and `Find` with an unencodable value returned **500** (plain error) | coverage read of `docPut` | new `ErrBadValue` (HTTP 400) wraps both | `TestDocBadValues`, `TestHTTPAPI` |
| D-D6 | No HTTP test exercised `GET /v1/docs/{c}/{id}` (0 % coverage), a bad body, or a repeated index declaration | coverage | tests added (200, 404, 400 ×3, idempotent `POST /v1/indexes`) | `TestHTTPAPI` |

Also checked, no defect: an index declared after a compaction, and
rebuilt on reopen, sees every document (`TestDocIndexAfterCompactAndReopen`);
a read-only snapshot's `Find` does not see later commits (both paths).

## Measurements (2026-10-30, 8-core box, in-memory store)

Taken while a soak and the full suite shared the box: read them as orders
of magnitude, not as a baseline. `docs_bench_test.go`.

| bench | result |
|---|---|
| `Find` 1 % of 10k docs, read-only, no index | 36 ms (decodes every document) |
| same, with index | 20 µs (~1 800×) |
| `Find` in a read-write tx with a local write, no index / index | 37 ms / 67 µs |
| `Put` (1 doc per commit), no index / index | 109 µs / 78 µs; +5 KB, +50 allocs per op with the index (`DocField` decode) |
| `RegisterIndex` over 100k existing docs | 296 ms, **writers stalled for all of it** (`db.mu` + view lock during the replay) |
| index heap per document (100k docs, 100 values, 8-byte ids) | 153 B |

No "before" numbers: D-D1..D-D3 were correctness fixes (the old indexed
`Find` was wrong in read-write transactions); the new cost on the indexed
read-write path is the overlay of the transaction's own writes
(O(local writes)) and one prefix read entry.

## Tests, fuzzing, mutation, coverage

- Unit / property: `docs_find_test.go` (rapid: indexed vs unindexed DB
  over random puts, deletes, subjects and shreds, in read-only and
  read-write transactions), `lens/canon_test.go`, `TestHTTPAPI`;
  `TestPropEngineLensEqualsFoldAcrossRestart` (rapid) checks the index
  lens digest against a from-scratch fold across reopens.
- Deterministic simulation (`sim/`: store faults, crashes, takeovers,
  compaction, GC, shreds): now writes and deletes documents whose field
  `f` uses equal numbers in different literals (`1`, `1.0`, `10e-1`),
  indexes them on odd seeds and scans on even seeds, and after every
  recovery checks `Find` for each value against the model fold. It kills
  the "no remove on overwrite" mutant (seed 3, step 47). Before this
  review the simulator wrote no documents.
- Fuzz: `FuzzCanonNumber` (canonical equality ⇔ exact rational equality,
  4.6 M execs / 60 s), `FuzzDocField` (any stored bytes: no panic, result
  valid JSON and idempotent, 2.0 M execs / 60 s). The HTTP query decoder
  is `encoding/json`.
- Mutation (11 hand mutants on a worktree: drop the prefix read, drop or
  weaken the overlay, skip trailing-zero trim / leading-zero trim /
  fraction shift / sign, skip subject-map add / remove, skip remove on
  overwrite, float64 query decode): **11/11 killed**, after adding the
  sign cases (`-1` vs `1`) to `TestCanonicalJSONNumbers`. A 12th,
  skipping `DropSubject` for a replayed `OpShred` in `Apply`, survives
  everything including the simulator: it is equivalent today, because
  `Shred` calls `DropSubject` on every view directly and a replayed put of
  a shredded subject fails to decrypt (treated as a delete). The branch
  stays as defence in depth.
- Coverage (whole root/lens/server suites): `DocIndex` 100 %,
  `canonNumber` 100 % (unit cases for exponents past int64),
  `CanonicalJSON` 72.7 %, `DocTx.Find` 89 %, `RegisterIndex` 82 %.
  Uncovered, justified: `CanonicalJSON`'s re-decode and re-marshal error
  returns (unreachable: the input was just produced by `json.Marshal`, and
  `canonNumbers` only swaps `json.Number` literals for valid ones);
  `Find`'s `lensView` and scan errors (a closed or failed DB, covered for
  the other tx methods); `RegisterIndex`'s manifest-commit and fold errors
  (store faults: covered by the manifest fault tests, not per caller).

## TLA+ alignment

The index is a lens: a deterministic fold of committed log entries, like
the KV and topic lenses, so `specs/FreshLog.tla`'s claim (a lens state is
a function of the committed prefix) covers it. What the spec does not
model is `Find`'s conflict rule (contract 4): serializability of a
predicate read rests on the collection prefix read, the same mechanism as
`Scan`, which the OCC part of the spec (component F) covers as a range
read. No spec change.

## Gaps (honest)

- `RegisterIndex` stalls every writer for the replay (296 ms per 100k
  docs here; linear). Building off-lock and catching up is not done.
- `Find` conflicts on any write to the collection (coarse; correct).
- Unindexed `Find` decodes every document (36 ms per 10k): no planner,
  no warning.
- JSON strings with invalid UTF-8 are replaced by U+FFFD by
  `encoding/json` before comparison, so two such distinct strings are
  equal to `Find` (both paths, consistently).
- Index state is in RAM (153 B/doc plus ids and values), rebuilt by a
  replay on every open (S5.1).
- Only equality on one top-level field; no compound, range or nested
  paths.

## Verdict

Correct for what it promises after D-D1..D-D5 (indexed and unindexed
`Find` agree, including in read-write transactions and on numbers; no
phantoms; bad input is a 400). Not production ready as a query engine:
the `RegisterIndex` writer stall, the full-collection conflict and the
unplanned full scan are documented limits, not fixed.
