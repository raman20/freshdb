# 13 — Format versioning, golden files, upgrade/downgrade policy

## Problem (audit S6.1–S6.3)

- Manifests and segments had version stamps; a base or journal file from a
  newer build was reported as corruption ("bad header", "not a journal
  file"), and the manifest refusal had no sentinel error to test for.
- Nothing pinned the bytes: an encoder change (field order, a default such
  as the inline limit recorded in KV snapshots) could change the format
  with every test still green.
- Compatibility with v0.1.0 was tested by building "v0.1.0-like" data
  with the current code, not with v0.1.0 itself. No written policy.

## Design

1. One sentinel, `ErrVersion` (= `format.ErrVersion` = `store.ErrVersion`),
   for every object with a version stamp: manifest, segment, base, journal.
   The check runs before the CRC/structure checks, so a newer object is
   never called corrupt. Caches (snapshots, HNSW) keep being ignored and
   rebuilt. Unversioned objects are gated by the manifest format.
2. `freshdb version` prints the formats; `verify` reports the manifest
   format of the database.
3. Golden databases built by a deterministic workload (fixed clock,
   seeded randomness): one written by the real v0.1.0 code (generator
   run in a v0.1.0 worktree; the workload source is shared and checked
   to be identical), two by main (plain and compacted). Read tests for
   all, byte tests for main.
4. Policy in docs/compatibility.md, with the downgrade matrix measured by
   running the v0.1.0 code on the main goldens.

No TLA+: nothing here is concurrent; the property ("a reader refuses what
it does not understand, and reads what older writers wrote") is checked by
the tests directly.

## Numbers

bench/results/2026-10-29-formats/before-after.txt. Golden data is 112 KB
(62 + 65 + 25 objects, v0.1.0 / main / main-compacted); the golden tests
take 0.2 s.

## Found along the way

- v0.1.0 does not recognise a journal directory and creates an empty
  files-layout database in it. Cannot be fixed in v0.1.0; documented in
  the matrix.
- v0.1.0 rewriting a main manifest drops the `ann` declaration (unknown
  JSON field): HNSW is off after a downgrade and re-upgrade. Data intact.

## Not done

- No migration tool for future format bumps (none exists yet to migrate).
- The v0.1.0 read-by-old-binary column is a manual run, not CI.
- arm64 byte-identity of HNSW snapshots is not checked.
