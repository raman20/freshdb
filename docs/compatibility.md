# Compatibility: format versions, upgrades and downgrades

freshdb is pre-1.0. This page says what is checked today, not what is
promised for 1.0. See [design 13](design/13-format-versioning.md) for the
reasoning and [formats.md](formats.md) for the byte layouts.

## Version stamps

`freshdb version` prints the formats the binary reads and writes:

```
formats: manifest 1-2, segment 1, base 1, journal 1, kv snapshot KV2, ann 1
```

| object | stamp | unknown version |
|---|---|---|
| manifest (`.mf`) | JSON `"format"`: 1, or 2 once compacted | open fails, `ErrVersion` |
| segment (`.seg`) | magic `FRDBSEG1` + u16 version | open fails, `ErrVersion` |
| base (`.base`) | magic `FRDBBAS1` (last byte = version) | open fails, `ErrVersion` |
| journal file (`journal.fj`) | magic `FRESHJ1\0` (byte 6 = version) | open fails, `ErrVersion` |
| KV/topic snapshot (`.snap`) | `KV2` tag inside | ignored, rebuilt from the log (cache) |
| HNSW snapshot (`ann/`) | magic `FHNSW1` | ignored, rebuilt (cache) |
| value chunk (`.val`) | magic `FRDBVAL1`, not checked | gated by the manifest format (each value has its own CRC) |
| keys (`keys/…`), shred certificates, token file | none | gated by the manifest format |

`ErrVersion` (`errors.Is(err, freshdb.ErrVersion)`) means "written by a
newer freshdb": the message names the object and says to upgrade the
binary. A refused open changes nothing on storage (tested for the
manifest and the journal; a segment or base is only read).

The manifest format is the database-level version: a change to any
unversioned object must bump it, so an older binary refuses the whole
database rather than misreading one object.

## Policy (pre-1.0)

- **Upgrades**: a new binary reads every format any earlier release
  wrote. This is tested by golden databases (below) for every released
  version, read in both local layouts.
- **No silent rewrites**: opening a database never rewrites existing
  objects in a newer format. A newer format is written only by an
  operation that needs it (today: `compact` writes manifest format 2).
- **Downgrades**: an older binary refuses formats it does not know (see
  the matrix). Downgrading is not supported once a newer format has been
  written; keep a backup taken before upgrading if you might go back.
- **Format changes** come with a version bump, a CHANGELOG entry under
  "Changed (breaking)" if older binaries can no longer read the result,
  and a new golden directory. Minor versions before 1.0 may change formats;
  1.0 will freeze this policy (docs/PRODUCTION_READINESS.md S6.4).

## Tested matrix

| written by → read by | v0.1.0 | main |
|---|---|---|
| v0.1.0 (files layout) | yes | yes (golden `v0.1.0`, files and after `migrate` to journal) |
| main, uncompacted, files layout | yes (reads the same state; drops the `ann` setting from the manifest it writes, so HNSW is off after going back) | yes (golden `main`) |
| main, compacted (manifest 2) | refused: "manifest format 2 not supported by this binary" | yes (golden `main-compacted`) |
| main, journal layout | **not recognised**: v0.1.0 treats the directory as empty and creates a new files-layout database next to `journal.fj` (main keeps using the journal and ignores those files) | yes |

The v0.1.0 column was run once by hand with the v0.1.0 code
(`testdata/golden/gen`, see its README), not in CI.

## Golden files

`testdata/golden/<version>/db` holds a database written by that version's
code with one fixed workload (KV with inline and out-of-line values,
deletes, topics, a consumer group, documents with an index, vectors, a
shredded subject, a branch), and `dump.txt` what that version read back.

- `TestGoldenRead` opens a copy of each one (files layout, and journal
  after `store.Migrate`) and must read exactly `dump.txt`.
- `TestGoldenBytes` rebuilds `main` and `main-compacted` with fixed clock
  and randomness and requires every object to be byte-identical, so an
  encoder change cannot alter the on-storage format without a failing test.
  (HNSW snapshots are compared on amd64 only: fused multiply-adds on arm64
  can change float bits; they are a cache.)
- An intended format change: bump the stamp, copy the current `main*`
  directories to `testdata/golden/<last release or commit>` so they stay
  under `TestGoldenRead`, then `FRESHDB_UPDATE_GOLDEN=1 go test -run
  TestGoldenBytes .`.
