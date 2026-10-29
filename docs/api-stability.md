# API stability, versioning and deprecation

freshDB is **pre-1.0**. This is the policy the project follows now and
intends to keep at 1.0; it is not yet a 1.0 promise.

## Versioning

[Semantic Versioning](https://semver.org/). Before 1.0 a minor version
(`v0.N`) may break any surface below; a patch version (`v0.N.x`) never
does. From 1.0, breaking a **stable** surface needs a new major version.
Every break is listed under "Changed (breaking)" in
[CHANGELOG.md](../CHANGELOG.md).

## Surfaces and their status

| surface | status | "breaking" means |
|---|---|---|
| on-storage format (manifests, segments, bases, journal) | stable track: versioned, golden-tested ([compatibility.md](compatibility.md)) | a binary can no longer open data an earlier release wrote |
| Go package `freshdb` (Open, Tx, KV/topic/collection API, errors, options) | stable track | removing an exported identifier or changing its signature or documented behaviour |
| Go package `store` (Store interface, Open, S3Check) | stable track | same |
| HTTP API under `/v1/` | stable track | removing a route, field or status code; new fields and routes are not breaking |
| CLI command names and flags | stable track | removing or renaming a command or flag; human-readable output is **not** stable |
| `/metrics` names | stable track | renaming or removing a metric |
| Go packages `server`, `format`, `lens` | experimental | may change in any minor release |
| `sim`, `internal/...`, `examples`, `bench` | internal | no guarantee |
| vector search (HNSW), lenses, branches | experimental features | may change in any minor release |
| S3 backend | experimental ([s3.md](s3.md)) | tested on MinIO only |

"Stable track" means changes are reviewed as API changes today, and these
surfaces become stable at 1.0.

## How API changes are caught

`scripts/api-surface.sh` writes the exported Go API of `freshdb`, `store`,
`server`, `format` and `lens` to `api/*.txt`. CI regenerates it and fails
when it differs from the committed copy, so no exported change lands
without a visible diff (and a CHANGELOG line if it breaks). The
on-storage format is guarded by golden databases (`testdata/golden`).
The HTTP API and CLI have no automated diff yet.

## Deprecation

1. Mark it: `// Deprecated:` (Go), a `Deprecation` response header
   (HTTP), a warning on stderr (CLI flag).
2. Announce it under "Deprecated" in CHANGELOG, naming the replacement.
3. Keep it working for at least one minor release (pre-1.0) or until the
   next major (from 1.0).
4. Remove it in a release whose CHANGELOG lists it under "Removed".

Data formats are not removed: a newer binary reads every format version a
release wrote, or refuses with `ErrVersion` and names a migration.
