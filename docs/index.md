# freshDB documentation

freshDB is pre-1.0. These docs describe unreleased `main` unless they say
otherwise; the latest release is
[v0.1.0](https://github.com/raman20/freshdb/releases/tag/v0.1.0).

## Start here

| Doc | For |
|---|---|
| [README](../README.md) | what freshDB is, quickstart, API tour, guarantees, benchmark summary |
| [getting-started.md](getting-started.md) | every lens from the CLI, server mode, Go, the agent demo |
| [faq.md](faq.md) | production readiness, platforms, upgrading from v0.1.0, S3/R2, shred |

## Running it

| Doc | For |
|---|---|
| [operations.md](operations.md) | storage URLs and layouts, `freshdb migrate`, memory and cache sizing, server, backups, compaction |
| [security.md](security.md) | what is encrypted, shred limits, dev-mode keys, network exposure |
| [../SECURITY.md](../SECURITY.md) | how to report a vulnerability |

## How it works

| Doc | For |
|---|---|
| [architecture.md](architecture.md) | stores, manifests, fencing, writer and group commit, lenses, branches, shred |
| [formats.md](formats.md) | byte layouts: bucket keys, journal file, segments, payloads, manifests, snapshots |
| [compatibility.md](compatibility.md) | format version stamps, upgrade/downgrade policy and tested matrix, golden files |
| [lens-authoring.md](lens-authoring.md) | the lens contract and how to add or change one |
| [design-principles.md](design-principles.md) | why one log, and what we refuse to build |
| [design/01-journal-default.md](design/01-journal-default.md) | design: journal as the default local layout, migration |
| [design/02-read-path.md](design/02-read-path.md) | design: point reads, the off-heap value cache |
| [design/03-compaction.md](design/03-compaction.md) | design: compaction base images, garbage with a grace period |
| [design/04-hnsw.md](design/04-hnsw.md) | design: the opt-in HNSW vector index |
| [design/05-auth-tls-keys.md](design/05-auth-tls-keys.md) | design: bearer-token auth, TLS, key providers |
| [design/06-push-consumers.md](design/06-push-consumers.md) | design: `WaitHead`, long-poll and SSE for topic consumers |
| [design/07-memory.md](design/07-memory.md) | design: where RSS goes, packed versions, `GOMEMLIMIT` curve, fair Pebble comparison |
| [design/08-gc-retention.md](design/08-gc-retention.md) | design: GC of old logs, manifests and fences with a retention window; journal shrink |
| [design/09-journal-prealloc.md](design/09-journal-prealloc.md) | design: growing the journal file off the commit path |
| [design/10-hnsw-persist.md](design/10-hnsw-persist.md) | design: persisting HNSW graphs, validated restore and catch-up |
| [design/11-auth-scopes.md](design/11-auth-scopes.md) | design: scoped tokens, rotation, certificate reload, stream/long-poll caps |
| [design/12-durability.md](design/12-durability.md) | design: size limits, journal corruption vs torn tail, repair opt-in, power-loss crash-point test |
| [design/13-format-versioning.md](design/13-format-versioning.md) | design: version stamps, `ErrVersion`, golden databases, compatibility policy |
| [design/14-observability.md](design/14-observability.md) | design: `/metrics`, `/readyz`, structured/access/slow logs, pprof, `DB.Stats` |
| [design/15-backup.md](design/15-backup.md) | design: online backup (delete hold), incremental copy, digest record, restore, PITR |

## Evidence and numbers

| Doc | For |
|---|---|
| [benchmarks.md](benchmarks.md) | every benchmark table, method, environment and caveat; "Changes since v0.1.0" |
| [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) | production-readiness audit: every known gap by severity, with status |
| [STATUS.md](STATUS.md) | post-v0.1.0 work items with commit and measured result |
| [../spec/RESULTS.md](../spec/RESULTS.md) | TLA+ specs and TLC results |
| [../bench/](../bench/) | the `freshdb-bench` suite and raw results |

## Project

[ROADMAP](../ROADMAP.md) · [CHANGELOG](../CHANGELOG.md) ·
[CONTRIBUTING](../CONTRIBUTING.md) · [GOVERNANCE](../GOVERNANCE.md) ·
[website](https://raman20.github.io/freshdb/)
