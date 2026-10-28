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
| [lens-authoring.md](lens-authoring.md) | the lens contract and how to add or change one |
| [design-principles.md](design-principles.md) | why one log, and what we refuse to build |
| [design/01-journal-default.md](design/01-journal-default.md) | design: journal as the default local layout, migration |
| [design/02-read-path.md](design/02-read-path.md) | design: point reads, the off-heap value cache |
| [design/03-compaction.md](design/03-compaction.md) | design: compaction base images, garbage with a grace period |
| [design/04-hnsw.md](design/04-hnsw.md) | design: the opt-in HNSW vector index |
| [design/05-auth-tls-keys.md](design/05-auth-tls-keys.md) | design: bearer-token auth, TLS, key providers |
| [design/06-push-consumers.md](design/06-push-consumers.md) | design: `WaitHead`, long-poll and SSE for topic consumers |

## Evidence and numbers

| Doc | For |
|---|---|
| [benchmarks.md](benchmarks.md) | every benchmark table, method, environment and caveat; "Changes since v0.1.0" |
| [STATUS.md](STATUS.md) | post-v0.1.0 work items with commit and measured result |
| [../spec/RESULTS.md](../spec/RESULTS.md) | TLA+ specs and TLC results |
| [../bench/](../bench/) | the `freshdb-bench` suite and raw results |

## Project

[ROADMAP](../ROADMAP.md) · [CHANGELOG](../CHANGELOG.md) ·
[CONTRIBUTING](../CONTRIBUTING.md) · [GOVERNANCE](../GOVERNANCE.md) ·
[website](https://raman20.github.io/freshdb/)
