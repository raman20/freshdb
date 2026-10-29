# 14. Observability (item 15; gaps S3.1–S3.4)

## Problem

Before this change an operator of `freshdb serve` had `/healthz` (process
up) and unstructured `log.Printf` lines. There was no way to see commit
latency, conflict rate, compaction pauses, cache hit rate or a writer that
had halted on a storage error short of reading logs, and no profiler.

## Design

**Engine counters (`DB.Stats`)**: lock-free atomics and fixed-bucket
histograms in the `DB` itself (no dependency, no registry):

| counter | where it is counted |
|---|---|
| commits / commit errors / conflicts, commit latency | `writer.submit`: the caller's wait from submit to ack (queueing for the group commit + the segment write). Conflicts are separate from errors; a paused attempt retried after compaction is neither. |
| segments, segment bytes, segment write latency | around `putExactlyOnce` of each group commit (store write + fsync) |
| compactions / GCs and their errors, compaction pause | `runCompaction` (commits are paused for its whole duration) |
| snapshots / snapshot errors | after each KV snapshot write |
| value cache hits / misses | `valueCache.get`, per shard |
| halted | the first error that halted a writer (sticky until reopen) |

Histogram buckets (`LatencyBounds`) run from 100 µs to 10 s, 16 bounds +
`+Inf`; observe is one bucket search and two atomic adds.

**Health**: `DB.Healthy()` is in-memory (closed / fenced / halted).
`DB.Ping(ctx)` adds two store reads: the current manifest must be readable
and manifest id+1 must not exist (if it does, another process took the
lease and this one is fenced; it would otherwise only learn that at its
next commit).

**HTTP** (`server`):

- `GET /metrics`: Prometheus text format 0.0.4, written by hand (≈150
  lines; the client library would be the first non-x dependency). Admin
  scope when auth is on (it reveals branch counts, request routes and
  rates). Route labels are the mux *patterns* (`GET /v1/kv/{key...}`),
  never paths, so cardinality is bounded by the route table.
- `GET /readyz`: `Ping`, cached 1 s so a probe storm cannot load the store;
  unauthenticated like `/healthz`, so it names only the class of problem
  (closed / fenced / halted / storage unreachable), not the error text.
- `--pprof`: `net/http/pprof` under `/debug/pprof/`, admin scope; `serve`
  refuses it without auth and the server ignores it without auth (heap
  profiles expose memory contents, which may include plaintext values).
- Logs: `--log-format json` installs a JSON `slog` handler as default, so
  existing `log.Printf` lines become JSON objects too. `--access-log` logs
  every request; `--slow-op` (default 1 s) logs slower ones at WARN.
  Streams and long-polls (`?wait=`) are exempt from the slow log and the
  latency histogram (their duration is the client's choice). Log lines
  carry the token's *name*, never the token.

The response wrapper keeps `http.Flusher` (SSE) and implements `Unwrap`
(`http.ResponseController`).

## Tests

- `TestMetricsEndpoint`: exposition parses (every sample after its TYPE,
  families declared once), exact counts for commits, routes × codes,
  unauthorized, histogram `+Inf` = `_count`; 401 without token, 403 for a
  read token; no token text in the body.
- `TestMetricsCounters`: a forced serializability conflict counts once,
  compact and GC are counted by kind, head LSN.
- `TestReadyz`: ok → (another process opens the DB) cached ok for 1 s →
  503 fenced → (Close) 503 closed; `/healthz` stays 200.
- `TestPprofNeedsAdmin`, `TestAccessAndSlowLog` (JSON lines, token name not
  token, long-poll exempt), `TestStreamThroughMetricsWrapper`.
- `TestDiskFull` (engine) now also checks `Ping` → `ErrUnknownOutcome`
  wrapping ENOSPC, `Stats` halted/commit errors/compaction errors.
- Mutations: dropping `statusWriter.Flush` fails the stream test; not
  recording the halt fails `TestDiskFull`.

No formal model: this adds counters and read-only endpoints; no protocol.

## Cost

See `bench/results/2026-10-29-observability/before-after.txt`.

## Not done

- No OpenTelemetry tracing.
- Per-branch head LSN is exported for `main` only (branch count can be
  unbounded); `Stats().Heads` has all of them.
- No alerting rules shipped; suggested ones are in docs/operations.md.
