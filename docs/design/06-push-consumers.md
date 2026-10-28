# 06: Push for consumers (long-poll and SSE)

Status: implemented (see the outcome at the end).

## Problem

Consumers poll: `sub --follow` and HTTP clients call `GET /v1/topics/{t}` or
`POST .../groups/{g}/next` in a loop with a sleep. Latency is the sleep, and
an idle consumer still costs a request per interval.

## Design

**Change notification in the DB.** `db.chg` is a channel that is closed
and replaced (under a small mutex) whenever any branch head may have moved:
after the writer applies a batch to the live view, and after every manifest
commit (branch create, rewind, promote, compaction, ANN, close).
`(*DB).WaitHead(ctx, branch, seen) (head, error)` returns as soon as the
branch's head LSN differs from `seen` (so a rewind wakes it too), or when
ctx ends (`ctx.Err()`) or the DB closes (`ErrClosed`). It takes the current
channel *before* reading the head, so a change between the read and the
wait cannot be lost: either the read sees it, or the channel taken earlier
is closed.

**Long-poll.** `GET /v1/topics/{t}?from=&max=&wait=30s` and
`POST /v1/topics/{t}/groups/{g}/next?max=&wait=30s` return immediately if
there are messages; otherwise they wait for a head change and retry, until
messages arrive or `wait` (capped at 60 s) passes, then return `[]`. The head
is read before each attempt, so a commit during the attempt causes one
extra retry, never a missed wake-up. `next` keeps its semantics (fetch and
ack in one transaction, at-most-once to the HTTP client).

**SSE.** `GET /v1/topics/{t}/stream?from=N` (or the `Last-Event-ID` header)
returns `text/event-stream`: one `event: message`, `id: <offset>`,
`data: <JSON message>` per message, `: ping` comments every 15 s, until the
client disconnects. Reads come from the live head of the branch. A
compaction does not break a stream (topic messages are kept); a rewind
below the stream's offset just means no new events until the topic grows
past it again.

**CLI.** `sub --follow` uses long-poll (`wait=30s`) instead of a sleep loop.

**Go API.** Embedded users call `WaitHead` around their own `View`/`Update`.

## Not done

- No server-side consumer-group push over SSE (groups use long-poll `next`).
- No fan-out limit: each waiting request holds a goroutine and a connection.
  `http.Server` has no write timeout, so streams can live forever.
- No webhooks.

## TLA+

`spec/FreshNotify.tla` models one writer (advance or rewind the head, then
close-and-replace the channel) and N waiters running the
take-channel / read-head / compare / block loop. Invariant `NoLostWakeup`:
a waiter blocked on generation g with a stale head view always has a newer
generation pending (`gen > g`), so it will wake. Liveness is not checked;
the invariant is the property the code relies on. A mutation (read the head
before taking the channel) must violate it.

## Tests

- `WaitHead`: wakes on commit, on rewind, on ctx timeout, on Close; no
  wake without a change.
- HTTP long-poll: returns at once with data; waits and returns the message
  published 100 ms later; returns `[]` after `wait`; `next` acks.
- SSE: receives existing and new messages in order with ids; resumes from
  `Last-Event-ID`.
- Benchmark: delivery latency publish → consumer receipt, long-poll vs the
  previous 100 ms-sleep polling.

## Outcome (commit 282c3a6)

- TLC: `FreshNotify.cfg` (2 waiters, MaxHead 2, MaxSteps 5) 87,149 states,
  29,427 distinct, no error; `FreshNotifyBug.cfg` (head read before taking
  the channel) violates `NoLostWakeup`. The same mutation also makes
  `TestWaitHeadNoLostWakeup` fail within 200 runs.
- `freshdb-bench msg -idle`, 1k msgs/s, default URL, fsync on: e2e p99
  495 ms (500 ms sleep, old `--follow`), 99 ms (100 ms sleep),
  3.6–15.7 ms (200 µs spin), 1.0–14.5 ms (`WaitHead`); p50 0.24 ms (spin)
  → 0.13 ms (`WaitHead`). The p99 floor is the disk stalls described in
  `docs/benchmarks.md`. Max publish rate is unchanged (105–115k msgs/s in
  every mode). Results: `bench/results/2026-10-28-push/`.
- Not done: no limit on concurrent waiters or SSE streams per server; HTTP
  delivery latency and many-waiter fan-out are not benchmarked; SSE has no
  consumer-group mode (use long-poll `next`).
