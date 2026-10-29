# 16. Operational safety (item 17; gaps S2.3, S2.4, S2.5, S2.7)

## Problems (found in the audit, confirmed by tests before the fix)

- **Shutdown (S2.3).** `serve` called `ListenAndServe`; on SIGTERM a
  goroutine called `Shutdown` with a 5 s timeout. `ListenAndServe` returns
  as soon as `Shutdown` *starts*, so `main` ran `db.Close()` while
  requests were still in flight, and open SSE streams held `Shutdown` for
  its whole timeout.
- **Bind after open.** `serve` opened the database (taking the write
  lease, fencing any running writer) and only then bound the port. A
  second `serve` started by mistake on a busy port fenced the healthy
  server and then died on `address already in use` (the soak harness hit
  this; `TestServeLifecycle` reproduces it against the old binary).
- **Slow clients (S2.4).** Only `ReadHeaderTimeout`: a client that sends
  headers and then trickles (or stalls) its body held a handler goroutine
  forever; same for a client that never reads its response.
- **Sizes (S2.5).** Keys and topic/collection/subject names were
  unbounded; every key lives in the in-memory index.
- **Who holds it (S2.7).** Opening fences the previous writer by design,
  but neither side said who the other was: the fenced writer got a bare
  `ErrFenced`.

## Design

- **Listen, then open.** `net.Listen` first; a bind failure exits before
  anything is opened ("nothing opened; any running writer is untouched").
- **Drain.** `server.Options.Drain` (a channel). SIGTERM/SIGINT closes it:
  `/readyz` answers 503 `draining` (load balancers stop routing), SSE
  streams end with `event: shutdown` (clients resume with
  `Last-Event-ID`), long polls return what they have. Then
  `http.Server.Shutdown` (stop accepting, wait for in-flight requests) up
  to `--shutdown-timeout` (default 30 s), then `Close`; `main` waits for
  that before `db.Close()`, and logs `closed cleanly`. A second signal
  exits at once; nothing acknowledged is lost either way (the log is
  crash-safe), the drain only saves clients from errors.
- **Per-request deadlines.** Server-wide `ReadTimeout`/`WriteTimeout`
  would kill SSE streams, long polls and long admin calls (a compaction
  can take minutes), so deadlines are per request through
  `http.ResponseController`:
  - read: armed only when there is a body (`--read-timeout`, default
    5 m); cleared at EOF so it bounds the upload, not the handler. On a
    timeout it stays armed, so the server's discard of the rest also
    fails fast and the connection is closed (nothing is written).
  - write: `--write-timeout` (default 2 m) for read and write routes;
    streams re-arm it before every event/ping, long polls get it on top
    of `?wait=`; admin routes (compact, gc, backup, verify, ...) are
    exempt. `IdleTimeout` 2 m for keep-alive connections.
- **Limits.** `MaxKeyBytes` = 8 KiB for keys and topic, collection and
  subject names, checked when the write is staged (`ErrTooLarge`, 413).
  Existing larger keys stay readable. Request JSON nesting is bounded by
  `encoding/json` (depth 10 000, a 400). Already in place: body cap
  (`--max-body`), transaction bytes/ops (`MaxTxBytes`, `MaxTxOps`),
  stream/long-poll caps.
- **Lease records.** With `WithHolder(desc)` (the CLI always sets
  `freshdb VERSION, COMMAND, pid P on HOST`), a writable open writes
  `lease/<epoch>.json` {epoch, holder, since}. The opener logs "took the
  write lease (epoch N) from epoch M (holder, since T); that writer, if
  still running, is now fenced"; a fenced writer's error becomes
  `ErrFenced (our epoch M; now held by epoch N (holder, since T))`
  (`errors.Is` unchanged). Records are informational, never read for
  correctness (no TLA+ change: they are outside the manifest/segment
  protocol), best effort, and GC keeps the newest 16. Library users
  without `WithHolder` write none (keeps the simulator and golden bytes
  deterministic).
- **Busy.** A compaction or GC started while one runs was HTTP 500; it is
  `ErrBusy` / 409 (found by the soak harness, item 18).

## Tests

- `cmd/freshdb TestServeLifecycle` (real binary): serve on a busy port
  exits and the running server still accepts writes (fails on the old
  binary: it fenced it); a second server logs whom it fenced and the first
  answers 503 naming the new holder; SIGTERM with an SSE stream and a 60 s
  long poll open: stream gets `event: shutdown`, poll returns 200, process
  exits 0 with `closed cleanly` in < 5 s, the last acknowledged write is
  there.
- `server TestTimeouts`: a stalled upload is cut off after the read
  timeout (300 ms in the test; 3 s hang before the fix of keeping the
  deadline on timeout) and not written; a 1 s long poll and a stream
  outlive a 300 ms write timeout. `TestDrain`: readyz 503 `draining`,
  stream and long poll end. `TestFailCodes`: ErrBusy 409, deadline 408.
- `TestLeaseMessages`, `TestMaxKeyBytes`.

## Cost

`BenchmarkLoopbackPut` (PUT over real loopback HTTP, same binary,
deadlines on vs off, -count 6, while the soak was running): medians
66.4 µs vs 69.4 µs, i.e. within noise. See
bench/results/2026-10-29-opsafety/.

## Not done

No per-token rate limits or global in-flight cap (cap at a proxy); no
request-level context deadline for DB work (a request blocks while
compaction pauses commits, S2.2); lease records are not written on S3 by
library users unless they opt in.
