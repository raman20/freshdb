# 11 — Scoped tokens, rotation, certificate reload, connection limits

## Problem

`serve` had one shared bearer token (all or nothing), no way to rotate it
or a TLS certificate without a restart, and no cap on SSE streams or
long-poll waiters: each holds a goroutine and a connection, so one client
could open thousands.

## Design

**Token file.** `--token-file F`, one token per line, `#` comments:

    SCOPE TOKEN [name=LABEL] [kv:PREFIX* | topic:NAME | docs:COLL | vectors:COLL ...]
    TOKEN                       # a bare token is admin (the old --token-file format)

- Scopes are ordered read < write < admin.
  - read: kv get/scan, topic read/next/stream, doc get/find, vector
    search, list branches, log, verify.
  - write: adds kv put/del, `/v1/tx`, publish, doc put, vector put/del,
    branch create.
  - admin: adds rewind, promote, branch delete, shred, indexes, ANN
    enable, compact, gc.
- Resources (optional) limit a token to named collections; a trailing `*`
  makes a prefix (`kv:users/*`). A limited token is refused every
  database-wide route (branches, log, verify, admin routes), and a scan
  needs a `prefix` inside its grant. `/v1/tx` checks every op (put/del → kv
  key, pub/ack → topic, doc → docs collection, vec → vectors collection)
  before anything is written; one refused op refuses the whole tx.
- Tokens are at least 8 characters, unique, and kept as SHA-256 hashes in
  a map (lookup by hash, so no per-token compare). `--token` /
  `FRESHDB_TOKEN` still works and is an admin token next to the file.
- Unknown or missing token: 401. Known token, not allowed: 403.

**Rotation without restart.** The set is an immutable `*Tokens` behind an
atomic pointer. The server re-reads the file when its
stamp (mtime, size) changes, checked every 2 s, and immediately on SIGHUP. A file that
does not parse is logged and ignored; the previous set stays. Every
request looks up its token's current grant, and an SSE stream re-checks
its grant on each loop turn, so a removed token stops working for new
requests at once and an open stream ends with an `error` event at its next
message or keep-alive ping (≤ 15 s). Rotation is: add the new token, move
clients, remove the old one.

**Certificate reload.** `tls.Config.GetCertificate` serves the current
pair from an atomic pointer; the cert and key files are polled every 10 s
and reloaded on SIGHUP. A pair that does not load (e.g. a renewal caught
between writing the key and the cert) is logged and the old one is kept.

**Limits.** `--max-streams` (default 1024) caps open SSE streams and
`--max-waiters` (default 4096) caps long-polls (`wait>0` reads and
group `next`); over a cap the request gets 503 with `Retry-After: 1`.
Reads without `wait` are never capped. A negative value removes a cap.

## Formal model

None: this is per-request authorization in one process with no protocol
between nodes and no persistent state. The properties (a revoked token is
refused; a bad file keeps the old set; a limited token cannot reach other
collections) are checked by tests instead.

## Tests

`server/scopes_test.go`: a route × token table (401/403/200, per-op tx
checks, nothing written by a refused tx); token-file parse errors;
rotation (overlap, revoke → 401, bad file keeps tokens, an open stream of
a revoked token ends without delivering more messages); stream and waiter
caps (503 + Retry-After, slot freed on disconnect, plain reads uncapped);
certificate reload with generated certs (new serial served, a broken pair
refused).

## Results

See the end of this note (filled from `BenchmarkAuthGet`).

## Limits

- No mTLS, no OIDC/JWT, no per-branch scopes; resources are names only.
- Polling (2 s / 10 s) means a rotation via plain file edit takes up to
  that long; SIGHUP is immediate.
- A revoked token's open stream lasts until its next event or ping.
- The CLI client's `--token-file` still reads a single token.
