# 05: Server auth, TLS and the key provider

Status: implemented (see the outcome at the end).

## Problem

`freshdb serve` accepted every request from anyone who could reach the port,
spoke plain HTTP, and the only way to supply the key-encryption key (KEK)
was `WithKEK` from Go. The CLI always ran in dev-key mode: the KEK is
generated and stored in the bucket at `keys/_kek`, so anyone with a copy of
the bucket can decrypt everything that was not shredded.

## Design

**Bearer token.** `server.Options{Token}`; `server.New(db, opts)` wraps the
mux. When a token is set, every request needs `Authorization: Bearer <t>`
(compared in constant time); otherwise 401 with `WWW-Authenticate: Bearer`.
`GET /healthz` stays open so load balancers can probe without a secret.
CLI: `--token` or `FRESHDB_TOKEN`, or `--token-file`. The CLI client sends
`FRESHDB_TOKEN` when talking to `--addr`.

**TLS.** `--tls-cert` / `--tls-key` (or `FRESHDB_TLS_CERT` / `_KEY`) switch
`serve` to `ListenAndServeTLS` with TLS 1.2 as the minimum. No ACME, no mTLS.

**Refuse open binds.** `serve` refuses to listen on a non-loopback address
without a token unless `--insecure-no-auth` is given. Loopback is decided
from the parsed host: `127.0.0.0/8`, `::1`, `localhost`. An empty host
(`:7070`) is all interfaces and therefore not loopback.

**Key provider.** `type KeyProvider interface { KEK(ctx) ([]byte, error) }`
and `WithKeyProvider(p)`. Built in: `KeyFromEnv(name)` and
`KeyFromFile(path)`, both accepting 64 hex chars or base64 of 32 bytes.
`WithKEK` stays. A KMS is a user-supplied provider. The CLI uses
`--kek-file` or `FRESHDB_KEK` (env); with neither it keeps the dev default
and prints a loud warning on every open.

**Wrong-key detection.** Without a check, a wrong KEK on a database with no
DEKs yet would silently start a second key domain. Open now stores
`keys/_kekcheck` (AEAD-sealed constant, `PutIfAbsent`, written once) and
verifies it on every open, returning `ErrWrongKEK`. Databases created before
this change get the check object on their first open with the KEK in use,
after their DEKs unwrap correctly.

## Not done

- No rekey or dev → real KEK migration: a database created in dev mode
  keeps needing the stored `keys/_kek`. Rekey (rewrap every DEK under a
  new KEK, crash-safe) needs its own design and TLA+ model.
- No per-user tokens, scopes, or audit of who called what.
- No certificate reload; restart to rotate.

## TLA+

None. Auth and TLS are request filters with no storage or concurrency
protocol. The key check is one immutable `PutIfAbsent` object, the same
pattern as `keys/_kek` and `keys/_sign`, which the existing models treat as
setup state.

## Tests

- server: 401 without/with a wrong token, 200 with the right one, healthz
  open, token comparison over TLS with a self-signed cert (httptest).
- CLI: loopback detection table; refusal message on a non-loopback bind.
- keys: env/file parsing (hex, base64, wrong length), wrong KEK →
  `ErrWrongKEK`, right KEK reopens and reads encrypted data, legacy DB
  without the check object gets one.

## Outcome

As designed, with one change: the dev-key warning is logged once per
process by `Open` (the CLI no longer prints its own copy). Overhead of the
token check, in process: ≈ +0.06–0.1 µs on a ~2 µs GET
(`BenchmarkAuthGet`). Checked by hand: the refusal on `0.0.0.0`, 401 / 200 /
healthz over HTTPS with a self-signed cert, wrong `FRESHDB_KEK` refused,
right one reads.
