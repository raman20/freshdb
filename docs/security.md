# Security notes

freshDB is pre-1.0. **Do not use it to protect real personal data yet.**
To report a vulnerability, follow [SECURITY.md](../SECURITY.md) (private
advisory, not a public issue).

## Threat model in one paragraph

freshDB trusts the process that opens the database and anyone with access
to the storage (directory or bucket). It protects subject-tagged values from
being read **after their subject is shredded**, by anyone, including someone
with a full copy of the storage taken afterwards. It does not authenticate
clients, encrypt traffic, or hide metadata.

## What is encrypted

- Values written with a subject (`--subject S`, `freshdb.Subject(S)`, or
  `?subject=` in HTTP; `"subject"` in `/v1/tx` ops) are sealed with AES-256-GCM under that subject's data
  key (DEK). The additional data binds each ciphertext to its log, LSN, op,
  space and key, so it can't be moved elsewhere.
- DEKs are stored in `keys/dek/<hash>`, wrapped by the key-encryption key
  (KEK). Subject names are hashed before they reach storage.
- **Not encrypted:** keys, document ids, topic names, collection names,
  index declarations, and any value written without a subject. Use opaque ids in keys.

## Shred

`Shred(s)` writes a marker, deletes the DEK, appends an audit entry and
returns an ed25519-signed certificate. Afterwards no lens, branch or LSN
returns `s`'s data, new writes for `s` fail with `410`, and `verify` still
passes because the hash chain covers ciphertext. Limits:
- Copies of the DEK outside freshDB's control (backups, object versions)
  defeat it. Expire noncurrent versions of `keys/`.
- A DEK create that lands after a shred leaves an orphan key until the next
  open or `verify`; no plaintext is served in that window.
- Shredded ciphertext stays in the log until a compaction (`freshdb compact`,
  `main` only) drops it; until then it is unreadable without the DEK.
  Backups taken earlier still hold it.

## Keys (dev mode vs a supplied KEK)

By default (dev mode) the KEK is generated and stored in `keys/_kek`, and
the certificate signing seed in `keys/_sign`, **in the same storage as the
data**. Anyone who can read the storage can then decrypt every subject that
has not been shredded. Open logs a warning in this mode.

Supply the KEK instead with `--kek-file PATH` or `FRESHDB_KEK` (64 hex
characters or base64 of 32 bytes), or from Go with `WithKEK`,
`WithKeyProvider(KeyFromEnv(..))`, `KeyFromFile(..)`, or your own
`KeyProvider` (for example one that calls your KMS). Open stores a small
check object, `keys/_kekcheck`, sealed under the KEK, so a wrong key fails at
open with `ErrWrongKEK` even before any subject has a DEK. Limits: there is no
rekey (a database created in dev mode keeps needing `keys/_kek`), no
built-in KMS client, and the signing seed `keys/_sign` still lives in the
bucket ([roadmap](../ROADMAP.md)).

## Network

`freshdb serve` listens on `127.0.0.1:7070` by default.

- **Tokens.** `--token` or `FRESHDB_TOKEN` is one admin token;
  `--token-file` holds scoped tokens (read / write / admin, optionally
  limited to kv prefixes, topics, doc or vector collections; see
  [operations.md](operations.md)). Every request except `GET /healthz`
  needs `Authorization: Bearer <token>` (401 unknown, 403 not allowed).
  Scopes apply to every branch: a write token can write any branch it can
  name, and a limited token's resources are names, not branches.
- **Rotation.** The token file and certificate files are re-read on change
  (2 s / 10 s polling) and on SIGHUP; a bad file keeps the old set.
- **TLS.** `--tls-cert` / `--tls-key` (or `FRESHDB_TLS_CERT` / `_KEY`)
  serve HTTPS, TLS 1.2 minimum. No mTLS.
- **Connection caps.** `--max-streams` / `--max-waiters` bound SSE streams
  and long-polls (503 over the cap). There is no per-token rate limit.
- **Open binds.** A non-loopback `--listen` (including `:7070`) without a
  token is refused unless you pass `--insecure-no-auth`. Over a network,
  use TLS too, or the token travels in clear text.

## Integrity

Every segment frame has a CRC-32C and every committed transaction extends a
SHA-256 hash chain; values read from the log are CRC-checked before they
are returned or cached. `freshdb verify` re-checks all of it. Damage below
the tail is reported as `ErrCorrupt`, never silently dropped.
