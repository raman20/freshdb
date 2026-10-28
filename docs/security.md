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

## Keys (dev mode vs `WithKEK`)

By default (dev mode) the KEK is generated and stored in `keys/_kek`, and
the certificate signing seed in `keys/_sign`, **in the same storage as the
data**. Anyone who can read the storage can then decrypt every subject that
has not been shredded. The Go API accepts `freshdb.WithKEK(key)` so the KEK
can come from your own secret store. The CLI and server have no `--kek`
flag yet, and there is no KMS integration (both on the
[roadmap](../ROADMAP.md)).

## Network

`freshdb serve` has **no authentication and no TLS**. It listens on
`127.0.0.1:7070` by default. Anyone who can reach the port can read, write,
rewind and shred. Keep it on loopback or put an authenticating TLS proxy in
front.

## Integrity

Every segment frame has a CRC-32C and every committed transaction extends a
SHA-256 hash chain; values read from the log are CRC-checked before they
are returned or cached. `freshdb verify` re-checks all of it. Damage below
the tail is reported as `ErrCorrupt`, never silently dropped.
