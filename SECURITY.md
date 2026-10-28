# Security policy

freshDB is **pre-1.0 prototype software. Do not use it to protect real personal
data yet.** That said, we take security reports seriously, especially anything
that breaks the shred guarantee or writer fencing.

## Reporting a vulnerability

Please **do not open a public issue.** Report privately through GitHub:
**[Report a vulnerability](https://github.com/raman20/freshdb/security/advisories/new)**
(Security → Advisories → "Report a vulnerability").

Include the affected version or commit, the storage backend, and steps or a
simulator seed that reproduce the problem. Expect an acknowledgement within
**7 days** (best effort; this is a small project). We will agree a disclosure
timeline with you and credit you unless you prefer otherwise.

## Supported versions

Only the latest `main` and the latest `v0.x` release get fixes.

## What counts as a vulnerability

- Plaintext of a shredded subject is recoverable through freshDB, or from the
  bucket without the destroyed DEK.
- A fenced (older-epoch) writer can commit after a newer writer opened.
- Acked data is lost, or the log can be made unrecoverable (bricked) by
  storage faults the design claims to tolerate.
- Hash-chain or certificate verification can be bypassed.
- Remote crashes or unbounded memory use via the HTTP API or segment decoder.

## Known security limitations (not vulnerabilities)

These are documented gaps. Reports are still welcome if you find a worse
consequence than the one described here.

- **HTTP authorization is coarse.** Tokens have a scope (read, write or
  admin) and optionally a list of collections, but no per-branch rights,
  no mTLS and no rate limits. The server binds to `127.0.0.1` by default
  and refuses other addresses without a token unless `--insecure-no-auth`
  is given. An admin token can shred, compact and rewind everything. Use TLS (`--tls-cert`,
  `--tls-key`) whenever the token crosses a network.
- **In dev mode, the key-encryption key (`keys/_kek`) and the certificate
  signing key (`keys/_sign`) are stored in the bucket.** Anyone with read
  access to the bucket can decrypt the values of subjects that have not been
  shredded. Shredded subjects stay unreadable because their DEK is deleted.
  However, storage-level versioning or backups taken before the shred may still
  hold that DEK. Supply a KEK (`--kek-file`, `FRESHDB_KEK`, or a
  `KeyProvider` backed by your KMS), and expire old object versions. Open
  logs a warning in dev-key mode. There is no rekey yet, so a database
  created in dev-key mode cannot move to a real KEK; see the
  [roadmap](ROADMAP.md).
- **Late DEK window.** If a DEK-create PUT is delayed past a shred, an orphan
  DEK can exist until the next open or `verify` sweeps it. freshDB never
  serves plaintext for a shredded subject in that window.
- Keys, topic names and other metadata are **not encrypted**. Only values
  written with a subject are. Do not put personal data in keys.
