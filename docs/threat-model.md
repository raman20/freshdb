# Threat model

Scope: freshDB as shipped in this repository (library, `freshdb serve`,
CLI), pre-1.0. Summary in [security.md](security.md); reporting in
[SECURITY.md](../SECURITY.md).

## Assets

1. Acknowledged data (must not be lost or silently altered).
2. Plaintext of subject-tagged values, especially after a shred.
3. Integrity of history (hash chain, shred certificates).
4. Availability of the server.
5. Credentials: API tokens, KEK, S3 keys.

## Actors and trust

| actor | trusted? | can |
|---|---|---|
| process embedding freshDB / running `serve` | fully | everything |
| storage (directory, bucket) and whoever can write to it | for availability; **not** for silent corruption | delete or rewrite objects |
| admin token holder | trusted | shred, compact, GC, rewind, backup to a server-side path, pprof |
| read/write token holder (optionally scoped to collections) | partly | read or write within scope |
| network client without a token | untrusted | `/healthz`, `/readyz` (with auth on) |
| network attacker on the path | untrusted | sees and alters traffic unless TLS is on |
| a second writer process (stale or misconfigured) | untrusted for correctness | is fenced by the manifest epoch |

## Threats

| # | threat | mitigation | status / evidence |
|---|---|---|---|
| T1 | stale writer commits after a takeover (split brain) | create-only manifests with epochs; every commit checks the epoch | TLA+ model, simulator, kill -9 and soak tests; on S3 relies on conditional PUT (`s3check`) |
| T2 | storage corruption returns wrong data | CRC per record/object, hash chain, `verify`, `ErrCorrupt` (no silent repair) | corruption and power-loss tests (design 12) |
| T3 | shredded plaintext recovered | per-subject DEK deleted; AEAD bound to position | tests. **Limits:** dev-mode KEK in the bucket; old object versions or backups holding a DEK |
| T4 | eavesdropping / token theft | `--tls-cert/--tls-key`; non-loopback bind refused without a token; tokens kept as SHA-256 hashes in memory | no mTLS; bearer tokens; the token file holds them in plaintext (protect it) |
| T5 | escalation between token scopes | scope check per route (design 11) | scoped-token tests (`server/scopes_test.go`); not an exhaustive per-route matrix |
| T6 | resource exhaustion | body/tx/key size limits, per-request read/write deadlines, slow-client cut-off, drain on shutdown | **open:** no rate limits, no global in-flight cap; key index RAM-bound (S5.1); disk full halts writes (tested) |
| T7 | malicious bytes crash the process | bounds-checked decoders; fuzz targets in CI | short fuzz runs per CI |
| T8 | admin token used to read/write server paths (`backup?to=`) | admin scope only | **by design**: admin is root over the server's storage view |
| T9 | pprof leaks memory contents | off by default, admin scope, refused without auth | — |
| T10 | supply chain | few deps pinned in `go.sum`, govulncheck in CI, SBOM + checksums + optional SSH signature from `scripts/release.sh` (dry-run verified in CI) | **no release signing key published yet**; no reproducible-build check |
| T11 | metadata leaks (keys, names, sizes, timing) | none: only values with a subject are encrypted | documented limitation |

## Out of scope

A compromised host or Go runtime; side channels; a storage provider that
serves a consistent but forged *older* state of the whole bucket (the hash
chain detects edits, not a coherent rollback).
