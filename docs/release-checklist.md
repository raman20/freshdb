# Release checklist

For maintainers. Nothing is tagged or published until every item holds.

1. **CI green** on the release commit (lint incl. staticcheck, govulncheck
   and the API diff; race tests; macOS + cross builds; sim; fuzz; S3 on
   MinIO incl. `s3check`; bench smoke; TLA+; release dry run).
2. **Local gates** on that commit: full suite (gofmt, vet, staticcheck,
   race, sim 2000 seeds, kill -9 crash tests, bench module, TLC).
3. **Vulnerabilities**: `govulncheck ./...` reports nothing the code can
   reach; required modules with known issues are upgraded or named in the
   release notes with why they are unreachable.
4. **API**: the `api/` diff since the last release is reviewed; every
   breaking change is in CHANGELOG "Changed (breaking)"; deprecations
   follow [api-stability.md](api-stability.md).
5. **Formats**: `freshdb version` compared with the last release; a new
   format version has a golden database, a refusal test and a migration
   note in [compatibility.md](compatibility.md).
6. **Honest docs**: STATUS, PRODUCTION_READINESS, README and the site say
   what is tested, with numbers from `bench/results`; experimental parts
   are labelled.
7. **Soak**: `bench/cmd/freshdb-soak` for at least 1 hour on the release
   commit ended with `SOAK OK`.
8. **CHANGELOG**: "Unreleased" becomes the version and date.
9. **Build**: `scripts/release.sh vX.Y.Z` writes archives,
   `sbom.spdx.json` and `checksums.txt` to `dist/` (gitignored).
10. **Sign**: with `FRESHDB_SIGN_KEY` set the script also writes
    `checksums.txt.sig`; verify it against the published allowed-signers
    file. No release key is published yet, so releases are unsigned until
    one is, and the release notes must say so.
11. **Tag and publish** (maintainer only): signed tag; GitHub release with
    the `dist/` files and the CHANGELOG section.
12. **After**: `go install github.com/raman20/freshdb/cmd/freshdb@vX.Y.Z`
    works on a clean machine; downloaded archives match `checksums.txt`.
