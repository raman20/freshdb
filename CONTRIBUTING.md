# Contributing to freshDB

Thanks for your interest! freshDB is a small, young project. Contributions of
every size are welcome: bug reports, failing simulator seeds, docs, tests and code.

## Before you start

- Read [docs/design-principles.md](docs/design-principles.md), especially
  **"What we refuse to build"**. It saves everyone time.
- For anything bigger than a small fix, **open an issue first** so we can agree
  on the approach.
- Look for [`good first issue`](https://github.com/raman20/freshdb/labels/good%20first%20issue)
  and the list in [ROADMAP.md](ROADMAP.md#good-first-issues).
- Be kind. This project follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## Development setup

You need Go (the version in `go.mod`), plus optionally `jq` and `curl` for the
demo, Docker for MinIO, and Java 11+ for TLC.

```sh
git clone https://github.com/raman20/freshdb && cd freshdb
go test -race ./...                       # unit, property, crash, sim (500 seeds)
go run ./examples/tour                    # API walkthrough
go build -o /tmp/freshdb ./cmd/freshdb && FRESHDB=/tmp/freshdb ./examples/agent-demo.sh
```

S3 tests (skipped unless configured):

```sh
docker run -d -p 9000:9000 -e MINIO_ROOT_USER=dev -e MINIO_ROOT_PASSWORD=devpassword minio/minio server /data
FRESHDB_TEST_S3='s3://freshdb-test?endpoint=127.0.0.1:9000&insecure=1&create=1' \
  AWS_ACCESS_KEY_ID=dev AWS_SECRET_ACCESS_KEY=devpassword go test -run S3 -v ./ ./store
```

## The correctness bar

freshDB is a database, so changes to its core carry a higher bar:

| You changed… | Also do this |
|---|---|
| anything | `gofmt`, `go vet`, `staticcheck`, `go test -race ./...` |
| a lens | keep `lens == fold(log)` and `DropSubject == fold(redacted)`; property tests in `lens/` |
| writer, recovery, fencing, branches, shred | run the simulator with more seeds: `go test ./sim -run TestSimulation -seeds=5000` |
| the protocol (what is written to storage, and when) | update the TLA+ spec in `spec/`, run `spec/run_tlc.sh`, update `spec/RESULTS.md` |
| the on-storage format | update `docs/formats.md`, bump the version, add a CHANGELOG entry |

When the simulator finds a failure, it prints the seed. Reproduce it with
`go test ./sim -run TestSimulation -seed=N`. Please put the seed in your issue or PR.

## Pull requests

- Keep each PR focused, and include tests.
- Fill in the PR template checklist.
- CI must be green. It runs lint, race tests, a 2,000-seed simulation, fuzzing,
  MinIO S3 tests and TLC.
- Commits are squash-merged. Write a clear PR title.

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
