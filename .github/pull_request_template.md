## What and why

<!-- One or two sentences. Link the issue: Fixes #123 -->

## How it was tested

- [ ] `go test -race ./...` passes
- [ ] `gofmt`, `go vet`, `staticcheck` clean
- [ ] For changes to the log, fencing, transactions, branches or shred: simulator run with more seeds (`go test ./sim -run TestSimulation -seeds=5000`)
- [ ] If the protocol changed: the TLA+ spec in `spec/` is updated and `spec/run_tlc.sh` passes; `spec/RESULTS.md` updated
- [ ] If a lens changed: property tests in `lens/` still hold (`lens == fold(log)`)

## Compatibility

- [ ] No change to the on-storage format (`docs/formats.md`) — or the change is versioned and documented
- [ ] Docs/README/CHANGELOG updated if user-visible
