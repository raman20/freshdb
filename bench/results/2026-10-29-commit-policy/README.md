# Commit policy (group-commit budget), 2026-10-29

`go test -run XXX -bench BenchmarkCommitPolicy -benchtime 2s -count 1 .`
on the shared 8-core build box (other jobs running; numbers are indicative).

The store is the in-memory store with a fixed 2 ms sleep per segment PUT
(`slowPuts` in bench_test.go), standing in for an object store round trip.
`ns/op` is wall time per transaction across all writers; `ms-lat/txn` the
mean commit latency seen by a writer; `puts/txn` segment PUTs per
transaction. Summary and reading: docs/operations.md ("Commit policy").
