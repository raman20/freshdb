Golden databases for TestGoldenRead / TestGoldenBytes (docs/compatibility.md).

- `v0.1.0/`: written by the v0.1.0 code:

      git worktree add /tmp/wt010 v0.1.0
      mkdir -p /tmp/wt010/testdata/golden && cp -r testdata/golden/gen /tmp/wt010/testdata/golden/
      (cd /tmp/wt010/testdata/golden/gen && go run . $PWD/testdata/golden/v0.1.0)

- `main/`, `main-compacted/`: written by TestGoldenBytes with
  `FRESHDB_UPDATE_GOLDEN=1` (only for an intended format change).

`gen/workload.go` must stay identical to the workload in `golden_test.go`
(TestGoldenWorkloadInSync).
