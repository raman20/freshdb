# Open time, 2026-10-29

`go test -run XXX -bench BenchmarkOpenRecover10k -benchtime 10x .`, FS store
wrapped in `slowGets` (bench_test.go: fixed latency per GET, counts segment
GETs). Shared 8-core box (a 6h soak was running), so the get=0s rows are
noisy; the get=2ms rows are dominated by GET latency.

- `before`: HEAD 29ae382 with this benchmark file.
- `after`: segment-index checkpoints, sequential replay.
- `after2`: checkpoints + replay read-ahead (8 in flight).
