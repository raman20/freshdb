#!/usr/bin/env bash
# Full benchmark run. Writes JSON + results.md to bench/results/<date>/.
# Needs: Go (see go.mod), ~10 GiB free disk under $BENCH_DIR, ~4 GiB RAM.
# Optional: a MinIO/S3 endpoint for the s3 suite:
#   FRESHDB_BENCH_S3='s3://bucket?endpoint=127.0.0.1:9000&insecure=1&create=1'
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
# Takes roughly 1.5-2 hours on an 8 vCPU VM. Run on an otherwise idle machine.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${OUT:-results/$(date +%Y-%m-%d)}
BENCH_DIR=${BENCH_DIR:-/tmp/freshdb-bench}
KEYS=${KEYS:-1000000}
mkdir -p "$OUT" bin
go build -o bin/freshdb-bench ./cmd/freshdb-bench
B=bin/freshdb-bench

$B calib -out "$OUT"
for fsync in true false; do
  for sys in freshdb badger pebble bbolt sqlite; do
    $B ycsb -system $sys -fsync=$fsync -keys "$KEYS" -value 1024 -workloads A,B,C,F,D \
      -clients 1,8,64 -warmup 2s -duration 5s -open-frac 0.5 -seed 42 -dir "$BENCH_DIR" -out "$OUT"
  done
  for sys in freshdb nats; do
    $B msg -system $sys -fsync=$fsync -rates 1000,5000,10000,20000 -producers 8 -groups 1,4 \
      -duration 10s -dir "$BENCH_DIR" -out "$OUT"
  done
  $B txn -fsync=$fsync -clients 8,64 -hot 1,16,256,65536 -partitions 1,8 -batch 1,10 \
    -duration 5s -seed 42 -dir "$BENCH_DIR" -out "$OUT"
done
$B vector -n 10000,100000 -dim 128 -clusters 100 -sigma 0.15 -queries 200 -clients 1,8 \
  -duration 10s -seed 42 -dir "$BENCH_DIR" -out "$OUT"
if [[ -n "${FRESHDB_BENCH_S3:-}" ]]; then
  $B s3 -batch 1,10,100,1000 -clients 1,8,64 -segments 100,1000,5000 -duration 10s -out "$OUT"
else
  echo "FRESHDB_BENCH_S3 not set: skipping s3 suite" >&2
fi
$B calib -out "$OUT/calib-end" # noise floor again at the end
$B report -dir "$OUT"
echo "done: $OUT/results.md"
