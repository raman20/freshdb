// Command freshdb-bench is freshDB's reproducible benchmark suite. Every
// subcommand writes one JSON file to -out; `freshdb-bench report` turns a
// results directory into markdown. See docs/benchmarks.md.
package main

import (
	"fmt"
	"os"
)

const usage = `freshdb-bench <suite> [flags]

suites:
  ycsb     KV workloads A,B,C,D,F (zipfian) on freshdb|badger|pebble|bbolt|sqlite
  msg      topic producer->consumer latency at fixed rates, max throughput, groups
  nats     the same messaging workload on embedded NATS JetStream (baseline)
  txn      contended OCC transactions and multi-lens consume+write transactions
  vector   recall@10 and QPS on synthetic clustered vectors (exact search)
  s3       commit latency, batching, requests/commit and cold open on S3/MinIO
  compact  storage, open time and heap before/after compaction (overwrite-heavy)
  calib    harness + VM noise floor (no-op latency)
  report   render results/*.json as markdown

run "freshdb-bench <suite> -h" for flags.`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	args := os.Args[2:]
	switch os.Args[1] {
	case "ycsb":
		cmdYCSB(args)
	case "msg":
		cmdMsg(args)
	case "nats":
		cmdMsg(append([]string{"-system", "nats"}, args...))
	case "txn":
		cmdTxn(args)
	case "vector":
		cmdVector(args)
	case "s3":
		cmdS3(args)
	case "compact":
		cmdCompact(args)
	case "calib":
		cmdCalib(args)
	case "report":
		cmdReport(args)
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
}

func wipe(dir string) { os.RemoveAll(dir) }
