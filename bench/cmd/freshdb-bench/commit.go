package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/raman20/freshdb"
)

type commitRun struct {
	Clients   int     `json:"clients"`
	Commits   int     `json:"commits"`
	OpsPerSec float64 `json:"ops_per_sec"`
	Latency   Lat     `json:"latency"`
	Over2ms   int     `json:"over_2ms"`
	Over10ms  int     `json:"over_10ms"`
	FileMiB   float64 `json:"file_mib"`
}

// cmdCommit measures commit latency tails on the default (journal) layout,
// where the file grows in 8 MiB chunks: c clients, one put per commit.
func cmdCommit(args []string) {
	fl := flag.NewFlagSet("commit", flag.ExitOnError)
	dir := fl.String("dir", "/tmp/freshdb-bench/commit", "database directory (wiped per run)")
	commits := fl.Int("commits", 30000, "commits per run")
	vsize := fl.Int("value", 4096, "value bytes")
	clients := fl.String("clients", "1,16", "client counts")
	reps := fl.Int("reps", 3, "repetitions")
	out := fl.String("out", "results", "output directory")
	fl.Parse(args)
	ctx := context.Background()
	res := &Result{Suite: "commit", Env: envInfo(*dir), Params: map[string]any{
		"commits": *commits, "value": *vsize, "clients": *clients, "reps": *reps}}
	val := []byte(strings.Repeat("v", *vsize))
	for rep := 0; rep < *reps; rep++ {
		for _, cs := range strings.Split(*clients, ",") {
			var c int
			fmt.Sscan(cs, &c)
			wipe(*dir)
			db, err := freshdb.Open(ctx, *dir, freshdb.WithLogf(func(string, ...any) {}))
			if err != nil {
				fatal(err)
			}
			hs := make([]*hdrLat, c)
			var wg sync.WaitGroup
			t0 := time.Now()
			for w := 0; w < c; w++ {
				hs[w] = &hdrLat{h: newHist()}
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := w; i < *commits; i += c {
						t := time.Now()
						if err := db.Update(ctx, func(tx *freshdb.Tx) error {
							return tx.Put(fmt.Sprintf("k%07d", i), val)
						}); err != nil {
							fatal(err)
						}
						d := time.Since(t)
						record(hs[w].h, d)
						if d > 2*time.Millisecond {
							hs[w].over2++
						}
						if d > 10*time.Millisecond {
							hs[w].over10++
						}
					}
				}(w)
			}
			wg.Wait()
			el := time.Since(t0).Seconds()
			db.Close()
			h := newHist()
			r := commitRun{Clients: c, Commits: *commits, OpsPerSec: float64(*commits) / el}
			for _, x := range hs {
				h.Merge(x.h)
				r.Over2ms += x.over2
				r.Over10ms += x.over10
			}
			r.Latency = summarize(h)
			_, app, _ := du(*dir)
			r.FileMiB = float64(app) / (1 << 20)
			logf("commit c=%-2d %6.0f commits/s  p50 %6.0fus p99 %6.0fus p99.9 %6.0fus max %7.0fus  >2ms %d >10ms %d  file %.0f MiB",
				c, r.OpsPerSec, r.Latency.P50Us, r.Latency.P99Us, r.Latency.P999Us, r.Latency.MaxUs, r.Over2ms, r.Over10ms, r.FileMiB)
			res.Runs = append(res.Runs, r)
		}
	}
	writeResult(*out, "commit", res)
}

type hdrLat struct {
	h             *hdrhistogram.Histogram
	over2, over10 int
}
