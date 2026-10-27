package main

import (
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

type calibRun struct {
	What    string  `json:"what"`
	Clients int     `json:"clients,omitempty"`
	Rate    float64 `json:"rate,omitempty"`
	Latency Lat     `json:"latency"`
}

// cmdCalib measures the harness's own noise floor on this machine: the
// open-loop scheduler with a no-op, and a pure spin loop. Any engine latency
// below these numbers is not distinguishable from scheduler/VM noise.
func cmdCalib(args []string) {
	fs := flag.NewFlagSet("calib", flag.ExitOnError)
	dur := fs.Duration("duration", 3*time.Second, "per cell")
	out := fs.String("out", "results", "output directory")
	ratesF := fs.String("rates", "20000,200000,1000000", "open-loop no-op rates (ops/s) to calibrate")
	dir := fs.String("dir", "/tmp/freshdb-bench", "directory on the disk under test (for the fsync probe)")
	fs.Parse(args)
	res := &Result{Suite: "calib", Env: envInfo("/tmp"), Params: map[string]any{"duration": dur.String()}}
	h := newHist()
	start := time.Now()
	iv := 100 * time.Microsecond
	for n := 0; time.Since(start) < *dur; n++ {
		due := start.Add(time.Duration(n) * iv)
		for time.Now().Before(due) {
			runtime.Gosched()
		}
		record(h, time.Since(due))
	}
	res.Runs = append(res.Runs, calibRun{What: "spin loop: lateness vs schedule (10k/s)", Latency: summarize(h)})
	for _, r := range parseInts(*ratesF) {
		for _, c := range []int{1, 8, 64} {
			p := runOpen(c, float64(r), *dur, func(int) opFunc { return func() error { return nil } })
			res.Runs = append(res.Runs, calibRun{What: "open-loop harness, no-op", Clients: c, Rate: float64(r), Latency: p.Latency})
			logf("calib no-op %d/s c=%d achieved %.0f/s p50 %.1fus p99 %.1fus p99.9 %.1fus", r, c, p.OpsPerSec, p.Latency.P50Us, p.Latency.P99Us, p.Latency.P999Us)
		}
	}
	// The disk: how long does a 4 KiB append + fsync take here? Engines that
	// fsync per commit cannot have a lower commit-latency tail than this.
	os.MkdirAll(*dir, 0o755)
	f, err := os.CreateTemp(*dir, "fsync-probe-*")
	if err != nil {
		fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()
	buf := make([]byte, 4096)
	h = newHist()
	for start := time.Now(); time.Since(start) < *dur; {
		t := time.Now()
		if _, err := f.Write(buf); err != nil {
			fatal(err)
		}
		if err := f.Sync(); err != nil {
			fatal(err)
		}
		record(h, time.Since(t))
	}
	l := summarize(h)
	res.Runs = append(res.Runs, calibRun{What: "disk: 4 KiB append + fsync, sequential, in " + filepath.Clean(*dir), Latency: l})
	logf("calib fsync p50 %.0fus p99 %.0fus p99.9 %.0fus max %.0fus (%d syncs)", l.P50Us, l.P99Us, l.P999Us, l.MaxUs, l.Count)
	writeResult(*out, "calib", res)
}
