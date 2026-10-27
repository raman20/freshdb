package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// newHist records nanoseconds from 1ns to 120s with 3 significant digits.
func newHist() *hdrhistogram.Histogram { return hdrhistogram.New(1, 120e9, 3) }

// Lat is a latency summary in microseconds.
type Lat struct {
	Count  int64   `json:"count"`
	MeanUs float64 `json:"mean_us"`
	P50Us  float64 `json:"p50_us"`
	P99Us  float64 `json:"p99_us"`
	P999Us float64 `json:"p999_us"`
	MaxUs  float64 `json:"max_us"`
}

func summarize(h *hdrhistogram.Histogram) Lat {
	us := func(v int64) float64 { return math.Round(float64(v)/10) / 100 }
	return Lat{
		Count: h.TotalCount(), MeanUs: math.Round(h.Mean()/10) / 100,
		P50Us: us(h.ValueAtQuantile(50)), P99Us: us(h.ValueAtQuantile(99)),
		P999Us: us(h.ValueAtQuantile(99.9)), MaxUs: us(h.Max()),
	}
}

func record(h *hdrhistogram.Histogram, d time.Duration) {
	v := int64(d)
	if v < 1 {
		v = 1
	}
	if v > 120e9 {
		v = 120e9
	}
	h.RecordValue(v)
}

// opFunc runs one operation; it returns an error that is counted, not fatal.
type opFunc func() error

// stopAware reports errors that mean "this client is done" (ends the closed loop early).
var stopAware = func(error) bool { return false }

// Phase is the outcome of one measured phase.
type Phase struct {
	Mode       string  `json:"mode"` // closed | open
	Seconds    float64 `json:"seconds"`
	Ops        int64   `json:"ops"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	TargetRate float64 `json:"target_ops_per_sec,omitempty"`
	Errors     int64   `json:"errors"`
	FirstError string  `json:"first_error,omitempty"`
	Latency    Lat     `json:"latency"`
	GC         *GCStat `json:"go_gc,omitempty"`
}

// GCStat is what the Go garbage collector did during a phase. Every engine
// here runs in the benchmark process, so GC pauses and assists land in its
// latency; this makes that visible.
type GCStat struct {
	Cycles      uint32  `json:"cycles"`
	PauseMs     float64 `json:"stw_pause_total_ms"`
	MaxPauseMs  float64 `json:"stw_pause_max_ms"`
	HeapEndMB   float64 `json:"heap_inuse_end_mb"`
	CPUFraction float64 `json:"gc_cpu_fraction_cumulative"`
}

func gcStart() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func gcSince(a runtime.MemStats) *GCStat {
	var b runtime.MemStats
	runtime.ReadMemStats(&b)
	g := &GCStat{Cycles: b.NumGC - a.NumGC, PauseMs: round(float64(b.PauseTotalNs-a.PauseTotalNs)/1e6, 2),
		HeapEndMB: round(float64(b.HeapInuse)/(1<<20), 0), CPUFraction: round(b.GCCPUFraction, 4)}
	for i := a.NumGC; i < b.NumGC && i < a.NumGC+256; i++ {
		if ms := float64(b.PauseNs[(i+1+255)%256]) / 1e6; ms > g.MaxPauseMs {
			g.MaxPauseMs = round(ms, 2)
		}
	}
	return g
}

// runClosed: each client issues its next op as soon as the previous one
// returns. Gives maximum throughput; latency here is service time only.
func runClosed(clients int, d time.Duration, mk func(id int) opFunc) Phase {
	gc0 := gcStart()
	var ops, errs atomic.Int64
	var firstErr atomic.Value
	hs := make([]*hdrhistogram.Histogram, clients)
	var wg sync.WaitGroup
	start := time.Now()
	end := start.Add(d)
	for i := 0; i < clients; i++ {
		hs[i] = newHist()
		op := mk(i)
		wg.Add(1)
		go func(h *hdrhistogram.Histogram) {
			defer wg.Done()
			for {
				t := time.Now()
				if !t.Before(end) {
					return
				}
				if err := op(); err != nil {
					if stopAware(err) {
						return
					}
					errs.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
				}
				record(h, time.Since(t))
				ops.Add(1)
			}
		}(hs[i])
	}
	wg.Wait()
	el := time.Since(start).Seconds()
	p := finish("closed", el, ops.Load(), errs.Load(), firstErr, hs, 0)
	p.GC = gcSince(gc0)
	return p
}

// runOpen issues ops on a fixed schedule (op i is due at start + i/rate) and
// measures each op's latency from its *intended* start time, so queueing
// behind a slow op is counted (coordinated-omission aware, like wrk2).
// One dispatcher goroutine paces the schedule (sleeping only for long gaps
// and spin-yielding for the last 2ms, because OS timer slack on VMs is
// ~1ms) and hands due times to the clients over an unbuffered channel.
// The dispatcher costs about one CPU core.
func runOpen(clients int, rate float64, d time.Duration, mk func(id int) opFunc) Phase {
	return runOpenDue(clients, rate, d, func(id int) func(time.Time) error {
		op := mk(id)
		return func(time.Time) error { return op() }
	})
}

// runOpenDue is runOpen where each op also receives its intended start time.
// runOpenDue is an open-loop (fixed-rate) run, paced per client like wrk2:
// client i owns schedule slots i, i+clients, i+2*clients, ... of a global
// schedule at the target rate, so there is no shared dispatcher to become the
// bottleneck. Latency is measured from each op's intended start time, so a
// client that falls behind is charged for the queueing (coordinated omission
// is corrected). The wait before each slot sleeps when it is long and yields
// (runtime.Gosched) for the last 2ms, because VM timer slack is ~1ms.
func runOpenDue(clients int, rate float64, d time.Duration, mk func(id int) func(due time.Time) error) Phase {
	gc0 := gcStart()
	var ops, errs atomic.Int64
	var firstErr atomic.Value
	hs := make([]*hdrhistogram.Histogram, clients)
	var wg sync.WaitGroup
	interval := float64(time.Second) / rate
	total := int64(rate * d.Seconds())
	start := time.Now().Add(2 * time.Millisecond)
	for i := 0; i < clients; i++ {
		hs[i] = newHist()
		op := mk(i)
		wg.Add(1)
		go func(i int, h *hdrhistogram.Histogram) {
			defer wg.Done()
			for n := int64(i); n < total; n += int64(clients) {
				due := start.Add(time.Duration(float64(n) * interval))
				for {
					w := time.Until(due)
					if w <= 0 {
						break
					}
					if w > 2*time.Millisecond {
						time.Sleep(w - 2*time.Millisecond)
					} else {
						runtime.Gosched()
					}
				}
				if err := op(due); err != nil {
					errs.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
				}
				record(h, time.Since(due))
				ops.Add(1)
			}
		}(i, hs[i])
	}
	wg.Wait()
	el := time.Since(start).Seconds()
	p := finish("open", el, ops.Load(), errs.Load(), firstErr, hs, rate)
	p.GC = gcSince(gc0)
	return p
}

func finish(mode string, el float64, ops, errs int64, fe atomic.Value, hs []*hdrhistogram.Histogram, rate float64) Phase {
	all := newHist()
	for _, h := range hs {
		all.Merge(h)
	}
	p := Phase{Mode: mode, Seconds: round(el, 2), Ops: ops, OpsPerSec: round(float64(ops)/el, 1), TargetRate: round(rate, 1), Errors: errs, Latency: summarize(all)}
	if s, ok := fe.Load().(string); ok {
		p.FirstError = s
	}
	return p
}

func round(v float64, d int) float64 { p := math.Pow(10, float64(d)); return math.Round(v*p) / p }

// Env describes where the numbers came from.
type Env struct {
	Time      string `json:"time"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Kernel    string `json:"kernel"`
	CPU       string `json:"cpu"`
	CPUs      int    `json:"cpus"`
	MemTotal  string `json:"mem_total"`
	Disk      string `json:"disk"`
	Commit    string `json:"freshdb_commit"`
	Command   string `json:"command"`
	LoadAvg   string `json:"loadavg_at_start"`
}

func envInfo(dir string) Env {
	e := Env{Time: time.Now().Format(time.RFC3339), GoVersion: runtime.Version(), OS: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU(), Command: strings.Join(os.Args, " ")}
	e.Kernel = strings.TrimSpace(sh("uname", "-r"))
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		e.LoadAvg = strings.Join(strings.Fields(string(b))[:3], " ")
	}
	if f, err := os.Open("/proc/cpuinfo"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "model name") {
				e.CPU = strings.TrimSpace(strings.SplitN(sc.Text(), ":", 2)[1])
				break
			}
		}
		f.Close()
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		e.MemTotal = strings.Join(strings.Fields(strings.SplitN(string(b), "\n", 2)[0])[1:], " ")
	}
	os.MkdirAll(dir, 0o755)
	e.Disk = strings.TrimSpace(sh("sh", "-c", "df -hT "+dir+" | tail -1 | awk '{print $1\" \"$2\" \"$3}'"))
	e.Commit = strings.TrimSpace(sh("git", "rev-parse", "--short", "HEAD"))
	if d := strings.TrimSpace(sh("git", "status", "--porcelain", "--untracked-files=no")); d != "" {
		e.Commit += "-dirty"
	}
	return e
}

func sh(name string, args ...string) string {
	b, _ := exec.Command(name, args...).Output()
	return string(b)
}

// peakRSS reports this process's peak resident set size (Linux only).
func peakRSS() string {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return ""
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmHWM:") {
			return strings.Join(strings.Fields(l)[1:], " ")
		}
	}
	return ""
}

func diskUsage(dir string) string {
	return strings.Fields(sh("du", "-sh", dir) + " ?")[0]
}

// Result is one JSON file written per invocation.
type Result struct {
	Suite  string   `json:"suite"`
	Env    Env      `json:"env"`
	Params any      `json:"params"`
	Runs   []any    `json:"runs"`
	Notes  []string `json:"notes,omitempty"`
}

func writeResult(outDir, name string, r *Result) {
	os.MkdirAll(outDir, 0o755)
	p := filepath.Join(outDir, name+".json")
	b, _ := json.MarshalIndent(r, "", "  ")
	if err := os.WriteFile(p, append(b, '\n'), 0o644); err != nil {
		fatal(err)
	}
	fmt.Fprintln(os.Stderr, "wrote", p)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "freshdb-bench:", err)
	os.Exit(1)
}

func logf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, time.Now().Format("15:04:05 ")+format+"\n", a...)
}
