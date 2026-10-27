package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type workload struct {
	Name, Desc string
	Read       float64 // fraction of reads; the rest is the write op below
	Write      string  // update | insert | rmw | none
	Latest     bool    // reads skewed to the most recently inserted keys
}

var workloads = map[string]workload{
	"A": {"A", "50% read / 50% update", 0.5, "update", false},
	"B": {"B", "95% read / 5% update", 0.95, "update", false},
	"C": {"C", "100% read", 1, "none", false},
	"D": {"D", "95% read latest / 5% insert", 0.95, "insert", true},
	"F": {"F", "50% read / 50% read-modify-write", 0.5, "rmw", false},
}

type ycsbRun struct {
	System     string `json:"system"`
	Fsync      bool   `json:"fsync"`
	Durability string `json:"durability"`
	Workload   string `json:"workload"`
	Desc       string `json:"desc"`
	Clients    int    `json:"clients"`
	Closed     Phase  `json:"closed_loop"`
	Open       Phase  `json:"open_loop"`
}

type ycsbParams struct {
	System     string  `json:"system"`
	Fsync      bool    `json:"fsync"`
	Keys       int     `json:"keys"`
	ValueBytes int     `json:"value_bytes"`
	Workloads  string  `json:"workloads"`
	Clients    []int   `json:"clients"`
	Warmup     string  `json:"warmup"`
	Duration   string  `json:"duration"`
	OpenFrac   float64 `json:"open_loop_fraction_of_max"`
	OpenCap    float64 `json:"open_loop_cap_ops_per_sec"`
	Seed       uint64  `json:"seed"`
	Theta      float64 `json:"zipf_theta"`
	LoadSecs   float64 `json:"load_seconds"`
	LoadRate   float64 `json:"load_keys_per_sec"`
	DiskAfter  string  `json:"disk_after_run"`
	PeakRSS    string  `json:"peak_rss"`
}

func keyName(i uint64) string { return fmt.Sprintf("user%019d", i) }

func cmdYCSB(args []string) {
	fs := flag.NewFlagSet("ycsb", flag.ExitOnError)
	system := fs.String("system", "freshdb", "freshdb|badger|pebble|bbolt|sqlite")
	fsync := fs.Bool("fsync", true, "durable commits (false = each engine's no-sync mode)")
	keys := fs.Int("keys", 1_000_000, "records loaded before the run")
	vsize := fs.Int("value", 1024, "value size in bytes")
	wls := fs.String("workloads", "A,B,C,F,D", "YCSB workloads, run in this order on the same data (YCSB's recommended order)")
	clientsF := fs.String("clients", "1,8,64", "concurrent clients")
	warm := fs.Duration("warmup", 2*time.Second, "closed-loop warm-up per cell (discarded)")
	dur := fs.Duration("duration", 5*time.Second, "measurement per phase")
	frac := fs.Float64("open-frac", 0.5, "open-loop target rate as a fraction of the measured closed-loop max")
	openCap := fs.Float64("open-cap", 200000, "upper bound on the open-loop rate (ops/s); above ~200k/s the harness itself adds ms of tail latency on the reference box (see calib)")
	seed := fs.Uint64("seed", 42, "RNG seed")
	theta := fs.Float64("theta", 0.99, "zipfian constant")
	dataDir := fs.String("dir", "/tmp/freshdb-bench", "data directory (wiped)")
	out := fs.String("out", "results", "output directory")
	fs.DurationVar(&boltBatchDelay, "bolt-batch-delay", 0, "bbolt only: 0 = db.Update per write; >0 = db.Batch with this MaxBatchDelay")
	fs.Parse(args)
	clients := parseInts(*clientsF)

	dir := filepath.Join(*dataDir, fmt.Sprintf("ycsb-%s-%v", *system, *fsync))
	kv, err := openKV(*system, dir, *fsync)
	if err != nil {
		fatal(err)
	}
	pool := make([]byte, 1<<20+*vsize)
	r0 := rand.New(rand.NewPCG(*seed, 0))
	for i := range pool {
		pool[i] = byte(r0.Uint32())
	}
	// a fresh copy per op, as a real application would hand over (engines may retain it)
	val := func(r *rand.Rand) []byte { o := r.IntN(1 << 20); return append([]byte(nil), pool[o:o+*vsize]...) }

	logf("ycsb %s fsync=%v: loading %d x %dB", *system, *fsync, *keys, *vsize)
	lr := rand.New(rand.NewPCG(*seed, 1))
	t0 := time.Now()
	if err := kv.Load(*keys, func(i int) string { return keyName(uint64(i)) }, func(int) []byte { return val(lr) }); err != nil {
		fatal(fmt.Errorf("load: %w", err))
	}
	loadSecs := time.Since(t0).Seconds()
	logf("loaded in %.1fs", loadSecs)

	n := uint64(*keys)
	z := newZipfian(n, *theta)
	var inserted atomic.Uint64 // next insert id to hand out
	inserted.Store(n)
	acked := newAckCounter(n) // like YCSB's AcknowledgedCounterGenerator: reads only see fully committed prefixes
	res := &Result{Suite: "ycsb", Env: envInfo(*dataDir)}
	for _, name := range strings.Split(*wls, ",") {
		wl, ok := workloads[strings.TrimSpace(name)]
		if !ok {
			fatal(fmt.Errorf("unknown workload %q", name))
		}
		for _, c := range clients {
			mk := func(salt uint64) func(id int) opFunc {
				return func(id int) opFunc {
					r := rand.New(rand.NewPCG(*seed, salt*1000+uint64(id)+7))
					pick := func() string {
						if wl.Latest {
							// read only keys whose insert (and every earlier insert) has committed
							top := acked.limit()
							rank := z.next(r)
							if rank >= top {
								rank = top - 1
							}
							return keyName(top - 1 - rank)
						}
						return keyName(scrambled(z.next(r), n))
					}
					return func() error {
						if r.Float64() < wl.Read {
							_, err := kv.Get(pick())
							return err
						}
						switch wl.Write {
						case "update":
							return kv.Put(pick(), val(r))
						case "insert":
							id := inserted.Add(1) - 1
							err := kv.Put(keyName(id), val(r))
							if err == nil {
								acked.ack(id)
							}
							return err
						case "rmw":
							v := val(r)
							return kv.RMW(pick(), func(old []byte) []byte { return v })
						}
						return nil
					}
				}
			}
			runClosed(c, *warm, mk(1))
			closed := runClosed(c, *dur, mk(2))
			rate := closed.OpsPerSec * *frac
			if rate > *openCap {
				rate = *openCap
			}
			open := runOpen(c, rate, *dur, mk(3))
			run := ycsbRun{System: *system, Fsync: *fsync, Durability: kv.Durability(), Workload: wl.Name, Desc: wl.Desc, Clients: c, Closed: closed, Open: open}
			res.Runs = append(res.Runs, run)
			logf("%s %s fsync=%v c=%d: max %.0f ops/s | open @%.0f/s p50 %.1fus p99 %.1fus p99.9 %.1fus | errs %d/%d not-issued %d",
				*system, wl.Name, *fsync, c, closed.OpsPerSec, rate, open.Latency.P50Us, open.Latency.P99Us, open.Latency.P999Us, closed.Errors, open.Errors, open.NotIssued)
		}
	}
	kv.Close()
	runtime.GC()
	res.Params = ycsbParams{System: *system, Fsync: *fsync, Keys: *keys, ValueBytes: *vsize, Workloads: *wls, Clients: clients,
		Warmup: warm.String(), Duration: dur.String(), OpenFrac: *frac, OpenCap: *openCap, Seed: *seed, Theta: *theta,
		LoadSecs: round(loadSecs, 2), LoadRate: round(float64(*keys)/loadSecs, 0), DiskAfter: diskUsage(dir), PeakRSS: peakRSS()}
	writeResult(*out, fmt.Sprintf("ycsb-%s-fsync-%v", *system, *fsync), res)
	wipe(dir)
}

func parseInts(s string) []int {
	var out []int
	for _, f := range strings.Split(s, ",") {
		var v int
		fmt.Sscan(strings.TrimSpace(f), &v)
		if v > 0 {
			out = append(out, v)
		}
	}
	return out
}

// ackCounter tracks the largest prefix of insert ids that have all committed.
type ackCounter struct {
	mu   sync.Mutex
	next uint64 // every id < next is committed
	done map[uint64]bool
}

func newAckCounter(n uint64) *ackCounter { return &ackCounter{next: n, done: map[uint64]bool{}} }

func (a *ackCounter) ack(id uint64) {
	a.mu.Lock()
	a.done[id] = true
	for a.done[a.next] {
		delete(a.done, a.next)
		a.next++
	}
	a.mu.Unlock()
}

func (a *ackCounter) limit() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.next
}
