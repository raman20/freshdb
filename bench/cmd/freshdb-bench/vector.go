package main

import (
	"container/heap"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raman20/freshdb"
)

type vecRun struct {
	System     string  `json:"system"`
	N          int     `json:"n"`
	Dim        int     `json:"dim"`
	Clients    int     `json:"clients"`
	K          int     `json:"k"`
	RecallAt10 float64 `json:"recall_at_10"`
	QPS        float64 `json:"qps"`
	Latency    Lat     `json:"latency"`
	InsertPerS float64 `json:"insert_vectors_per_sec,omitempty"`
	Index      string  `json:"index"`
	BuildSecs  float64 `json:"hnsw_build_secs,omitempty"` // EnableANN on the loaded collection
	OpenSecs   float64 `json:"open_secs,omitempty"`       // reopen incl. replay (+ graph build with ANN)
	HeapMiB    float64 `json:"heap_after_open_mib,omitempty"`
	CloseSecs  float64 `json:"close_secs,omitempty"`        // Close incl. saving the graph
	CatchSecs  float64 `json:"catchup_open_secs,omitempty"` // reopen after a crash with 1% newer vectors than the saved graph

}

func cmdVector(args []string) {
	fs := flag.NewFlagSet("vector", flag.ExitOnError)
	nF := fs.String("n", "10000,100000", "dataset sizes")
	dim := fs.Int("dim", 128, "dimensions (SIFT-like)")
	clusters := fs.Int("clusters", 100, "gaussian clusters")
	sigma := fs.Float64("sigma", 0.15, "cluster spread (centers uniform in [-1,1]^dim)")
	nq := fs.Int("queries", 200, "queries (drawn from the same mixture, not in the dataset)")
	clientsF := fs.String("clients", "1,8", "concurrent query clients")
	dur := fs.Duration("duration", 5*time.Second, "per cell")
	seed := fs.Uint64("seed", 42, "RNG seed")
	dataDir := fs.String("dir", "/tmp/freshdb-bench", "data directory (wiped)")
	out := fs.String("out", "results", "output directory")
	annF := fs.String("ann", "", "HNSW beam widths to test after the exact runs, e.g. 32,64,128 (empty: exact only)")
	annM := fs.Int("ann-m", 16, "HNSW M")
	annEfc := fs.Int("ann-efc", 100, "HNSW efConstruction")
	flatOn := fs.Bool("flat", true, "also run the flat Go loop")
	fs.Parse(args)
	res := &Result{Suite: "vector", Env: envInfo(*dataDir), Params: map[string]any{"dim": *dim, "clusters": *clusters, "sigma": *sigma,
		"queries": *nq, "duration": dur.String(), "seed": *seed, "metric": "cosine", "fsync": false,
		"dataset": "synthetic gaussian clusters (not SIFT/GloVe); ground truth by exact float64 search"}}
	if *annF != "" {
		res.Params.(map[string]any)["ann"] = map[string]any{"ef": *annF, "m": *annM, "ef_construction": *annEfc}
		res.Notes = append(res.Notes, "freshdb-hnsw-efN rows use the opt-in HNSW graph (EnableANN) with beam width N; recall is against the same float64 ground truth. Exact search is the 'freshdb' row.")
	}
	res.Notes = append(res.Notes, "Exact rows (freshdb, flat-go-loop): search is exact brute force, so recall is 1.0 by construction and QPS falls linearly with N. The flat Go loop is a lower bound on brute-force cost, not an ANN competitor.")
	for _, n := range parseInts(*nF) {
		r := rand.New(rand.NewPCG(*seed, uint64(n)))
		centers := make([][]float32, *clusters)
		for i := range centers {
			centers[i] = make([]float32, *dim)
			for j := range centers[i] {
				centers[i][j] = float32(r.Float64()*2 - 1)
			}
		}
		gen := func() []float32 {
			c := centers[r.IntN(*clusters)]
			v := make([]float32, *dim)
			for j := range v {
				v[j] = c[j] + float32(r.NormFloat64()**sigma)
			}
			return v
		}
		data := make([][]float32, n)
		for i := range data {
			data[i] = gen()
		}
		qs := make([][]float32, *nq)
		for i := range qs {
			qs[i] = gen()
		}
		truth := groundTruth(data, qs, 10)

		dir := filepath.Join(*dataDir, "vector")
		db, err := openFreshDB(dir, false, "")
		if err != nil {
			fatal(err)
		}
		t0 := time.Now()
		for i := 0; i < n; i += 1000 {
			if err := db.Update(bg, func(tx *freshdb.Tx) error {
				for j := i; j < i+1000 && j < n; j++ {
					if err := tx.Vector("v").Put(fmt.Sprintf("v%d", j), data[j]); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				fatal(err)
			}
		}
		ins := float64(n) / time.Since(t0).Seconds()
		fresh := func(q []float32) []string {
			var ids []string
			db.View(bg, func(tx *freshdb.Tx) error {
				hits, err := tx.Vector("v").Search(q, 10)
				for _, h := range hits {
					ids = append(ids, h.ID)
				}
				return err
			})
			return ids
		}
		flat := newFlat(data)
		systems := []string{"freshdb"}
		if *flatOn {
			systems = append(systems, "flat-go-loop")
		}
		for _, sys := range systems {
			search := fresh
			index := "none (exact brute force over the vector lens)"
			if sys != "freshdb" {
				search = flat.search
				index = "none (normalized float32 slice + heap, no engine)"
			}
			rec := 0.0
			for i, q := range qs {
				rec += recall(search(q), truth[i])
			}
			rec /= float64(len(qs))
			for _, c := range parseInts(*clientsF) {
				var qi atomic.Int64
				ph := runClosed(c, *dur, func(int) opFunc {
					return func() error { search(qs[int(qi.Add(1))%len(qs)]); return nil }
				})
				vr := vecRun{System: sys, N: n, Dim: *dim, Clients: c, K: 10, RecallAt10: round(rec, 4), QPS: ph.OpsPerSec, Latency: ph.Latency, Index: index}
				if sys == "freshdb" {
					vr.InsertPerS = round(ins, 0)
				}
				res.Runs = append(res.Runs, vr)
				logf("vector %s n=%d c=%d: recall@10 %.4f qps %.1f p50 %.0fus p99 %.0fus", sys, n, c, rec, ph.OpsPerSec, ph.Latency.P50Us, ph.Latency.P99Us)
			}
		}
		flat = nil
		if *annF != "" {
			t0 := time.Now()
			if err := db.EnableANN(bg, "v", freshdb.ANNOptions{M: *annM, EfConstruction: *annEfc}); err != nil {
				fatal(err)
			}
			build := time.Since(t0).Seconds()
			logf("vector hnsw n=%d: build %.1fs", n, build)
			t0 = time.Now()
			db.Close()
			closeS := time.Since(t0).Seconds()
			logf("vector hnsw n=%d: close (saves the graph) %.1fs", n, closeS)
			runtime.GC()
			t0 = time.Now()
			if db, err = openFreshDB(dir, false, ""); err != nil {
				fatal(err)
			}
			open := time.Since(t0).Seconds()
			runtime.GC()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			logf("vector hnsw n=%d: reopen %.1fs heap %.0f MiB", n, open, float64(ms.HeapAlloc)/(1<<20))
			for _, ef := range parseInts(*annF) {
				search := func(q []float32) []string {
					var ids []string
					db.View(bg, func(tx *freshdb.Tx) error {
						hits, err := tx.Vector("v").SearchANN(q, 10, ef)
						for _, h := range hits {
							ids = append(ids, h.ID)
						}
						return err
					})
					return ids
				}
				rec := 0.0
				for i, q := range qs {
					rec += recall(search(q), truth[i])
				}
				rec /= float64(len(qs))
				for _, c := range parseInts(*clientsF) {
					var qi atomic.Int64
					ph := runClosed(c, *dur, func(int) opFunc {
						return func() error { search(qs[int(qi.Add(1))%len(qs)]); return nil }
					})
					sys := fmt.Sprintf("freshdb-hnsw-ef%d", ef)
					res.Runs = append(res.Runs, vecRun{System: sys, N: n, Dim: *dim, Clients: c, K: 10, RecallAt10: round(rec, 4), QPS: ph.OpsPerSec, Latency: ph.Latency,
						Index: fmt.Sprintf("HNSW M=%d efC=%d ef=%d", *annM, *annEfc, ef), BuildSecs: round(build, 2), OpenSecs: round(open, 2), CloseSecs: round(closeS, 2), HeapMiB: round(float64(ms.HeapAlloc)/(1<<20), 0)})
					logf("vector %s n=%d c=%d: recall@10 %.4f qps %.1f p50 %.0fus p99 %.0fus", sys, n, c, rec, ph.OpsPerSec, ph.Latency.P50Us, ph.Latency.P99Us)
				}
			}
			// catch-up: 1% more vectors than the saved graph, then a crash
			extra := max(1, n/100)
			for i := 0; i < extra; i += 1000 {
				err := db.Update(bg, func(tx *freshdb.Tx) error {
					for j := i; j < min(extra, i+1000); j++ {
						if err := tx.Vector("v").Put(fmt.Sprint("new", j), qs[j%len(qs)]); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					fatal(err)
				}
			}
			db.Crash()
			runtime.GC()
			t0 = time.Now()
			if db, err = openFreshDB(dir, false, ""); err != nil {
				fatal(err)
			}
			catch := time.Since(t0).Seconds()
			logf("vector hnsw n=%d: reopen after crash with %d newer vectors %.1fs", n, extra, catch)
			for i, x := range res.Runs {
				if r, ok := x.(vecRun); ok && r.N == n && r.OpenSecs > 0 {
					r.CatchSecs = round(catch, 2)
					res.Runs[i] = r
				}
			}
		}
		db.Close()
		wipe(dir)
		runtime.GC()
	}
	res.Params.(map[string]any)["peak_rss"] = peakRSS()
	writeResult(*out, "vector", res)
}

func normalize(v []float32) []float32 {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	n := float32(1 / math.Sqrt(s))
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * n
	}
	return out
}

func groundTruth(data, qs [][]float32, k int) [][]string {
	out := make([][]string, len(qs))
	var wg sync.WaitGroup
	for qi := range qs {
		wg.Add(1)
		go func(qi int) {
			defer wg.Done()
			q := qs[qi]
			type sc struct {
				i int
				s float64
			}
			all := make([]sc, len(data))
			var qn float64
			for _, x := range q {
				qn += float64(x) * float64(x)
			}
			for i, d := range data {
				var dot, dn float64
				for j := range d {
					dot += float64(d[j]) * float64(q[j])
					dn += float64(d[j]) * float64(d[j])
				}
				all[i] = sc{i, dot / math.Sqrt(dn*qn)}
			}
			sort.Slice(all, func(a, b int) bool { return all[a].s > all[b].s })
			for _, x := range all[:k] {
				out[qi] = append(out[qi], fmt.Sprintf("v%d", x.i))
			}
		}(qi)
	}
	wg.Wait()
	return out
}

func recall(got, want []string) float64 {
	w := map[string]bool{}
	for _, x := range want {
		w[x] = true
	}
	hit := 0
	for _, x := range got {
		if w[x] {
			hit++
		}
	}
	return float64(hit) / float64(len(want))
}

type flatIndex struct{ v [][]float32 }

func newFlat(data [][]float32) *flatIndex {
	f := &flatIndex{}
	for _, d := range data {
		f.v = append(f.v, normalize(d))
	}
	return f
}

type hitHeap []struct {
	i int
	s float32
}

func (h hitHeap) Len() int           { return len(h) }
func (h hitHeap) Less(a, b int) bool { return h[a].s < h[b].s }
func (h hitHeap) Swap(a, b int)      { h[a], h[b] = h[b], h[a] }
func (h *hitHeap) Push(x any) {
	*h = append(*h, x.(struct {
		i int
		s float32
	}))
}
func (h *hitHeap) Pop() any { o := *h; x := o[len(o)-1]; *h = o[:len(o)-1]; return x }

func (f *flatIndex) search(q []float32) []string {
	q = normalize(q)
	h := &hitHeap{}
	for i, d := range f.v {
		var s float32
		for j := range d {
			s += d[j] * q[j]
		}
		if h.Len() < 10 {
			heap.Push(h, struct {
				i int
				s float32
			}{i, s})
		} else if s > (*h)[0].s {
			(*h)[0] = struct {
				i int
				s float32
			}{i, s}
			heap.Fix(h, 0)
		}
	}
	out := make([]string, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = fmt.Sprintf("v%d", heap.Pop(h).(struct {
			i int
			s float32
		}).i)
	}
	return out
}
