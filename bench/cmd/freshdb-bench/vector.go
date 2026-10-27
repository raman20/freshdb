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
	fs.Parse(args)
	res := &Result{Suite: "vector", Env: envInfo(*dataDir), Params: map[string]any{"dim": *dim, "clusters": *clusters, "sigma": *sigma,
		"queries": *nq, "duration": dur.String(), "seed": *seed, "metric": "cosine", "fsync": false,
		"dataset": "synthetic gaussian clusters (not SIFT/GloVe); ground truth by exact float64 search"}}
	res.Notes = append(res.Notes, "freshDB has no ANN index yet: search is exact brute force, so recall is 1.0 by construction and QPS falls linearly with N. The flat Go loop is a lower bound on brute-force cost, not an ANN competitor.")
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
		db, err := openFreshDB(dir, false)
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
		for _, sys := range []string{"freshdb", "flat-go-loop"} {
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
