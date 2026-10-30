package lens

import (
	"math/rand"
	"testing"
)

// unreachable counts live nodes that no layer-0 path from the entry
// reaches: a search can never return them, whatever ef.
func unreachable(g *hnsw) int {
	if g.entry < 0 {
		return 0
	}
	seen := map[int32]bool{g.entry: true}
	stack := []int32{g.entry}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, x := range g.links[n][0] {
			if g.slot[x] >= 0 && !seen[x] {
				seen[x] = true
				stack = append(stack, x)
			}
		}
	}
	live := 0
	for _, s := range g.slot {
		if s >= 0 {
			live++
		}
	}
	return live - len(seen)
}

// Reachability of the graph under the data shapes that stress the
// neighbor heuristic. Logged for docs/components/vectors.md; asserted
// only where it holds (random data, no churn).
func TestHNSWReachability(t *testing.T) {
	type shape struct {
		name  string
		n, d  int
		gen   func(r *rand.Rand, i int) []float32
		churn bool
	}
	clustered := func(r *rand.Rand, i int) []float32 {
		c := rand.New(rand.NewSource(int64(i % 20)))
		v := randVec(c, 32)
		for j := range v {
			v[j] += 0.1 * float32(r.NormFloat64())
		}
		return v
	}
	shapes := []shape{
		{"random 32-d", 5000, 32, func(r *rand.Rand, i int) []float32 { return randVec(r, 32) }, false},
		{"random 32-d churn", 5000, 32, func(r *rand.Rand, i int) []float32 { return randVec(r, 32) }, true},
		{"clustered 32-d", 5000, 32, clustered, false},
		{"10% exact duplicates", 5000, 32, func(r *rand.Rand, i int) []float32 {
			if i%10 == 0 && i > 0 {
				return randVec(rand.New(rand.NewSource(int64(i-1))), 32)
			}
			return randVec(rand.New(rand.NewSource(int64(i))), 32)
		}, false},
		{"low-dim 2-d", 5000, 2, func(r *rand.Rand, i int) []float32 { return randVec(r, 2) }, false},
		{"35 distinct, M 4", 5000, 2, func(r *rand.Rand, i int) []float32 { return []float32{float32(i%7) + 1, float32(i % 5)} }, false},
		{"35 distinct", 5000, 2, func(r *rand.Rand, i int) []float32 { return []float32{float32(i%7) + 1, float32(i % 5)} }, false},
		{"35 distinct churn", 5000, 2, func(r *rand.Rand, i int) []float32 { return []float32{float32(r.Intn(7)) + 1, float32(r.Intn(5))} }, true},
	}
	for _, s := range shapes {
		r := rand.New(rand.NewSource(1))
		l := NewVector()
		p := ANNParams{}
		if s.name == "35 distinct, M 4" {
			p.M = 4
		}
		l.EnableANN("c", p)
		for i := 0; i < s.n; i++ {
			put(l, "c", itoa(i), "", s.gen(r, i))
		}
		if s.churn {
			for i := 0; i < 3*s.n; i++ {
				j := r.Intn(s.n)
				if r.Intn(2) == 0 {
					l.del("c", itoa(j))
				} else {
					put(l, "c", itoa(j), "", s.gen(r, j))
				}
			}
		}
		sl := l.m["c"].slabs[s.d]
		checkGraph(t, sl)
		u := unreachable(sl.g)
		var rc float64
		for q := 0; q < 50; q++ {
			v, _ := normalize(s.gen(r, r.Intn(s.n)))
			got, _ := l.ANN("c", v, 10, 0, 0)
			rc += recall(MergeHits([][]Hit{got}, 10), MergeHits([][]Hit{l.TopK("c", v, 10, 0, len(sl.ids))}, 10)) / 50
		}
		nodes := 0
		for _, x := range sl.g.slot {
			if x >= 0 {
				nodes++
			}
		}
		t.Logf("%-22s n=%d nodes=%d unreachable=%d recall@10=%.3f", s.name, len(sl.ids), nodes, u, rc)
		if u != 0 || rc < 0.95 {
			t.Errorf("%s: %d unreachable nodes, recall %.3f", s.name, u, rc)
		}
	}
}
