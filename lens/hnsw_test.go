package lens

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func randVec(r *rand.Rand, d int) []float32 {
	v := make([]float32, d)
	for i := range v {
		v[i] = float32(r.NormFloat64())
	}
	return v
}

func put(l *Vector, coll, id, subj string, v []float32) {
	l.del(coll, id)
	u, _ := normalize(v)
	l.add(coll, id, subj, u)
}

func recall(got, want []Hit) float64 {
	in := map[string]bool{}
	for _, h := range want {
		in[h.ID] = true
	}
	n := 0
	for _, h := range got {
		if in[h.ID] {
			n++
		}
	}
	return float64(n) / float64(len(want))
}

// checkGraph verifies the slot/node mapping and link invariants.
func checkGraph(t *testing.T, sl *slab) {
	t.Helper()
	l := &Vector{m: map[string]*vecColl{"c": {slabs: map[int]*slab{sl.dim: sl}}}}
	if err := l.CheckANN(); err != nil {
		t.Fatal(err)
	}
}

func TestHNSWRecall(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	l := NewVector()
	l.EnableANN("c", ANNParams{})
	const n, d = 10000, 32
	for i := 0; i < n; i++ {
		put(l, "c", fmt.Sprint("v", i), "", randVec(r, d))
	}
	checkGraph(t, l.m["c"].slabs[d])
	var sum float64
	const q = 200
	for i := 0; i < q; i++ {
		u, _ := normalize(randVec(r, d))
		got, ok := l.ANN("c", u, 10, 0, 0)
		if !ok {
			t.Fatal("no graph")
		}
		want := MergeHits([][]Hit{l.TopK("c", u, 10, 0, n)}, 10)
		for _, h := range got { // scores are the exact ones
			if s := dot(u, l.m["c"].slabs[d].data[l.m["c"].slabs[d].pos[h.ID]*d:][:d]); s != h.Score {
				t.Fatalf("%s: score %v, exact %v", h.ID, h.Score, s)
			}
		}
		sum += recall(MergeHits([][]Hit{got}, 10), want)
	}
	if rc := sum / q; rc < 0.9 {
		t.Fatalf("recall@10 %.3f", rc)
	} else {
		t.Logf("recall@10 %.3f (n=%d d=%d ef=64)", rc, n, d)
	}
}

// TestHNSWChurn: random puts, overwrites, deletes and shreds keep the
// graph consistent, never return a dead id, keep high recall with a wide
// beam, and the same history builds the same graph.
func TestHNSWChurn(t *testing.T) {
	for seed := int64(0); seed < 8; seed++ {
		r := rand.New(rand.NewSource(seed))
		a, b := NewVector(), NewVector()
		a.EnableANN("c", ANNParams{M: 4, EfConstruction: 16})
		model := map[string]bool{}
		var ops []func(*Vector)
		for i := 0; i < 3000; i++ {
			id := fmt.Sprintf("v%03d", r.Intn(400))
			switch x := r.Intn(10); {
			case x < 6:
				v, subj := randVec(r, 8), fmt.Sprint("s", r.Intn(20))
				ops = append(ops, func(l *Vector) { put(l, "c", id, subj, v) })
				model[id] = true
			case x < 9:
				ops = append(ops, func(l *Vector) { l.del("c", id) })
				delete(model, id)
			default:
				subj := fmt.Sprint("s", r.Intn(20))
				ops = append(ops, func(l *Vector) { l.DropSubject(subj) })
			}
			ops[len(ops)-1](a)
			if i%300 == 299 && a.m["c"] != nil {
				sl := a.m["c"].slabs[8]
				checkGraph(t, sl)
				for _, id := range sl.ids {
					if !model[id] {
						delete(model, id) // shredded
					}
				}
				for q := 0; q < 20; q++ {
					u, _ := normalize(randVec(r, 8))
					got, _ := a.ANN("c", u, 10, len(sl.ids), 0)
					for _, h := range got {
						if _, ok := sl.pos[h.ID]; !ok {
							t.Fatalf("seed %d: dead id %s returned", seed, h.ID)
						}
					}
					want := MergeHits([][]Hit{a.TopK("c", u, 10, 0, len(sl.ids))}, 10)
					if rc := recall(got, want); rc < 0.9 {
						t.Fatalf("seed %d op %d: recall %.2f with ef=n", seed, i, rc)
					}
				}
			}
		}
		b.EnableANN("c", ANNParams{M: 4, EfConstruction: 16})
		for _, op := range ops {
			op(b)
		}
		if a.Digest() != b.Digest() {
			t.Fatal("digest differs")
		}
		if a.m["c"] != nil && !reflect.DeepEqual(a.m["c"].slabs[8].g.links, b.m["c"].slabs[8].g.links) {
			t.Fatalf("seed %d: same history built different graphs", seed)
		}
	}
}

// Enabling ANN must not change the digest, and a shredded vector's bytes
// must not stay in the slab.
func TestHNSWDigestAndShred(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	a, b := NewVector(), NewVector()
	b.EnableANN("c", ANNParams{})
	for i := 0; i < 500; i++ {
		v := randVec(r, 4)
		put(a, "c", fmt.Sprint("v", i), fmt.Sprint("s", i%3), v)
		put(b, "c", fmt.Sprint("v", i), fmt.Sprint("s", i%3), v)
	}
	if a.Digest() != b.Digest() {
		t.Fatal("ANN changed the digest")
	}
	b.DropSubject("s1")
	sl := b.m["c"].slabs[4]
	for _, x := range sl.data[len(sl.data):cap(sl.data)] {
		if x != 0 {
			t.Fatal("deleted vector bytes left in the slab")
		}
	}
	checkGraph(t, sl)
	u, _ := normalize(randVec(r, 4))
	hs, _ := b.ANN("c", u, 50, 500, 0)
	for _, h := range hs {
		if _, ok := sl.pos[h.ID]; !ok {
			t.Fatalf("shredded %s returned", h.ID)
		}
	}
}
