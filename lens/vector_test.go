package lens

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// naiveSearch is the obvious reference: score everything, sort, cut.
func naiveSearch(m map[string][]float32, q []float32, k int) []Hit {
	u, _ := normalize(q)
	var h []Hit
	for id, v := range m {
		if len(v) == len(u) {
			h = append(h, Hit{id, dot(u, v)})
		}
	}
	SortHits(h)
	if len(h) > k {
		h = h[:k]
	}
	return h
}

// TestVectorSlabMatchesNaive checks the slab layout, swap-remove deletes,
// mixed dimensions and top-k selection against a naive model, and that
// the digest and results do not depend on insertion order.
func TestVectorSlabMatchesNaive(t *testing.T) {
	for seed := int64(0); seed < 20; seed++ {
		r := rand.New(rand.NewSource(seed))
		l, model := NewVector(), map[string][]float32{}
		for i := 0; i < 600; i++ {
			id := fmt.Sprintf("v%03d", r.Intn(200))
			if r.Intn(4) == 0 {
				l.del("c", id)
				delete(model, id)
				continue
			}
			dim := 3 + r.Intn(2)*5 // 3 or 8: two slabs
			v := make([]float32, dim)
			for j := range v {
				v[j] = float32(r.Intn(5) - 2) // small ints force score ties
			}
			u, ok := normalize(v)
			if !ok {
				continue
			}
			l.del("c", id)
			l.add("c", id, "s"+id[3:], u)
			model[id] = u
		}
		if l.Count("c") != len(model) {
			t.Fatalf("seed %d: count %d, model %d", seed, l.Count("c"), len(model))
		}
		for _, k := range []int{1, 5, 1000} {
			for _, dim := range []int{3, 8} {
				q := make([]float32, dim)
				for j := range q {
					q[j] = float32(r.Intn(5) - 2)
				}
				q[0] = 1
				got, want := l.Search("c", q, k), naiveSearch(model, q, k)
				if len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
					t.Fatalf("seed %d k=%d dim=%d:\n got %v\nwant %v", seed, k, dim, got, want)
				}
			}
		}
		// Same final contents inserted in a different order: same digest.
		l2 := NewVector()
		ids := sortedKeys(model)
		r.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		for _, id := range ids {
			l2.add("c", id, "s"+id[3:], model[id])
		}
		if l.Digest() != l2.Digest() {
			t.Fatalf("seed %d: digest depends on history", seed)
		}
		// Dropping a subject removes exactly its vectors.
		for _, id := range ids {
			l.DropSubject("s" + id[3:])
		}
		if l.Count("c") != 0 || len(l.m) != 0 {
			t.Fatalf("seed %d: DropSubject left %d", seed, l.Count("c"))
		}
	}
}

// TestVectorTopKPartsMerge checks that any split of the slot range merges
// to the full result (the basis of the parallel search).
func TestVectorTopKPartsMerge(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	l := NewVector()
	for i := 0; i < 5000; i++ {
		v := []float32{r.Float32() - .5, r.Float32() - .5, float32(r.Intn(3)), 1}
		u, _ := normalize(v)
		l.add("c", fmt.Sprint(i), "", u)
	}
	u, _ := normalize([]float32{.3, -.2, 1, .5})
	want := l.Search("c", u, 10)
	n := l.Slots("c", 4)
	for p := 1; p <= 7; p++ {
		var parts [][]Hit
		for i := 0; i < p; i++ {
			parts = append(parts, l.TopK("c", u, 10, n*i/p, n*(i+1)/p))
		}
		if got := MergeHits(parts, 10); !reflect.DeepEqual(got, want) {
			t.Fatalf("p=%d: %v != %v", p, got, want)
		}
	}
}

func BenchmarkVectorSearch100k(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	l := NewVector()
	for i := 0; i < 100000; i++ {
		v := make([]float32, 128)
		for j := range v {
			v[j] = r.Float32() - .5
		}
		u, _ := normalize(v)
		l.add("c", fmt.Sprint(i), "", u)
	}
	q := make([]float32, 128)
	for j := range q {
		q[j] = r.Float32() - .5
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Search("c", q, 10)
	}
}
