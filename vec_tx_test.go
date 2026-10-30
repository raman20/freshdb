package freshdb

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"

	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
	"pgregory.net/rapid"
)

// modelTopK is the contract: cosine similarity on vectors of the query's
// dimension, best first, ties by id.
func modelTopK(m map[string][]float32, q []float32, k int) []lens.Hit {
	u, ok := lens.Normalize(q)
	if !ok || k <= 0 {
		return nil
	}
	var hs []lens.Hit
	for id, v := range m {
		if w, _ := lens.Normalize(v); len(w) == len(u) {
			hs = append(hs, lens.Hit{ID: id, Score: lens.Dot(u, w)})
		}
	}
	return lens.MergeHits([][]lens.Hit{hs}, k)
}

// Property: over random puts (mixed dimensions, ties), deletes and
// shreds, Search inside a read-write transaction (own writes included),
// Search after the commit, and an ANN-enabled twin all equal the model.
func TestVectorSearchProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		plain := mustOpen(t, store.NewMem())
		defer plain.Close()
		ann := mustOpen(t, store.NewMem())
		defer ann.Close()
		if err := ann.EnableANN(ctx, "c", ANNOptions{M: 2}); err != nil {
			rt.Fatal(err)
		}
		model := map[string][]float32{}
		subjOf := map[string]string{}
		ids := []string{"a", "b", "c", "d", "e", "f"}
		subjects := []string{"", "s1", "s2"}
		comp := rapid.SampledFrom([]float32{-2, -1, 0, 1, 3})
		vec := func(name string) []float32 {
			v := make([]float32, rapid.IntRange(2, 3).Draw(rt, name+"dim"))
			for i := range v {
				v[i] = comp.Draw(rt, name)
			}
			if _, ok := lens.Normalize(v); !ok {
				v[0] = 1
			}
			return v
		}
		for step := rapid.IntRange(1, 20).Draw(rt, "steps"); step > 0; step-- {
			if rapid.IntRange(0, 9).Draw(rt, "kind") == 9 {
				i := rapid.IntRange(1, len(subjects)-1).Draw(rt, "shred")
				s := subjects[i]
				subjects[i] = s + "'"
				for _, db := range []*DB{plain, ann} {
					if _, err := db.Shred(ctx, s); err != nil {
						rt.Fatal(err)
					}
				}
				for id, sj := range subjOf {
					if sj == s {
						delete(model, id)
						delete(subjOf, id)
					}
				}
				continue
			}
			type op struct {
				id, subj string
				v        []float32
			}
			var ops []op
			for n := rapid.IntRange(1, 4).Draw(rt, "nops"); n > 0; n-- {
				o := op{id: rapid.SampledFrom(ids).Draw(rt, "id"), subj: rapid.SampledFrom(subjects).Draw(rt, "subj")}
				if rapid.IntRange(0, 3).Draw(rt, "del") > 0 {
					o.v = vec("v")
				}
				ops = append(ops, o)
			}
			q, k := vec("q"), rapid.IntRange(0, 7).Draw(rt, "k")
			after := map[string][]float32{}
			for id, v := range model {
				after[id] = v
			}
			for _, o := range ops {
				if o.v == nil {
					delete(after, o.id)
				} else {
					after[o.id] = o.v
				}
			}
			want := fmt.Sprint(modelTopK(after, q, k))
			for _, db := range []*DB{plain, ann} {
				err := db.Update(ctx, func(tx *Tx) error {
					for _, o := range ops {
						var err error
						switch {
						case o.v == nil:
							err = tx.Vector("c").Delete(o.id)
						case o.subj != "":
							err = tx.Vector("c").Put(o.id, o.v, Subject(o.subj))
						default:
							err = tx.Vector("c").Put(o.id, o.v)
						}
						if err != nil {
							return err
						}
					}
					hs, err := tx.Vector("c").Search(q, k)
					if got := fmt.Sprint(hs); err != nil || got != want {
						rt.Fatalf("in tx: %s %v, want %s", got, err, want)
					}
					return nil
				})
				if err != nil {
					rt.Fatal(err)
				}
				if err := db.View(ctx, func(tx *Tx) error {
					hs, err := tx.Vector("c").Search(q, k)
					if got := fmt.Sprint(hs); err != nil || got != want {
						rt.Fatalf("after commit: %s %v, want %s", got, err, want)
					}
					return nil
				}); err != nil {
					rt.Fatal(err)
				}
			}
			for _, o := range ops {
				if o.v == nil {
					delete(subjOf, o.id)
				} else {
					subjOf[o.id] = o.subj
				}
			}
			model = after
			if err := ann.CheckANN("main"); err != nil {
				rt.Fatal(err)
			}
		}
	})
}

// A Search in a read-write transaction reads the whole collection: a
// write to it committed meanwhile makes the transaction retry.
func TestVectorSearchConflicts(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	put := func(id string, v []float32) {
		if err := db.Update(ctx, func(tx *Tx) error { return tx.Vector("c").Put(id, v) }); err != nil {
			t.Fatal(err)
		}
	}
	put("a", []float32{1, 0})
	var runs atomic.Int32
	err := db.Update(ctx, func(tx *Tx) error {
		hs, err := tx.Vector("c").Search([]float32{0, 1}, 1)
		if err != nil {
			return err
		}
		if runs.Add(1) == 1 {
			put("b", []float32{0, 1}) // a better match appears concurrently
		}
		return tx.Put("best", []byte(hs[0].ID))
	})
	if err != nil || runs.Load() != 2 {
		t.Fatalf("runs %d, %v", runs.Load(), err)
	}
	if v, err := db.Get(ctx, "best"); err != nil || string(v) != "b" {
		t.Fatalf("best = %q %v", v, err)
	}
}

func TestVectorBadInput(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	nan, inf := float32(math.NaN()), float32(math.Inf(1))
	for _, v := range [][]float32{nil, {}, {0, 0}, {nan}, {1, inf}} {
		err := db.Update(ctx, func(tx *Tx) error { return tx.Vector("c").Put("x", v) })
		if !errors.Is(err, ErrBadValue) {
			t.Errorf("Put %v: %v", v, err)
		}
	}
	for _, o := range []ANNOptions{{M: -1}, {M: MaxM + 1}, {EfConstruction: MaxEf + 1}, {EfSearch: -1}} {
		if err := db.EnableANN(ctx, "c", o); !errors.Is(err, ErrBadValue) {
			t.Errorf("EnableANN %+v: %v", o, err)
		}
	}
	err := db.View(ctx, func(tx *Tx) error {
		for _, q := range [][]float32{{nan, 1}, {inf}} {
			if _, err := tx.Vector("c").Search(q, 1); !errors.Is(err, ErrBadValue) {
				t.Errorf("Search %v: %v", q, err)
			}
		}
		for _, ef := range []int{-1, MaxEf + 1} {
			if _, err := tx.Vector("c").SearchANN([]float32{1}, 1, ef); !errors.Is(err, ErrBadValue) {
				t.Errorf("ef %d: %v", ef, err)
			}
		}
		if hs, err := tx.Vector("c").Search([]float32{0, 0}, 1); hs != nil || err != nil {
			t.Errorf("zero query: %v %v", hs, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// k is capped by the candidates: a huge k (from a request) on an HNSW
// collection returns every vector instead of allocating for k.
func TestVectorHugeK(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	if err := db.EnableANN(ctx, "c", ANNOptions{M: 4, EfConstruction: 16}); err != nil {
		t.Fatal(err)
	}
	const n = ANNExactBelow + 10
	err := db.Update(ctx, func(tx *Tx) error {
		for i := 0; i < n; i++ {
			if err := tx.Vector("c").Put(fmt.Sprint(i), []float32{float32(i%7) + 1, float32(i % 5), float32(i % 3)}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, exact := range []bool{false, true} {
		hs := annSearch(t, db, []float32{1, 2, 3}, 1<<40, exact)
		if len(hs) != n {
			t.Fatalf("exact=%v: %d hits, want %d", exact, len(hs), n)
		}
	}
	// k + own writes must not overflow
	db.Update(ctx, func(tx *Tx) error {
		tx.Vector("c").Put("own", []float32{1, 2, 3})
		hs, err := tx.Vector("c").Search([]float32{1, 2, 3}, math.MaxInt)
		if err != nil || len(hs) != n+1 || hs[0].ID != "own" {
			t.Fatalf("in-tx k=MaxInt: %d hits, err %v", len(hs), err)
		}
		return nil
	})
}
