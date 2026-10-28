package freshdb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

func annSearch(t *testing.T, db *DB, q []float32, k int, exact bool) []lens.Hit {
	t.Helper()
	var hs []lens.Hit
	err := db.View(ctx, func(tx *Tx) error {
		var err error
		if exact {
			hs, err = tx.Vector("c").SearchExact(q, k)
		} else {
			hs, err = tx.Vector("c").Search(q, k)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return hs
}

func hasGraph(db *DB) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, v := range db.views {
		if _, ok := v.vec.ANN("c", []float32{1, 0, 0, 0, 0, 0, 0, 0}, 1, 0, 0); ok {
			return true
		}
	}
	return false
}

func TestANNSearch(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	r := rand.New(rand.NewSource(7))
	rv := func() []float32 {
		v := make([]float32, 8)
		for i := range v {
			v[i] = float32(r.NormFloat64())
		}
		return v
	}
	put := func(from, to int, subj string) {
		err := db.Update(ctx, func(tx *Tx) error {
			for i := from; i < to; i++ {
				var o []WriteOpt
				if subj != "" && i%10 == 0 {
					o = append(o, Subject(subj))
				}
				if err := tx.Vector("c").Put(fmt.Sprint("v", i), rv(), o...); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	put(0, 3000, "")
	if err := db.EnableANN(ctx, "c", ANNOptions{}); err != nil {
		t.Fatal(err)
	}
	q := rv()
	// below ANNExactBelow: exact
	if a, b := annSearch(t, db, q, 10, false), annSearch(t, db, q, 10, true); fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatal("small collection not exact")
	}
	for i := 3000; i < 6000; i += 500 {
		put(i, i+500, "alice")
	}
	h, _ := db.Branch("main").Head()
	check := func(when string) {
		t.Helper()
		if !hasGraph(db) {
			t.Fatalf("%s: no graph", when)
		}
		var sum float64
		for i := 0; i < 50; i++ {
			q := rv()
			got, want := annSearch(t, db, q, 10, false), annSearch(t, db, q, 10, true)
			if len(got) != 10 {
				t.Fatalf("%s: %d hits", when, len(got))
			}
			in := map[string]bool{}
			for _, w := range want {
				in[w.ID] = true
			}
			for _, g := range got {
				if in[g.ID] {
					sum++
				}
			}
		}
		if rc := sum / 500; rc < 0.9 {
			t.Fatalf("%s: recall %.3f", when, rc)
		}
	}
	check("enabled")
	db.Close()
	db = mustOpen(t, st)
	check("reopened")
	if _, err := db.Shred(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		for _, hit := range annSearch(t, db, rv(), 50, false) {
			var n int
			fmt.Sscanf(hit.ID, "v%d", &n)
			if n >= 3000 && n%10 == 0 {
				t.Fatalf("shredded %s returned", hit.ID)
			}
		}
	}
	check("shredded")
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	check("compacted")
	put(6000, 6001, "")
	// time travel at an older LSN: exact scan, still correct
	err := db.Branch("main").ViewAt(ctx, h+1, func(tx *Tx) error {
		hs, err := tx.Vector("c").Search(q, 3)
		ex, _ := tx.Vector("c").SearchExact(q, 3)
		if err != nil || fmt.Sprint(hs) != fmt.Sprint(ex) {
			t.Fatalf("time travel: %v %v %v", hs, ex, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasGraph(db) {
		t.Fatal("no graph after compaction")
	}
	db.Close()
}
