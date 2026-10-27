package freshdb

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// TestVectorParallelSearchEqualsSequential checks that the multi-core
// fan-out returns exactly the sequential lens result, including ties.
func TestVectorParallelSearchEqualsSequential(t *testing.T) {
	ctx := context.Background()
	db := mustOpen(t, store.NewMem())
	r := rand.New(rand.NewSource(7))
	n := 5*vecChunk + 123
	for i := 0; i < n; i += 4096 {
		err := db.Update(ctx, func(tx *Tx) error {
			for j := i; j < min(i+4096, n); j++ {
				v := []float32{float32(r.Intn(4)), float32(r.Intn(4)), 1}
				if err := tx.Vector("m").Put(fmt.Sprintf("v%06d", j), v); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var vw *view
	db.View(ctx, func(tx *Tx) error {
		var err error
		vw, err = tx.lensView()
		return err
	})
	if vw == nil {
		t.Fatal("no view")
	}
	for _, q := range [][]float32{{1, 0, 0}, {0, 1, .5}, {3, 3, 1}} {
		u, _ := lens.Normalize(q)
		vw.mu.RLock()
		want := lens.MergeHits([][]lens.Hit{vw.vec.TopK("m", u, 25, 0, n)}, 25)
		got := searchParallel(vw.vec, "m", u, 25)
		vw.mu.RUnlock()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("q=%v:\n got %v\nwant %v", q, got, want)
		}
		db.View(ctx, func(tx *Tx) error {
			h, err := tx.Vector("m").Search(q, 25)
			if err != nil || !reflect.DeepEqual(h, want) {
				t.Fatalf("Search q=%v: %v %v", q, h, err)
			}
			return nil
		})
	}
}
