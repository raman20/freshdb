package freshdb

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/raman20/freshdb/store"
)

var errRollback = errors.New("rollback")

// BenchmarkVectorSearchANN: top-10 over 20k x 128-dim vectors with an
// HNSW graph, from a read-only View, from a writable Update that wrote
// nothing, and from one that put a vector first (its own write must be
// in the result: the overlay path).
func BenchmarkVectorSearchANN(b *testing.B) {
	db := mustOpen(b, store.NewMem(), WithSnapshotEvery(1<<30))
	defer db.Close()
	if err := db.EnableANN(ctx, "v", ANNOptions{}); err != nil {
		b.Fatal(err)
	}
	r := rand.New(rand.NewSource(1))
	vec := func() []float32 {
		v := make([]float32, 128)
		for i := range v {
			v[i] = r.Float32() - 0.5
		}
		return v
	}
	for c := 0; c < 20; c++ {
		if err := db.Update(ctx, func(tx *Tx) error {
			for i := 0; i < 1000; i++ {
				if err := tx.Vector("v").Put(fmt.Sprintf("%d-%d", c, i), vec()); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	q, own := vec(), vec()
	search := func(tx *Tx) error { _, err := tx.Vector("v").Search(q, 10); return err }
	b.Run("view", func(b *testing.B) {
		for b.Loop() {
			if err := db.View(ctx, search); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("update", func(b *testing.B) {
		for b.Loop() {
			if err := db.Update(ctx, func(tx *Tx) error { search(tx); return errRollback }); err != errRollback {
				b.Fatal(err)
			}
		}
	})
	b.Run("update+put", func(b *testing.B) {
		for b.Loop() {
			if err := db.Update(ctx, func(tx *Tx) error {
				tx.Vector("v").Put("own", own)
				search(tx)
				return errRollback
			}); err != errRollback {
				b.Fatal(err)
			}
		}
	})
}
