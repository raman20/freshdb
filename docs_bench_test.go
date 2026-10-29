package freshdb

import (
	"fmt"
	"testing"

	"github.com/raman20/freshdb/store"
)

// docsDB returns an in-memory DB holding n documents in "c", field g with
// 100 distinct values, optionally indexed.
func docsDB(b *testing.B, n int, indexed bool) *DB {
	db := mustOpen(b, store.NewMem())
	if indexed {
		if err := db.RegisterIndex(ctx, "c", "g"); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < n; i += 1000 {
		if err := db.Update(ctx, func(tx *Tx) error {
			for j := i; j < i+1000 && j < n; j++ {
				if err := tx.Doc("c").Put(fmt.Sprintf("d%07d", j), map[string]any{"g": j % 100, "name": "some text", "n": 1.5}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	return db
}

// Find of 1% of 10k documents: read-only and inside a read-write tx.
func BenchmarkDocFind(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		db := docsDB(b, 10_000, indexed)
		b.Run(fmt.Sprintf("indexed=%v/view", indexed), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				db.View(ctx, func(tx *Tx) error { _, err := tx.Doc("c").Find("g", i%100); return err })
			}
		})
		b.Run(fmt.Sprintf("indexed=%v/update", indexed), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				db.Update(ctx, func(tx *Tx) error {
					tx.Doc("c").Put("new", map[string]any{"g": 1})
					_, err := tx.Doc("c").Find("g", i%100)
					return err
				})
			}
		})
		db.Close()
	}
}

// Put cost with and without an index (the lens apply decodes the JSON).
func BenchmarkDocPut(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprintf("indexed=%v", indexed), func(b *testing.B) {
			db := docsDB(b, 0, indexed)
			defer db.Close()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				db.Update(ctx, func(tx *Tx) error {
					return tx.Doc("c").Put(fmt.Sprintf("d%07d", i%10000), map[string]any{"g": i % 100, "name": "some text"})
				})
			}
		})
	}
}

// RegisterIndex on 100k existing documents: the replay holds the writers.
func BenchmarkDocRegisterIndex100k(b *testing.B) {
	db := docsDB(b, 100_000, false)
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.RegisterIndex(ctx, "c", fmt.Sprintf("f%d", i)); err != nil {
			b.Fatal(err)
		}
	}
}
