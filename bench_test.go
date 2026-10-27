package freshdb

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/raman20/freshdb/store"
)

func benchStores(b *testing.B) map[string]func() store.Store {
	return map[string]func() store.Store{
		"mem": func() store.Store { return store.NewMem() },
		"fs": func() store.Store {
			s, err := store.NewFS(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			return s
		},
	}
}

// One Put per transaction, one writer: each commit is one segment object (+fsync on fs).
func BenchmarkPutSerial(b *testing.B) {
	for name, mk := range benchStores(b) {
		b.Run(name, func(b *testing.B) {
			db := mustOpen(b, mk(), WithSnapshotEvery(1<<30))
			defer db.Close()
			v := make([]byte, 100)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Put(ctx, fmt.Sprintf("k%09d", i), v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Many concurrent writers: group commit batches them into one segment.
func BenchmarkPutParallel(b *testing.B) {
	for name, mk := range benchStores(b) {
		b.Run(name, func(b *testing.B) {
			db := mustOpen(b, mk(), WithSnapshotEvery(1<<30))
			defer db.Close()
			var n atomic.Int64
			v := make([]byte, 100)
			b.SetParallelism(16)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := db.Put(ctx, fmt.Sprintf("k%09d", n.Add(1)), v); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// Encrypted (per-subject AES-GCM) writes vs plain.
func BenchmarkPutParallelEncrypted(b *testing.B) {
	db := mustOpen(b, store.NewMem(), WithSnapshotEvery(1<<30))
	defer db.Close()
	var n atomic.Int64
	v := make([]byte, 100)
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := n.Add(1)
			err := db.Update(ctx, func(tx *Tx) error {
				return tx.Put(fmt.Sprintf("k%09d", i), v, Subject(fmt.Sprintf("user%d", i%100)))
			})
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkGet(b *testing.B) {
	db := mustOpen(b, store.NewMem(), WithSnapshotEvery(1<<30))
	defer db.Close()
	db.Update(ctx, func(tx *Tx) error {
		for i := 0; i < 10000; i++ {
			tx.Put(fmt.Sprintf("k%05d", i), make([]byte, 100))
		}
		return nil
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(ctx, fmt.Sprintf("k%05d", i%10000)); err != nil {
			b.Fatal(err)
		}
	}
}

// Brute-force cosine search over 10k x 128-dim vectors.
func BenchmarkVectorSearch10k(b *testing.B) {
	db := mustOpen(b, store.NewMem(), WithSnapshotEvery(1<<30))
	defer db.Close()
	r := rand.New(rand.NewSource(1))
	vec := func() []float32 {
		v := make([]float32, 128)
		for i := range v {
			v[i] = r.Float32() - 0.5
		}
		return v
	}
	for c := 0; c < 10; c++ {
		db.Update(ctx, func(tx *Tx) error {
			for i := 0; i < 1000; i++ {
				tx.Vector("v").Put(fmt.Sprintf("%d-%d", c, i), vec())
			}
			return nil
		})
	}
	q := vec()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.View(ctx, func(tx *Tx) error { _, err := tx.Vector("v").Search(q, 10); return err })
	}
}

// Cold open (recovery) of a 10k-commit log on fs, with and without a snapshot.
func BenchmarkOpenRecover10k(b *testing.B) {
	for _, snap := range []bool{false, true} {
		b.Run(fmt.Sprintf("snapshot=%v", snap), func(b *testing.B) {
			st, _ := store.NewFS(b.TempDir())
			every := -1 // never
			if snap {
				every = 50 // per 50 commits -> snapshot at commit 100 covers everything
			}
			db := mustOpen(b, st, WithSnapshotEvery(every))
			for c := 0; c < 100; c++ {
				// 100 commits x 100 puts: 10k keys, 100 segments
				db.Update(ctx, func(tx *Tx) error {
					for i := 0; i < 100; i++ {
						tx.Put(fmt.Sprintf("k%03d-%03d", c, i), make([]byte, 100))
					}
					return nil
				})
			}
			db.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d := mustOpen(b, st, WithSnapshotEvery(every))
				d.Close()
			}
		})
	}
}
