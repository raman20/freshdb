package freshdb

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
// BenchmarkOpenRecover10k opens a log of 100 segments (10k keys). With
// get=2ms every GET waits 2ms (an object store round trip); seg-gets/op
// counts segment reads per open.
func BenchmarkOpenRecover10k(b *testing.B) {
	for _, lat := range []time.Duration{0, 2 * time.Millisecond} {
		for _, snap := range []bool{false, true} {
			b.Run(fmt.Sprintf("get=%v/snapshot=%v", lat, snap), func(b *testing.B) {
				fs, _ := store.NewFS(b.TempDir())
				st := &slowGets{Store: fs}
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
				st.delay, st.segs = lat, 0
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					d := mustOpen(b, st, WithSnapshotEvery(every))
					d.Close()
				}
				b.ReportMetric(float64(st.segs)/float64(b.N), "seg-gets/op")
			})
		}
	}
}

// slowGets adds a fixed latency to every GET and counts segment GETs.
type slowGets struct {
	store.Store
	delay time.Duration
	mu    sync.Mutex
	segs  int
}

func (s *slowGets) Get(ctx context.Context, k string) ([]byte, error) {
	if strings.HasSuffix(k, ".seg") {
		s.mu.Lock()
		s.segs++
		s.mu.Unlock()
	}
	time.Sleep(s.delay)
	return s.Store.Get(ctx, k)
}

// slowPuts adds a fixed latency to every segment write (an object store's
// PUT round trip) and counts them.
type slowPuts struct {
	store.Store
	d    time.Duration
	segs atomic.Int64
}

func (s *slowPuts) PutIfAbsent(ctx context.Context, k string, b []byte) error {
	if strings.HasSuffix(k, ".seg") {
		s.segs.Add(1)
		time.Sleep(s.d)
	}
	return s.Store.PutIfAbsent(ctx, k, b)
}

// Group commit under a 2 ms segment PUT, by commit policy and number of
// concurrent writers: ns/op is per transaction (throughput), lat/txn the
// mean commit latency, puts/txn the segment PUTs per transaction.
func BenchmarkCommitPolicy(b *testing.B) {
	for _, writers := range []int{1, 8, 64} {
		for _, delay := range []time.Duration{0, time.Millisecond, 4 * time.Millisecond} {
			b.Run(fmt.Sprintf("writers=%d/delay=%v", writers, delay), func(b *testing.B) {
				st := &slowPuts{Store: store.NewMem(), d: 2 * time.Millisecond}
				db := mustOpen(b, st, WithSnapshotEvery(1<<30), WithCommitPolicy(CommitPolicy{MaxDelay: delay}))
				defer db.Close()
				before := st.segs.Load()
				var n, lat atomic.Int64
				v := make([]byte, 100)
				var wg sync.WaitGroup
				b.ResetTimer()
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := n.Add(1); i <= int64(b.N); i = n.Add(1) {
							t0 := time.Now()
							if err := db.Put(ctx, fmt.Sprintf("k%09d", i), v); err != nil {
								b.Error(err)
								return
							}
							lat.Add(int64(time.Since(t0)))
						}
					}()
				}
				wg.Wait()
				b.StopTimer()
				b.ReportMetric(float64(st.segs.Load()-before)/float64(b.N), "puts/txn")
				b.ReportMetric(float64(lat.Load())/float64(b.N)/1e6, "ms-lat/txn")
			})
		}
	}
}
