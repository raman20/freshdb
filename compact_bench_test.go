package freshdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/freshdb/store"
)

// BenchmarkCompactSweepPause: a journal DB with 2000 one-commit segments
// is compacted with no grace, so the run deletes about 2000 objects. A
// committer runs throughout; pause-ms is the compaction's commit pause
// (Stats.CompactionPause), max-commit-ms the slowest commit seen.
func BenchmarkCompactSweepPause(b *testing.B) {
	const segs = 2000
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		st, err := store.NewJournal(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		db := mustOpen(b, st, WithSnapshotEvery(1<<30))
		v := make([]byte, 100)
		for j := 0; j < segs; j++ {
			if err := db.Put(ctx, fmt.Sprintf("k%06d", j), v); err != nil {
				b.Fatal(err)
			}
		}
		var stop atomic.Bool
		var worst atomic.Int64
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; !stop.Load(); n++ {
				t0 := time.Now()
				if err := db.Put(ctx, fmt.Sprintf("w%06d", n%1000), v); err != nil {
					b.Error(err)
					return
				}
				if d := int64(time.Since(t0)); d > worst.Load() {
					worst.Store(d)
				}
			}
		}()
		time.Sleep(20 * time.Millisecond)
		b.StartTimer()
		cs, err := db.Compact(ctx, CompactOptions{})
		b.StopTimer()
		time.Sleep(20 * time.Millisecond)
		stop.Store(true)
		wg.Wait()
		if err != nil || cs.Deleted < segs {
			b.Fatalf("compact: %+v %v", cs, err)
		}
		p := db.Stats().CompactionPause
		b.ReportMetric(p.Sum*1e3/float64(p.Count), "pause-ms")
		b.ReportMetric(float64(worst.Load())/1e6, "max-commit-ms")
		b.ReportMetric(float64(cs.Deleted), "deleted")
		db.Close()
		st.Close()
	}
}
