package freshdb

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/raman20/freshdb/store"
)

// BenchmarkTopicPublish: one 256 B message per transaction, mem store.
func BenchmarkTopicPublish(b *testing.B) {
	db := mustOpen(b, store.NewMem(), WithSnapshotEvery(1<<30))
	defer db.Close()
	v := make([]byte, 256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Update(ctx, func(tx *Tx) error { return tx.Topic("t").Publish(v) }); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTopicConsume100: fetch 100 messages and ack them in one
// transaction (exactly-once consume), mem store.
func BenchmarkTopicConsume100(b *testing.B) {
	db := mustOpen(b, store.NewMem(), WithSnapshotEvery(1<<30))
	defer db.Close()
	v := make([]byte, 256)
	publishN(b, db, "t", b.N*100+100, v)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Update(ctx, func(tx *Tx) error {
			g := tx.Topic("t").Group("g")
			ms, err := g.Fetch(100)
			if err != nil || len(ms) != 100 {
				return fmt.Errorf("fetch: %d %v", len(ms), err)
			}
			return g.Ack(ms[len(ms)-1])
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func publishN(b *testing.B, db *DB, topic string, n int, v []byte) {
	for i := 0; i < n; i += 1000 {
		err := db.Update(ctx, func(tx *Tx) error {
			for j := i; j < i+1000 && j < n; j++ {
				if err := tx.Topic(topic).Publish(v); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTopicOpen200k: reopen a DB holding 200k messages of 256 B
// (replay, no snapshot); reports live heap per message after open.
func BenchmarkTopicOpen200k(b *testing.B) {
	st := store.NewMem()
	db := mustOpen(b, st, WithSnapshotEvery(-1))
	publishN(b, db, "t", 200000, make([]byte, 256))
	db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		d := mustOpen(b, st, WithSnapshotEvery(-1))
		runtime.GC()
		runtime.ReadMemStats(&m1)
		b.ReportMetric(float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc))/200000, "heapB/msg")
		d.Close()
	}
}

// BenchmarkTopicRetainOpen: 200k messages of 256 B, compacted with
// Topics.MaxMessages = 1000, then reopened. Reports the compaction time
// and the live heap after the reopen per message published, against
// BenchmarkTopicOpen200k (no retention).
func BenchmarkTopicRetainOpen(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		st := store.NewMem()
		db := mustOpen(b, st, WithSnapshotEvery(-1))
		publishN(b, db, "t", 200000, make([]byte, 256))
		b.StartTimer()
		s, err := db.Compact(ctx, CompactOptions{Topics: TopicRetention{MaxMessages: 1000}})
		b.StopTimer()
		if err != nil || s.Trimmed != 199000 {
			b.Fatalf("compact: %+v %v", s, err)
		}
		db.Close()
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		d := mustOpen(b, st, WithSnapshotEvery(-1))
		runtime.GC()
		runtime.ReadMemStats(&m1)
		b.ReportMetric(float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc))/200000, "heapB/msg")
		d.Close()
		b.StartTimer()
	}
}
