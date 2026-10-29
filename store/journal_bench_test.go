//go:build unix

package store

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkJournalOpen opens a journal of 100k objects, 90% deleted
// (dead records, data punched), the shape a long-lived database leaves
// before an offline shrink. dead=64KiB makes the punched data holes.
func BenchmarkJournalOpen(b *testing.B) {
	for _, size := range []int{200, 64 << 10} {
		b.Run(fmt.Sprintf("dead=%d", size), func(b *testing.B) {
			ctx := context.Background()
			d := b.TempDir()
			j, err := NewJournal(d)
			if err != nil {
				b.Fatal(err)
			}
			v := make([]byte, size)
			for i := 0; i < 100_000; i++ {
				k := fmt.Sprintf("obj/%08d", i)
				if err := j.PutIfAbsent(ctx, k, v); err != nil {
					b.Fatal(err)
				}
				if i%10 != 0 {
					if err := j.Delete(ctx, k); err != nil {
						b.Fatal(err)
					}
				}
			}
			j.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j, err := NewJournal(d)
				if err != nil {
					b.Fatal(err)
				}
				j.Close()
			}
		})
	}
}
