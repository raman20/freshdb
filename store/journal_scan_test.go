//go:build unix

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

// Open scans the journal through a window (jrnReader): records that
// straddle a window edge, records larger than the window, and dead
// records whose data is skipped must all come back exactly.
func TestJournalScanWindow(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	j := mustJournal(t, d)
	rng := rand.New(rand.NewSource(1))
	want := map[string][]byte{}
	sizes := []int{0, 1, 100, 4095, 4096, jrnWindow - jrnHdr - 3, jrnWindow, jrnWindow + 1, 3*jrnWindow + 17}
	for i := 0; i < 300; i++ {
		n := sizes[i%len(sizes)]
		if i%7 == 0 {
			n = rng.Intn(64 << 10)
		}
		v := make([]byte, n)
		rng.Read(v)
		k := fmt.Sprintf("k%03d", i)
		if err := j.PutIfAbsent(ctx, k, v); err != nil {
			t.Fatal(err)
		}
		want[k] = v
		if i%3 == 1 { // a dead record (data punched) between live ones
			if err := j.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
			delete(want, k)
		}
	}
	j.Close()
	for round := 0; round < 2; round++ { // the second open reads its own result
		j = mustJournal(t, d)
		for i := 0; i < 300; i++ {
			k := fmt.Sprintf("k%03d", i)
			got, err := j.Get(ctx, k)
			if w, ok := want[k]; ok {
				if err != nil || !bytes.Equal(got, w) {
					t.Fatalf("round %d %s: %d bytes, %v; want %d bytes", round, k, len(got), err, len(w))
				}
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatalf("round %d deleted %s: %v", round, k, err)
			}
		}
		j.Close()
	}
}
