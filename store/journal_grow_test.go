//go:build unix

package store

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// Background preallocation runs ahead of the writer: after crossing the
// threshold the file reaches the next chunk without a commit doing it.
func TestJournalGrowAhead(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	j := mustJournal(t, d)
	v := bytes.Repeat([]byte("g"), 64<<10)
	for i := 0; i < 70; i++ { // 4.4 MiB: past jrnGrowAt in the first 8 MiB chunk
		if err := j.PutIfAbsent(ctx, fmt.Sprint("k", i), v); err != nil {
			t.Fatal(err)
		}
	}
	j.growWG.Wait()
	fi, _ := os.Stat(filepath.Join(d, jrnFile))
	if fi.Size() != 2*jrnChunk {
		t.Fatalf("size %d, want %d", fi.Size(), 2*jrnChunk)
	}
	j.Close()
}

// Two instances (as two processes) write concurrently while both grow the
// file in the background; every record survives a reopen.
func TestJournalGrowConcurrent(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	js := []*Journal{mustJournal(t, d), mustJournal(t, d)}
	val := func(w, i int) []byte { return bytes.Repeat([]byte{byte(w), byte(i)}, 8<<10+i) }
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				if err := js[w%2].PutIfAbsent(ctx, fmt.Sprintf("w%d/%d", w, i), val(w, i)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	js[0].Close()
	js[1].Close()
	j := mustJournal(t, d)
	defer j.Close()
	for w := 0; w < 4; w++ {
		for i := 0; i < 300; i++ {
			got, err := j.Get(ctx, fmt.Sprintf("w%d/%d", w, i))
			if err != nil || !bytes.Equal(got, val(w, i)) {
				t.Fatalf("w%d/%d: %v", w, i, err)
			}
		}
	}
}
