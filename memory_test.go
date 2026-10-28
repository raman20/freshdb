package freshdb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/store"
)

// Branches at every LSN, including the last LSN of segments that are not
// the tip (whose chain head now comes from the next segment's first Prev),
// must reopen with an unbroken hash chain.
func TestBranchAtSegmentBoundaries(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	for i := 0; i < 4; i++ {
		db.Put(ctx, fmt.Sprint("s", i), []byte("x"))
	}
	var wg sync.WaitGroup // concurrent commits: some segments hold several frames
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); db.Put(ctx, fmt.Sprint("c", i), []byte("y")) }(i)
	}
	wg.Wait()
	db.Put(ctx, "last", []byte("z"))
	head, _ := db.Branch("main").Head()
	first := head - 36
	// Reference: fold the hash chain over every frame of every segment.
	if len(db.logs) != 1 {
		t.Fatalf("%d logs", len(db.logs))
	}
	for _, l := range db.logs {
		multi, ch := false, l.base
		for _, s := range l.segs {
			frames, err := l.readSeg(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			multi = multi || len(frames) > 1
			for _, d := range frames {
				ch = format.ChainNext(ch, d.Payload)
				got, err := l.chainAt(ctx, d.Frame.LSN)
				if err != nil || got != ch {
					t.Fatalf("chainAt(%d) = %x, %v; want %x", d.Frame.LSN, got[:4], err, ch[:4])
				}
			}
		}
		if !multi {
			t.Log("no segment held more than one frame")
		}
	}
	for lsn := first; lsn <= head; lsn++ {
		b := fmt.Sprint("b", lsn)
		if err := db.CreateBranch(ctx, b, "main", lsn); err != nil {
			t.Fatalf("branch at %d: %v", lsn, err)
		}
		if err := db.Branch(b).Update(ctx, func(tx *Tx) error { return tx.Put("on", []byte(b)) }); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	db = mustOpen(t, st) // replay checks every branch's chain
	defer db.Close()
	for lsn := first; lsn <= head; lsn++ {
		b := fmt.Sprint("b", lsn)
		db.Branch(b).View(ctx, func(tx *Tx) error {
			if v, _ := tx.Get("on"); string(v) != b {
				t.Fatalf("%s: got %q", b, v)
			}
			return nil
		})
	}
	if r, err := db.Verify(ctx); err != nil || !r.OK {
		t.Fatalf("verify: %v %+v", err, r)
	}
}

func TestCacheForMem(t *testing.T) {
	for _, c := range []struct {
		mem  int64
		want int
	}{{0, 256 << 20}, {-1, 256 << 20}, {128 << 20, 16 << 20}, {1 << 30, 64 << 20}, {2 << 30, 128 << 20}, {16 << 30, 256 << 20}} {
		if got := cacheForMem(c.mem); got != c.want {
			t.Errorf("cacheForMem(%d) = %d, want %d", c.mem, got, c.want)
		}
	}
	if n := defaultValueCacheBytes(); n < 16<<20 || n > 256<<20 {
		t.Fatalf("default %d out of range", n)
	}
}
