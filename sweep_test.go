package freshdb

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/freshdb/store"
)

// hookStore calls onDelete before each delete of a log segment.
type hookStore struct {
	store.Store
	mu       sync.Mutex
	onDelete func(key string)
}

func (h *hookStore) Delete(ctx context.Context, key string) error {
	h.mu.Lock()
	f := h.onDelete
	h.mu.Unlock()
	if f != nil && strings.HasPrefix(key, "log/") {
		f(key)
	}
	return h.Store.Delete(ctx, key)
}

// The sweep does not pause writers: a commit made from inside a garbage
// delete (pre- and post-commit sweeps alike) goes through.
func TestSweepDoesNotPauseCommits(t *testing.T) {
	hs := &hookStore{Store: store.NewMem()}
	var skew atomic.Int64
	db := mustOpen(t, hs, WithClock(func() int64 { return time.Now().UnixNano() + skew.Load() }))
	defer db.Close()
	fill(t, db, 10)
	if _, err := db.Compact(ctx, CompactOptions{Grace: time.Hour}); err != nil {
		t.Fatal(err)
	}
	fill(t, db, 10)
	var commits []error
	hs.onDelete = func(string) {
		if len(commits) >= 2 {
			return
		}
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		commits = append(commits, db.Update(c, func(tx *Tx) error { return tx.Put("during-sweep", []byte("x")) }))
	}
	// The first run's garbage (an hour of grace) is due for the pre-commit
	// sweep; this run's own (no grace) for the post-commit one.
	skew.Store(int64(2 * time.Hour))
	cs, err := db.Compact(ctx, CompactOptions{})
	if err != nil || cs.Deleted == 0 || cs.Pending != 0 || len(db.Manifest().Garbage) != 0 {
		t.Fatalf("%+v %v garbage=%v", cs, err, db.Manifest().Garbage)
	}
	if len(commits) == 0 {
		t.Fatal("no delete went through the hook")
	}
	for i, err := range commits {
		if err != nil {
			t.Fatalf("commit %d during the sweep: %v", i, err)
		}
	}
}

// A crashed DB writes nothing more: a sweep stops deleting and the
// garbage stays listed for the next opener, which finishes it.
func TestSweepStopsAfterCrash(t *testing.T) {
	hs := &hookStore{Store: store.NewMem()}
	db := mustOpen(t, hs)
	fill(t, db, 20)
	var deletes int
	hs.onDelete = func(string) {
		deletes++
		if deletes == 1 {
			db.Crash()
		}
	}
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	if deletes != 1 {
		t.Fatalf("%d deletes; want the sweep to stop after the crash", deletes)
	}
	hs.onDelete = nil
	db2 := mustOpen(t, hs)
	defer db2.Close()
	if len(db2.Manifest().Garbage) == 0 {
		t.Fatal("garbage dropped from the manifest though not deleted")
	}
	cs, err := db2.Compact(ctx, CompactOptions{})
	if err != nil || cs.Pending != 0 || len(db2.Manifest().Garbage) != 0 {
		t.Fatalf("%+v %v", cs, err)
	}
	if n, _ := objects(t, hs.Store, "log/L1/00000000000000000001"); n != 0 {
		t.Fatal("first segment never swept")
	}
}

func TestWithoutGarbage(t *testing.T) {
	a, b := Garbage{Kind: "segs", Log: "L1", Below: 3}, Garbage{Kind: "base", Key: "k"}
	got := withoutGarbage([]Garbage{a, b, a}, []Garbage{a})
	if len(got) != 2 || got[0] != b || got[1] != a {
		t.Fatalf("%v", got)
	}
}

// A backup's hold waits for deletes already passed to the store, so the
// key listing it takes next never includes an object being deleted (a
// sweep runs without db.mu; the backup drill caught this as a missing
// segment).
func TestHoldWaitsForInflightDelete(t *testing.T) {
	hs := &hookStore{Store: store.NewMem()}
	db := mustOpen(t, hs)
	defer db.Close()
	fill(t, db, 10)
	h := db.st.(*holdStore)
	var once sync.Once
	held := make(chan struct{})
	early := false
	hs.onDelete = func(string) {
		once.Do(func() {
			go func() {
				release := h.hold()
				close(held)
				release(ctx, t.Logf)
			}()
			select {
			case <-held:
				early = true
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	<-held
	if early {
		t.Fatal("hold taken while a delete was in flight")
	}
}
