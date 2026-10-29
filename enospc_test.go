package freshdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/raman20/freshdb/store"
)

// fullStore fails every write with ENOSPC while full is set, either before
// the write (nothing stored) or after it (stored, then the sync fails).
type fullStore struct {
	store.Store
	full  atomic.Bool
	after bool
}

func (f *fullStore) PutIfAbsent(c context.Context, key string, data []byte) error {
	if !f.full.Load() {
		return f.Store.PutIfAbsent(c, key, data)
	}
	if f.after {
		f.Store.PutIfAbsent(c, key, data)
	}
	return &os.PathError{Op: "write", Path: key, Err: syscall.ENOSPC}
}

// TestDiskFull: a commit that hits ENOSPC fails with ErrUnknownOutcome
// wrapping ENOSPC, the writer halts (later commits fail fast, reads keep
// working), compaction fails cleanly, and after space is freed a reopen
// holds every acknowledged commit, the failed one either way, and accepts
// writes again; Verify is clean.
func TestDiskFull(t *testing.T) {
	for name, mk := range backends(t) {
		if name == "mem" {
			continue
		}
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/after=%v", name, after), func(t *testing.T) {
				inner := mk()
				fs := &fullStore{Store: inner, after: after}
				db := mustOpen(t, fs)
				for i := 0; i < 50; i++ {
					if err := db.Put(ctx, fmt.Sprint("k", i), []byte(fmt.Sprint("v", i))); err != nil {
						t.Fatal(err)
					}
				}
				fs.full.Store(true)
				err := db.Put(ctx, "failed", []byte("x"))
				if !errors.Is(err, syscall.ENOSPC) || !errors.Is(err, ErrUnknownOutcome) {
					t.Fatalf("commit on a full disk: %v (want ErrUnknownOutcome wrapping ENOSPC)", err)
				}
				t.Logf("error: %v", err)
				// compaction must not revive the halted writer
				if _, err := db.Compact(ctx, CompactOptions{}); !errors.Is(err, ErrUnknownOutcome) {
					t.Fatalf("compact with a halted writer: %v", err)
				}
				fs.full.Store(false) // space freed: the writer stays halted
				if err := db.Put(ctx, "later", []byte("y")); !errors.Is(err, ErrUnknownOutcome) {
					t.Fatalf("commit after a halt: %v", err)
				}
				if v, err := db.Get(ctx, "k7"); err != nil || string(v) != "v7" {
					t.Fatalf("read after halt: %q %v", v, err)
				}
				// monitoring sees it: not ready, halted in Stats, errors counted
				if err := db.Ping(ctx); !errors.Is(err, ErrUnknownOutcome) || !errors.Is(err, syscall.ENOSPC) {
					t.Fatalf("Ping after a halt: %v", err)
				}
				st := db.Stats()
				if st.Halted == "" || st.Commits != 50 || st.CommitErrors != 2 || st.CompactionErrors != 1 {
					t.Fatalf("stats after a halt: halted %q commits %d errors %d compaction errors %d",
						st.Halted, st.Commits, st.CommitErrors, st.CompactionErrors)
				}
				db.Close()
				if err := db.Healthy(); !errors.Is(err, ErrClosed) {
					t.Fatalf("Healthy after Close: %v", err)
				}
				db = mustOpen(t, inner)
				defer db.Close()
				for i := 0; i < 50; i++ {
					if v, err := db.Get(ctx, fmt.Sprint("k", i)); err != nil || string(v) != fmt.Sprint("v", i) {
						t.Fatalf("k%d after reopen: %q %v", i, v, err)
					}
				}
				v, err := db.Get(ctx, "failed")
				if err == nil && string(v) != "x" || err != nil && !errors.Is(err, ErrNotFound) {
					t.Fatalf("failed commit: %q %v", v, err)
				}
				if _, err := db.Get(ctx, "later"); !errors.Is(err, ErrNotFound) {
					t.Fatalf("commit refused by the halted writer is visible: %v", err)
				}
				if err := db.Put(ctx, "again", []byte("z")); err != nil {
					t.Fatalf("write after reopen: %v", err)
				}
				rep, err := db.Verify(ctx)
				if err != nil || len(rep.Problems) > 0 {
					t.Fatalf("verify: %v %v", err, rep)
				}
			})
		}
	}
}
