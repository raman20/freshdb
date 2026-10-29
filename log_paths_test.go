package freshdb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/store"
)

// logFixture: L1 with segments from two epochs (open, 4 puts, reopen,
// 3 concurrent puts batched into one segment), no snapshots. Returns the store, the second DB (closed) and
// the segment keys in order.
func logFixture(t *testing.T) (*store.Mem, *DB, []string) {
	t.Helper()
	st := store.NewMem()
	for round := 0; round < 2; round++ {
		db := mustOpen(t, st, WithReadAhead(1), WithCommitPolicy(CommitPolicy{MaxDelay: time.Duration(round) * 200 * time.Millisecond}))
		var wg sync.WaitGroup
		for i := 0; i < 4-round; i++ {
			put := func() {
				if err := db.Put(ctx, fmt.Sprintf("k%d-%d", round, i), []byte("v")); err != nil {
					t.Error(err)
				}
			}
			if round == 0 {
				put()
				continue
			}
			wg.Add(1)
			go func() { defer wg.Done(); put() }()
		}
		wg.Wait()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if round == 1 {
			var segs []string
			st.Raw(func(o map[string][]byte) {
				for k := range o {
					if strings.HasPrefix(k, "log/L1/") && strings.HasSuffix(k, ".seg") {
						segs = append(segs, k)
					}
				}
			})
			sort.Strings(segs)
			return st, db, segs
		}
	}
	panic("unreachable")
}

// forge re-encodes segment k after edit changes its header or frames.
func forge(t *testing.T, st *store.Mem, k string, edit func(h *format.SegHeader, fs []*format.Frame)) {
	t.Helper()
	st.Raw(func(o map[string][]byte) {
		h, ds, _, err := format.DecodeSegment(o[k])
		if err != nil {
			t.Fatal(err)
		}
		fs := make([]*format.Frame, len(ds))
		for i, d := range ds {
			fs[i] = d.Frame
		}
		edit(&h, fs)
		b := format.EncodeHeader(h)
		for _, f := range fs {
			b, _ = format.AppendFrame(b, f)
		}
		o[k] = b
	})
}

// recoverErr is recoverWith returning the error (full scan, read-only).
func recoverErr(db *DB, st store.Store) error {
	m, err := loadLatestManifest(ctx, st)
	if err != nil {
		return err
	}
	fresh := &DB{st: st, opts: db.opts, epoch: db.epoch, keys: db.keys, env: db.env, man: m, logs: map[string]*logState{}, fullScan: true}
	_, err = fresh.loadLog(ctx, "L1", false)
	return err
}

// A segment that decodes cleanly but breaks a log invariant is refused.
func TestRecoverRefusesForgedSegments(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(t *testing.T, st *store.Mem, segs []string) store.Store
		want error
	}{
		{"epoch goes back", func(t *testing.T, st *store.Mem, segs []string) store.Store {
			forge(t, st, segs[len(segs)-1], func(h *format.SegHeader, _ []*format.Frame) { h.Epoch = 1 })
			return st
		}, ErrCorrupt},
		{"LSN gap", func(t *testing.T, st *store.Mem, segs []string) store.Store {
			forge(t, st, segs[2], func(_ *format.SegHeader, fs []*format.Frame) { fs[0].LSN++ })
			return st
		}, ErrCorrupt},
		{"LSN gap, chain re-forged", func(t *testing.T, st *store.Mem, segs []string) store.Store {
			// the chain check alone cannot see this: every Prev is right
			forge(t, st, segs[len(segs)-1], func(_ *format.SegHeader, fs []*format.Frame) {
				prev := fs[0].Prev
				for _, f := range fs {
					f.LSN++
					f.Prev = prev
					prev = format.ChainNext(prev, format.EncodePayload(f))
				}
			})
			return st
		}, ErrCorrupt},
		{"chain broken", func(t *testing.T, st *store.Mem, segs []string) store.Store {
			forge(t, st, segs[2], func(_ *format.SegHeader, fs []*format.Frame) { fs[0].Prev[0] ^= 1 })
			return st
		}, ErrCorrupt},
		{"store reports corruption", func(t *testing.T, st *store.Mem, segs []string) store.Store {
			return errGet{st, segs[2], store.ErrCorrupt}
		}, ErrCorrupt},
		{"read fails", func(t *testing.T, st *store.Mem, segs []string) store.Store {
			return failGet{st, segs[2]}
		}, errFailGet},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, db, segs := logFixture(t)
			if len(segs) < 5 {
				t.Fatalf("fixture: %d segments", len(segs))
			}
			if err := recoverErr(db, st); err != nil {
				t.Fatalf("intact log: %v", err)
			}
			if err := recoverErr(db, tc.mut(t, st, segs)); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

// errGet fails reads of one key with err.
type errGet struct {
	store.Store
	key string
	err error
}

func (e errGet) Get(ctx context.Context, k string) ([]byte, error) {
	if k == e.key {
		return nil, e.err
	}
	return e.Store.Get(ctx, k)
}

// The read path (chainAt, entryAt, readSeg, Entries) on good and bad input.
func TestLogReadPaths(t *testing.T) {
	st, _, _ := logFixture(t)
	db := mustOpen(t, st, WithReadAhead(1))
	defer db.Close()
	es, err := db.Entries("main")
	if err != nil || len(es) != 7 {
		t.Fatalf("Entries: %d, %v", len(es), err)
	}
	db.mu.Lock()
	l := db.logs[db.man.Refs["main"]]
	db.mu.Unlock()
	head := l.head()
	if _, err := l.chainAt(ctx, head+5); !errors.Is(err, ErrBadLSN) {
		t.Fatalf("chainAt past head: %v", err)
	}
	if _, err := l.entryAt(ctx, head+5); !errors.Is(err, ErrNotFound) {
		t.Fatalf("entryAt past head: %v", err)
	}
	if _, err := l.entryAt(ctx, 0); !errors.Is(err, ErrCompacted) {
		t.Fatalf("entryAt(0): %v", err)
	}
	// every chain value read back equals the one recovery computed
	for lsn := uint64(1); lsn <= head; lsn++ {
		if _, err := l.chainAt(ctx, lsn); err != nil {
			t.Fatalf("chainAt %d: %v", lsn, err)
		}
		if e, err := l.entryAt(ctx, lsn); err != nil || e.LSN != lsn {
			t.Fatalf("entryAt %d: %v", lsn, err)
		}
	}
	// a failing segment read surfaces from both
	s := l.segs[1]
	last := l.segs[len(l.segs)-1]
	if last.n < 2 {
		t.Fatalf("fixture: last segment has %d frames", last.n)
	}
	l.st = failGet{st, segKey("L1", last.seq)}
	if _, err := l.chainAt(ctx, last.first); !errors.Is(err, errFailGet) {
		t.Fatalf("chainAt, segment unreadable: %v", err)
	}
	l.st = failGet{st, segKey("L1", s.seq)}
	if _, err := l.chainAt(ctx, s.first-1); !errors.Is(err, errFailGet) {
		t.Fatalf("chainAt, next segment unreadable: %v", err)
	}
	if _, err := l.entryAt(ctx, s.first); !errors.Is(err, errFailGet) {
		t.Fatalf("entryAt, segment unreadable: %v", err)
	}
	// an index that disagrees with the segment is corruption, not data
	l.st = st
	l.mu.Lock()
	l.segs[1].first++
	l.mu.Unlock()
	if _, err := l.readSeg(ctx, l.segs[1]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("readSeg, index mismatch: %v", err)
	}
	l.mu.Lock()
	l.segs[1].first--
	l.mu.Unlock()
}
