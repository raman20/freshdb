package freshdb

// Component A (the log) tests: docs/components/log.md lists the defect
// each one guards.

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// A-D4: an open that fails after some writers started used to leave their
// goroutines (and the value cache) running.
func TestOpenFailureLeaksNothing(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	if err := db.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateBranch(ctx, "z", "main", 0); err != nil {
		t.Fatal(err)
	}
	id := db.Manifest().Refs["z"]
	db.Close()
	// a gap in branch z's log: every open fails after main's writer started
	if err := st.PutIfAbsent(ctx, segKey(id, 99), format.EncodeHeader(format.SegHeader{Epoch: 1, Seq: 99})); err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		if _, err := OpenStore(ctx, st, WithLogf(func(string, ...any) {})); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("open %d: %v, want ErrCorrupt", i, err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+2 {
		t.Fatalf("goroutines: %d before 20 failed opens, %d after", before, n)
	}
}

// dupStore applies every put but answers 412 (a retried request whose first
// attempt landed); reads fail while getDown is set.
type dupStore struct {
	store.Store
	getDown bool
}

func (d *dupStore) PutIfAbsent(ctx context.Context, k string, b []byte) error {
	if err := d.Store.PutIfAbsent(ctx, k, b); err != nil {
		return err
	}
	return store.ErrExists
}

func (d *dupStore) Get(ctx context.Context, k string) ([]byte, error) {
	if d.getDown {
		return nil, errors.New("get: connection reset")
	}
	return d.Store.Get(ctx, k)
}

// A-D3: a 412 whose read-back fails is an unknown outcome, never "fenced":
// the writer must not tell a client "not committed" for a write that landed.
func TestPutExactlyOnceOutcomes(t *testing.T) {
	d := &dupStore{Store: store.NewMem()}
	if err := putExactlyOnce(ctx, d, "a", []byte("x")); err != nil {
		t.Fatalf("lost-response put: %v, want nil", err)
	}
	if err := putExactlyOnce(ctx, d, "a", []byte("y")); !errors.Is(err, store.ErrExists) || errors.Is(err, ErrUnknownOutcome) {
		t.Fatalf("other bytes: %v, want plain ErrExists", err)
	}
	d.getDown = true
	err := putExactlyOnce(ctx, d, "b", []byte("x"))
	if !errors.Is(err, ErrUnknownOutcome) || errors.Is(err, store.ErrExists) {
		t.Fatalf("unreadable 412: %v, want ErrUnknownOutcome without ErrExists", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := putExactlyOnce(cctx, d, "c", []byte("x")); !errors.Is(err, ErrUnknownOutcome) {
		t.Fatalf("cancelled: %v, want ErrUnknownOutcome", err)
	}
}

// tornTail writes keys k0..k9 (one segment each), then cuts the newest
// segment of L1 in half. It returns that segment's key and damaged bytes.
func tornTail(t *testing.T, st *store.Mem) (string, []byte) {
	t.Helper()
	db := mustOpen(t, st)
	for i := 0; i < 10; i++ {
		if err := db.Put(ctx, fmt.Sprint("k", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	var last string
	var torn []byte
	st.Raw(func(o map[string][]byte) {
		for k := range o {
			if strings.HasPrefix(k, "log/L1/") && strings.HasSuffix(k, ".seg") && k > last {
				last = k
			}
		}
		o[last] = o[last][:len(o[last])-3]
		torn = append([]byte{}, o[last]...)
	})
	return last, torn
}

// noDeletes fails the test on any Delete of a log object.
type noDeletes struct {
	store.Store
	t *testing.T
}

func (n noDeletes) Delete(ctx context.Context, k string) error {
	if strings.HasPrefix(k, "log/") {
		n.t.Errorf("repair deleted %s", k)
	}
	return n.Store.Delete(ctx, k)
}

// A-D1: repair used to delete the damaged segment and re-create its prefix:
// a crash in between lost the prefix, and a zombie could fill the freed
// slot. Now the segment is never touched; a note records the repair.
func TestRepairTailNeverRewrites(t *testing.T) {
	st := store.NewMem()
	seg, torn := tornTail(t, st)
	db, err := OpenStore(ctx, noDeletes{st, t}, quiet(), WithRepairTail(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, "k9"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("k9 from the torn frame: %v", err)
	}
	if err := db.Put(ctx, "after", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if b, _ := st.Get(ctx, seg); string(b) != string(torn) {
		t.Fatal("damaged segment was rewritten")
	}
	// the note makes the repair permanent: no flag needed any more
	db = mustOpen(t, st)
	for _, k := range []string{"k8", "after"} {
		if _, err := db.Get(ctx, k); err != nil {
			t.Fatalf("%s after repair: %v", k, err)
		}
	}
	r, err := db.Verify(ctx)
	if err != nil || !r.OK || !strings.Contains(strings.Join(r.Warnings, "\n"), "repaired segment") {
		t.Fatalf("verify: %+v %v", r, err)
	}
	db.Close()
	// damage after the repair no longer matches the note
	st.Raw(func(o map[string][]byte) { o[seg] = o[seg][:len(o[seg])-1] })
	if _, err := OpenStore(ctx, st, quiet()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("segment changed after repair: %v, want ErrCorrupt", err)
	}
}

// A crash after the note and before the fence: the next open (with or
// without the flag) honours the note and fences as usual.
func TestRepairNoteWithoutFence(t *testing.T) {
	st := store.NewMem()
	seg, torn := tornTail(t, st)
	_, fr, valid, _ := format.DecodeSegment(torn)
	seq, _ := parseNum(seg, "log/L1/", ".seg")
	if err := st.PutIfAbsent(ctx, repairKey("L1", seq), repairNote(seq, torn, len(fr), valid)); err != nil {
		t.Fatal(err)
	}
	db := mustOpen(t, st)
	defer db.Close()
	if _, err := db.Get(ctx, "k8"); err != nil {
		t.Fatal(err)
	}
}

// A-D2: the tail was "the last key listed", so any other object under the
// log's prefix made a damaged tail look like mid-log damage.
func TestTailIgnoresOtherKeys(t *testing.T) {
	st := store.NewMem()
	tornTail(t, st)
	if err := st.PutIfAbsent(ctx, "log/L1/zz-stray", []byte("x")); err != nil {
		t.Fatal(err)
	}
	db, err := OpenStore(ctx, st, quiet(), WithRepairTail(true))
	if err != nil {
		t.Fatalf("repair with a stray key: %v", err)
	}
	db.Close()
}

// adopter: once armed, every manifest put is immediately followed by a
// "newer opener" creating the next id from it.
type adopter struct {
	store.Store
	armed bool
}

func (a *adopter) PutIfAbsent(ctx context.Context, k string, b []byte) error {
	err := a.Store.PutIfAbsent(ctx, k, b)
	if n, ok := parseNum(k, "manifest/", ".mf"); ok && err == nil && a.armed {
		a.armed = false
		_ = a.Store.PutIfAbsent(ctx, manifestKey(n+1), b)
	}
	return err
}

// A-D5: a manifest we created, then found below a newer one, may have been
// adopted: the error must say fenced AND unknown outcome, not "not committed".
func TestManifestLostAfterPutIsUnknown(t *testing.T) {
	a := &adopter{Store: store.NewMem()}
	db, err := OpenStore(ctx, a, quiet())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a.armed = true
	err = db.CreateBranch(ctx, "b", "main", 0)
	if !errors.Is(err, ErrFenced) || !errors.Is(err, ErrUnknownOutcome) {
		t.Fatalf("create branch: %v, want ErrFenced and ErrUnknownOutcome", err)
	}
}

// failGet fails reads of one key.
type failGet struct {
	store.Store
	key string
}

var errFailGet = errors.New("injected read failure")

func (f failGet) Get(ctx context.Context, k string) ([]byte, error) {
	if k == f.key {
		return nil, errFailGet
	}
	return f.Store.Get(ctx, k)
}

// Replay reads segments concurrently (ReadAhead) but hands entries over in
// LSN order, returns the first error in log order, and leaves no reads
// running after an error.
func TestEachReadAhead(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	for i := 0; i < 40; i++ {
		if err := db.Put(ctx, fmt.Sprint("k", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	defer db.Close()
	l, _ := recoverWith(t, db, st, true)
	before := runtime.NumGoroutine()
	for _, ra := range []int{1, 3, 8, 100} {
		l.readAhead = ra
		var got []uint64
		if err := l.each(ctx, 37, func(e *lens.Entry) error { got = append(got, e.LSN); return nil }); err != nil {
			t.Fatal(err)
		}
		if len(got) != 37 || got[0] != 1 || got[36] != 37 {
			t.Fatalf("readAhead %d: %d entries %v", ra, len(got), got)
		}
		for i := 1; i < len(got); i++ {
			if got[i] != got[i-1]+1 {
				t.Fatalf("readAhead %d: out of order at %d: %v", ra, i, got)
			}
		}
		stop := errors.New("stop")
		n := 0
		if err := l.each(ctx, 40, func(*lens.Entry) error {
			if n++; n == 5 {
				return stop
			}
			return nil
		}); err != stop || n != 5 {
			t.Fatalf("readAhead %d: early stop: %v after %d", ra, err, n)
		}
		x := &logState{meta: l.meta, segs: l.segs, readAhead: ra, st: failGet{Store: st, key: segKey("L1", l.segs[20].seq)}}
		n = 0
		if err := x.each(ctx, 40, func(*lens.Entry) error { n++; return nil }); !errors.Is(err, errFailGet) || n != 20 {
			t.Fatalf("readAhead %d: read failure: %v after %d entries", ra, err, n)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines: %d before, %d after", before, n)
	}
}
