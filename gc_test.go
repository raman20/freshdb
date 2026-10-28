package freshdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/freshdb/store"
)

func put(t testing.TB, db *DB, br, k, v string) {
	t.Helper()
	if err := db.Branch(br).Update(ctx, func(tx *Tx) error { return tx.Put(k, []byte(v)) }); err != nil {
		t.Fatal(err)
	}
}

func manifestIDs(t testing.TB, st store.Store) []string {
	t.Helper()
	ks, err := st.List(ctx, "manifest/")
	if err != nil {
		t.Fatal(err)
	}
	return ks
}

// KeepFor keeps history by frame time; KeepCommits by count; the cut is
// the floor afterwards.
func TestGCRetentionWindow(t *testing.T) {
	var now atomic.Int64
	st := store.NewMem()
	db := mustOpen(t, st, WithClock(func() int64 { return now.Load() }))
	defer db.Close()
	for i := 1; i <= 10; i++ {
		now.Store(int64(i) * int64(time.Second))
		put(t, db, "main", "k", fmt.Sprint(i))
	}
	h, _ := db.Branch("main").Head()
	// frames at t=1s..10s are LSNs h-9..h; keep 5.5s at now=10s => cut at t=4s
	if _, err := db.GC(ctx, GCOptions{KeepFor: 5500 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	cut := h - 6
	if err := db.Branch("main").ViewAt(ctx, cut-1, func(*Tx) error { return nil }); !errors.Is(err, ErrCompacted) {
		t.Fatalf("below the cut: %v", err)
	}
	for at := cut; at <= h; at++ {
		var got string
		if err := db.Branch("main").ViewAt(ctx, at, func(tx *Tx) error {
			v, err := tx.Get("k")
			got = string(v)
			return err
		}); err != nil || got != fmt.Sprint(at-(h-10)) {
			t.Fatalf("@%d: %q %v", at, got, err)
		}
	}
	// count-based: keep 2 => floor h-2
	if _, err := db.GC(ctx, GCOptions{KeepCommits: 2}); err != nil {
		t.Fatal(err)
	}
	if err := db.Branch("main").ViewAt(ctx, h-3, func(*Tx) error { return nil }); !errors.Is(err, ErrCompacted) {
		t.Fatalf("below keep-commits: %v", err)
	}
	if err := db.Rewind(ctx, "main", h-2); err != nil {
		t.Fatalf("rewind inside the window: %v", err)
	}
	// reopen: the base below the head and the segments above it load
	db.Close()
	db = mustOpen(t, st, WithClock(func() int64 { return now.Load() }))
	if r, err := db.Verify(ctx); err != nil || !r.OK {
		t.Fatalf("verify %+v %v", r, err)
	}
}

// Rewinds retire logs; GC drops the ones only old reflog entries reach,
// deletes their objects, and old manifests.
func TestGCDropsRetiredLogsAndManifests(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	defer db.Close()
	var ids []string
	for r := 0; r < 5; r++ {
		for i := 0; i < 3; i++ {
			put(t, db, "main", fmt.Sprint("k", i), fmt.Sprint(r, i))
		}
		ids = append(ids, db.Manifest().Refs["main"])
		h, _ := db.Branch("main").Head()
		if err := db.Rewind(ctx, "main", h-1); err != nil {
			t.Fatal(err)
		}
	}
	want := dump(t, db, "main", 0)
	before := len(manifestIDs(t, st))
	stats, err := db.GC(ctx, GCOptions{KeepReflog: 1, KeepManifests: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := dump(t, db, "main", 0); got != want {
		t.Fatalf("state changed:\n%s\nvs\n%s", got, want)
	}
	if stats.ReflogPruned != 4 || len(db.Manifest().Reflog) != 1 {
		t.Fatalf("reflog: %+v %d", stats, len(db.Manifest().Reflog))
	}
	// the last rewind's old log is kept (reflog), the four before are gone
	if stats.LogsDropped != 4 {
		t.Fatalf("dropped %d", stats.LogsDropped)
	}
	for _, id := range ids[:4] {
		if n, _ := objects(t, st, "log/"+id+"/"); n != 0 {
			t.Fatalf("log %s: %d segments left", id, n)
		}
		if err := db.ViewLog(ctx, id, 0, func(*Tx) error { return nil }); !errors.Is(err, ErrNotFound) {
			t.Fatalf("ViewLog of a dropped log: %v", err)
		}
	}
	if err := db.ViewLog(ctx, ids[4], 0, func(*Tx) error { return nil }); err != nil {
		t.Fatalf("ViewLog of the kept log: %v", err)
	}
	if after := len(manifestIDs(t, st)); after != 2 || stats.ManifestsDeleted < before-2 {
		t.Fatalf("manifests %d -> %d (%+v)", before, after, stats)
	}
	db.Close()
	db = mustOpen(t, st)
	if got := dump(t, db, "main", 0); got != want {
		t.Fatal("state changed after reopen")
	}
	if r, err := db.Verify(ctx); err != nil || !r.OK {
		t.Fatalf("verify %+v %v", r, err)
	}
}

// A branch that still reads its parent keeps the parent's history up to
// its branch point (and its own window).
func TestGCRespectsDependentBranch(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	defer db.Close()
	for i := 0; i < 6; i++ {
		put(t, db, "main", "k", fmt.Sprint(i))
	}
	if err := db.CreateBranch(ctx, "b", "main", 3); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		put(t, db, "main", "k", fmt.Sprint("m", i))
	}
	want := dump(t, db, "b", 0)
	if _, err := db.GC(ctx, GCOptions{KeepCommits: 1}); err != nil {
		t.Fatal(err)
	}
	if got := dump(t, db, "b", 0); got != want {
		t.Fatalf("branch b changed:\n%s\nvs\n%s", got, want)
	}
	// b has no frames of its own: it still depends on main up to 3, and
	// its window (head 3, keep 1) reaches LSN 2
	if err := db.Branch("b").ViewAt(ctx, 2, func(*Tx) error { return nil }); err != nil {
		t.Fatalf("b@2: %v", err)
	}
	db.Close()
	db = mustOpen(t, st)
	if got := dump(t, db, "b", 0); got != want {
		t.Fatal("b changed after reopen")
	}
	if r, err := db.Verify(ctx); err != nil || !r.OK {
		t.Fatalf("verify %+v %v", r, err)
	}
}

// A fenced writer whose next manifest slot GC deleted must still be fenced
// (putManifest's post-create List; FreshGC_nopost.cfg is the TLC
// counterexample without it), and leaves no manifest behind.
func TestGCZombieInDeletedManifestSlot(t *testing.T) {
	st := store.NewMem()
	z := mustOpen(t, st)
	put(t, z, "main", "a", "1")
	zid := z.Manifest().ID
	db := mustOpen(t, st)
	defer db.Close()
	put(t, db, "main", "b", "2")
	for i := 0; i < 3; i++ {
		if err := db.CreateBranch(ctx, fmt.Sprint("x", i), "main", 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.GC(ctx, GCOptions{KeepManifests: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, manifestKey(zid+1)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("slot %d not deleted: %v", zid+1, err)
	}
	latest := db.Manifest().ID
	if err := z.CreateBranch(ctx, "zombie", "main", 0); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie commit: %v", err)
	}
	z.Crash()
	if ks := manifestIDs(t, st); len(ks) != 1 || ks[0] != manifestKey(latest) {
		t.Fatalf("manifests %v, want only %d", ks, latest)
	}
	db.Close()
	db = mustOpen(t, st)
	if _, ok := db.Manifest().Refs["zombie"]; ok {
		t.Fatal("zombie branch visible")
	}
	if r, err := db.Verify(ctx); err != nil || !r.OK {
		t.Fatalf("verify %+v %v", r, err)
	}
}

// Crash at every store write and delete of a GC run, then reopen: the
// state is unchanged and Verify passes; a second GC finishes the job.
func TestGCCrashAnywhere(t *testing.T) {
	for k := 1; ; k++ {
		mem := store.NewMem()
		db := mustOpen(t, mem)
		for r := 0; r < 3; r++ {
			fill(t, db, 2)
			h, _ := db.Branch("main").Head()
			if err := db.Rewind(ctx, "main", h-1); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.CreateBranch(ctx, "b", "main", 0); err != nil {
			t.Fatal(err)
		}
		want, wantB := dump(t, db, "main", 0), dump(t, db, "b", 0)
		db.Crash()
		fs := &crashStore{Store: mem}
		db = mustOpen(t, fs)
		fs.failAt.Store(int64(k))
		_, gerr := db.GC(ctx, GCOptions{KeepCommits: 3, KeepReflog: 1, KeepManifests: 1})
		db.Crash()
		db = mustOpen(t, mem)
		if got := dump(t, db, "main", 0); got != want {
			t.Fatalf("crash at op %d: main differs", k)
		}
		if got := dump(t, db, "b", 0); got != wantB {
			t.Fatalf("crash at op %d: b differs", k)
		}
		if _, err := db.GC(ctx, GCOptions{KeepCommits: 3, KeepReflog: 1, KeepManifests: 1}); err != nil {
			t.Fatalf("crash at op %d: second gc: %v", k, err)
		}
		if r, err := db.Verify(ctx); err != nil || !r.OK {
			t.Fatalf("crash at op %d: verify %+v %v", k, r, err)
		}
		db.Close()
		if gerr == nil && !fs.hit.Load() {
			t.Logf("covered %d crash points", k-1)
			return // GC finished without reaching op k: every point covered
		}
		if k > 500 {
			t.Fatal("too many ops")
		}
	}
}

// crashStore fails the failAt-th write or delete and every one after it,
// like a process dying there (0 = off).
type crashStore struct {
	store.Store
	n, failAt atomic.Int64
	hit       atomic.Bool
}

func (c *crashStore) dead() bool {
	if c.failAt.Load() == 0 {
		return false
	}
	if c.n.Add(1) >= c.failAt.Load() {
		c.hit.Store(true)
		return true
	}
	return false
}
func (c *crashStore) PutIfAbsent(ctx context.Context, k string, b []byte) error {
	if c.dead() {
		return errors.New("injected crash")
	}
	return c.Store.PutIfAbsent(ctx, k, b)
}
func (c *crashStore) Delete(ctx context.Context, k string) error {
	if c.dead() {
		return errors.New("injected crash")
	}
	return c.Store.Delete(ctx, k)
}
func (c *crashStore) Keys(ctx context.Context) ([]string, error) { return store.Keys(ctx, c.Store) }

func TestGCReflogTimestamps(t *testing.T) {
	var now atomic.Int64
	db := mustOpen(t, store.NewMem(), WithClock(func() int64 { return now.Load() }))
	defer db.Close()
	for i := 1; i <= 4; i++ {
		now.Store(int64(i) * int64(time.Hour))
		put(t, db, "main", "k", fmt.Sprint(i))
		if err := db.CreateBranch(ctx, fmt.Sprint("b", i), "main", 0); err != nil {
			t.Fatal(err)
		}
	}
	// keep 1 by count, plus everything newer than 1.5h at now=4h (b3 at 3h, b4 at 4h)
	st, err := db.GC(ctx, GCOptions{KeepFor: 90 * time.Minute, KeepReflog: 1})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, r := range db.Manifest().Reflog {
		names = append(names, r.Name)
	}
	if strings.Join(names, ",") != "b3,b4" || st.ReflogPruned != 2 {
		t.Fatalf("reflog %v (%+v)", names, st)
	}
	if len(db.Branches()) != 5 {
		t.Fatal("branches must stay")
	}
}

// TestGCBoundsStorage: overwrite + branch + rewind churn with GC each cycle
// keeps data objects, bytes and manifests flat (Compact alone keeps every
// retired log and manifest).
func TestGCBoundsStorage(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	val := bytes.Repeat([]byte("x"), 200)
	var firstN, firstM int
	var first int64
	for cycle := 0; cycle < 10; cycle++ {
		for r := 0; r < 20; r++ {
			if err := db.Update(ctx, func(tx *Tx) error {
				for k := 0; k < 50; k++ {
					if err := tx.Put(fmt.Sprint("k", k), append([]byte(fmt.Sprint(cycle, r)), val...)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		h, _ := db.Branch("main").Head()
		if err := db.Rewind(ctx, "main", h-2); err != nil { // retires a log
			t.Fatal(err)
		}
		if _, err := db.GC(ctx, GCOptions{KeepReflog: 1, KeepManifests: 4}); err != nil {
			t.Fatal(err)
		}
		all, size := objects(t, st, "")
		m, msize := objects(t, st, "manifest/")
		n, size := all-m, size-msize
		if cycle == 0 {
			first, firstN, firstM = size, n, m
		}
		t.Logf("cycle %d: %d data objects / %d B, %d manifests", cycle, n, size, m)
		if size > first+first/5 || n > firstN+2 || m > firstM+2 {
			t.Fatalf("cycle %d: not bounded: %d objects %d B %d manifests (first %d / %d / %d)", cycle, n, size, m, firstN, first, firstM)
		}
	}
}

// TestDeleteBranchGC: a deleted branch stays readable through the reflog
// until GC prunes that entry, then its log is dropped.
func TestDeleteBranchGC(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	if err := db.Update(ctx, func(tx *Tx) error { return tx.Put("a", []byte("1")) }); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateBranch(ctx, "feat", "main", 0); err != nil {
		t.Fatal(err)
	}
	if err := db.Branch("feat").Update(ctx, func(tx *Tx) error { return tx.Put("f", []byte("x")) }); err != nil {
		t.Fatal(err)
	}
	id := db.Manifest().Refs["feat"]
	if err := db.DeleteBranch(ctx, "main"); !errors.Is(err, ErrMainBranch) {
		t.Fatalf("delete main: %v", err)
	}
	if err := db.DeleteBranch(ctx, "nope"); !errors.Is(err, ErrNoBranch) {
		t.Fatalf("delete nope: %v", err)
	}
	if err := db.DeleteBranch(ctx, "feat"); err != nil {
		t.Fatal(err)
	}
	if err := db.Branch("feat").Update(ctx, func(tx *Tx) error { return nil }); err == nil {
		t.Fatal("write to a deleted branch")
	}
	view := func() error {
		return db.ViewLog(ctx, id, 0, func(tx *Tx) error {
			v, err := tx.Get("f")
			if err == nil && string(v) != "x" {
				t.Fatalf("f = %q", v)
			}
			return err
		})
	}
	if _, err := db.GC(ctx, GCOptions{KeepReflog: 4}); err != nil {
		t.Fatal(err)
	}
	if err := view(); err != nil {
		t.Fatalf("deleted branch unreadable while in the reflog: %v", err)
	}
	for i := 0; i < 3; i++ { // push the delete entry out of a 1-entry reflog
		if err := db.CreateBranch(ctx, fmt.Sprint("t", i), "main", 0); err != nil {
			t.Fatal(err)
		}
	}
	gs, err := db.GC(ctx, GCOptions{KeepReflog: 1})
	if err != nil {
		t.Fatal(err)
	}
	if gs.LogsDropped == 0 {
		t.Fatalf("log not dropped: %+v", gs)
	}
	if err := view(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dropped log: %v", err)
	}
	db.Close()
	db = mustOpen(t, st)
	if r, err := db.Verify(ctx); err != nil || !r.OK {
		t.Fatalf("verify %+v %v", r, err)
	}
}
