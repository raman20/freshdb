package freshdb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

func annObjects(t *testing.T, db *DB, st store.Store) int {
	t.Helper()
	db.mu.RLock()
	ids := sortedMapKeys(db.man.Logs)
	db.mu.RUnlock()
	n := 0
	for _, id := range ids {
		ks, err := st.List(ctx, annDir(id))
		if err != nil {
			t.Fatal(err)
		}
		n += len(ks)
	}
	return n
}

func annStat(db *DB) lens.ANNLoadStats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.annStats[db.man.Refs["main"]]
}

// TestANNPersist: graphs saved on Close are restored on open (same
// answers), a crash leaves an older snapshot that is caught up, compaction
// hands graphs over, Shred deletes saved graphs and saves none built before
// it, and GC deletes the graphs of a dropped log.
func TestANNPersist(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	r := rand.New(rand.NewSource(11))
	rv := func() []float32 {
		v := make([]float32, 8)
		for i := range v {
			v[i] = float32(r.NormFloat64())
		}
		return v
	}
	put := func(db *DB, branch string, from, to int, subj string) {
		t.Helper()
		err := db.Branch(branch).Update(ctx, func(tx *Tx) error {
			for i := from; i < to; i++ {
				var o []WriteOpt
				if subj != "" && i%10 == 0 {
					o = append(o, Subject(subj))
				}
				if err := tx.Vector("c").Put(fmt.Sprint("v", i), rv(), o...); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := db.EnableANN(ctx, "c", ANNOptions{M: 8, EfConstruction: 40}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6000; i += 1000 {
		put(db, "main", i, i+1000, "alice")
	}
	qs := make([][]float32, 20)
	before := make([]string, len(qs))
	for i := range qs {
		qs[i] = rv()
		before[i] = fmt.Sprint(annSearch(t, db, qs[i], 10, false))
	}
	db.Close()
	if n := annObjects(t, db, st); n != 1 {
		t.Fatalf("%d ann objects after close", n)
	}

	db = mustOpen(t, st)
	if s := annStat(db); s.Graphs != 1 || s.Reused != 6000 || s.Stale+s.Inserted+s.Rebuilt != 0 {
		t.Fatalf("restore %+v", s)
	}
	for i, q := range qs {
		if got := fmt.Sprint(annSearch(t, db, q, 10, false)); got != before[i] {
			t.Fatalf("query %d differs after restore", i)
		}
	}
	// Crash after more writes: the snapshot is older than the log head.
	put(db, "main", 5000, 7000, "")
	db.Crash()
	db = mustOpen(t, st)
	if s := annStat(db); s.Graphs != 1 || s.Reused != 5000 || s.Stale != 1000 || s.Inserted != 2000 {
		t.Fatalf("catch-up %+v", s)
	}
	recallOK := func(when string) {
		t.Helper()
		var sum float64
		for i := 0; i < 30; i++ {
			q := rv()
			in := map[string]bool{}
			for _, h := range annSearch(t, db, q, 10, true) {
				in[h.ID] = true
			}
			for _, h := range annSearch(t, db, q, 10, false) {
				if in[h.ID] {
					sum++
				}
			}
		}
		if sum/300 < 0.9 {
			t.Fatalf("%s: recall %.3f", when, sum/300)
		}
	}
	recallOK("caught up")
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	if s := annStat(db); s.Graphs != 1 || s.Reused != 7000 {
		t.Fatalf("compaction hand-over %+v", s)
	}
	recallOK("compacted")
	db.Close()

	// Shred: saved graphs go, none is saved until a rebuild.
	db = mustOpen(t, st)
	if _, err := db.Shred(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	if n := annObjects(t, db, st); n != 0 {
		t.Fatalf("%d ann objects after shred", n)
	}
	db.Close()
	if n := annObjects(t, db, st); n != 0 {
		t.Fatalf("%d ann objects saved after shred", n)
	}
	db = mustOpen(t, st)
	if s := annStat(db); s.Rebuilt != 1 || s.Graphs != 0 {
		t.Fatalf("after shred %+v", s)
	}
	recallOK("rebuilt")
	db.Close()
	if n := annObjects(t, db, st); n != 1 {
		t.Fatalf("%d ann objects after rebuild+close", n)
	}

	// A deleted branch's saved graph goes when GC drops its log.
	db = mustOpen(t, st)
	h, _ := db.Branch("main").Head()
	if err := db.CreateBranch(ctx, "b", "main", h); err != nil {
		t.Fatal(err)
	}
	put(db, "b", 9000, 9010, "")
	db.Close()
	db = mustOpen(t, st)
	if n := annObjects(t, db, st); n != 2 {
		t.Fatalf("%d ann objects with a branch", n)
	}
	db.mu.RLock()
	bid := db.man.Refs["b"]
	db.mu.RUnlock()
	if err := db.DeleteBranch(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	// a newer reflog entry, so KeepReflog 1 lets b's log go
	if err := db.CreateBranch(ctx, "c", "main", 0); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteBranch(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	if gs, err := db.GC(ctx, GCOptions{KeepReflog: 1, KeepManifests: 1}); err != nil || gs.LogsDropped != 1 {
		t.Fatal(gs, err)
	}
	if ks, _ := st.List(ctx, annDir(bid)); len(ks) != 0 {
		t.Fatalf("dropped log keeps %d ann objects", len(ks))
	}
	db.Close()
}

// A corrupt or truncated snapshot is ignored (rebuild), never trusted.
func TestANNPersistCorrupt(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	if err := db.EnableANN(ctx, "c", ANNOptions{}); err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(2))
	err := db.Update(ctx, func(tx *Tx) error {
		for i := 0; i < 500; i++ {
			if err := tx.Vector("c").Put(fmt.Sprint(i), []float32{float32(r.NormFloat64()), float32(r.NormFloat64()), 1}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	id := db.man.Refs["main"]
	db.Close()
	ks, _ := st.List(ctx, annDir(id))
	if len(ks) != 1 {
		t.Fatalf("%d snapshots", len(ks))
	}
	b, _ := st.Get(ctx, ks[0])
	b[len(b)/2] ^= 1
	_ = st.Delete(ctx, ks[0])
	if err := st.PutIfAbsent(ctx, ks[0], b); err != nil {
		t.Fatal(err)
	}
	db = mustOpen(t, st)
	if s := annStat(db); s.Rebuilt != 1 || s.Graphs != 0 {
		t.Fatalf("corrupt snapshot used: %+v", s)
	}
	db.Close()
}

// A zombie (fenced by a newer opener) closing cleanly saves no graph.
func TestANNPersistZombie(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	if err := db.EnableANN(ctx, "c", ANNOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(ctx, func(tx *Tx) error { return tx.Vector("c").Put("a", []float32{1, 2}) }); err != nil {
		t.Fatal(err)
	}
	id := db.man.Refs["main"]
	nw := mustOpen(t, st)
	db.Close() // zombie: never noticed the fence
	if ks, _ := st.List(ctx, annDir(id)); len(ks) != 0 {
		t.Fatalf("zombie saved %v", ks)
	}
	nw.Close()
	if ks, _ := st.List(ctx, annDir(id)); len(ks) != 1 {
		t.Fatalf("owner saved %v", ks)
	}
}
