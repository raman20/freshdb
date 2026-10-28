package freshdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/freshdb/store"
	"pgregory.net/rapid"
)

// ftb is what testing.TB and *rapid.T share.
type ftb interface {
	Helper()
	Fatal(...any)
	Fatalf(string, ...any)
}

// dump renders everything a client can read on a branch at at (0 =
// head): KV, docs, an index, vector hits and topic messages (TS left
// out: compaction reads the clock).
func dump(t ftb, db *DB, br string, at uint64) string {
	t.Helper()
	var b strings.Builder
	err := db.Branch(br).ViewAt(ctx, at, func(tx *Tx) error {
		kvs, err := tx.Scan("")
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			fmt.Fprintf(&b, "kv %s=%q\n", kv.Key, kv.Value)
		}
		docs, err := tx.scan(docSpace("d"), "")
		if err != nil {
			return err
		}
		for _, kv := range docs {
			fmt.Fprintf(&b, "doc %s=%s\n", kv.Key, kv.Value)
		}
		for _, v := range []any{0, 1} {
			ids, err := tx.Doc("d").Find("f", v)
			if err != nil {
				return err
			}
			fmt.Fprintf(&b, "find f=%v %v\n", v, ids)
		}
		hits, err := tx.Vector("v").Search([]float32{1, 1}, 100)
		if err != nil {
			return err
		}
		for _, h := range hits {
			fmt.Fprintf(&b, "vec %s %.4f\n", h.ID, h.Score)
		}
		ms, err := tx.Topic("t").Read(0, 0)
		if err != nil {
			return err
		}
		for _, m := range ms {
			fmt.Fprintf(&b, "msg %d@%d %q %v\n", m.Offset, m.LSN, m.Value, m.Redacted)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("dump %s@%d: %v", br, at, err)
	}
	return b.String()
}

func objects(t testing.TB, st store.Store, prefix string) (n int, size int64) {
	t.Helper()
	keys, err := store.Keys(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			b, err := st.Get(ctx, k)
			if err != nil {
				t.Fatal(err)
			}
			n++
			size += int64(len(b))
		}
	}
	return
}

// fill writes a mixed workload: overwrites (some values above the inline
// limit), deletes, subject data, docs, vectors, topics, a branch, a
// rewind and a shred.
func fill(t testing.TB, db *DB, rounds int, subj ...string) {
	sp := "s"
	if len(subj) > 0 {
		sp = subj[0]
	}
	t.Helper()
	big := bytes.Repeat([]byte("v"), 300)
	for r := 0; r < rounds; r++ {
		err := db.Update(ctx, func(tx *Tx) error {
			for k := 0; k < 8; k++ {
				val := []byte(fmt.Sprintf("r%d-k%d", r, k))
				if k%2 == 0 {
					val = append(append([]byte{}, big...), val...)
				}
				var o []WriteOpt
				if k%3 == 0 {
					o = append(o, Subject(fmt.Sprint(sp, k%2)))
				}
				if err := tx.Put(fmt.Sprint("k", k), val, o...); err != nil {
					return err
				}
			}
			if r%3 == 2 {
				tx.Delete("k7")
			}
			tx.Doc("d").Put(fmt.Sprint("d", r%5), map[string]any{"f": r % 2, "pad": string(big)}, Subject(fmt.Sprint(sp, r%2)))
			tx.Vector("v").Put(fmt.Sprint("x", r%4), []float32{float32(r % 3), 1})
			tx.Topic("t").Publish([]byte(fmt.Sprint("plain", r)))
			return tx.Topic("t").Publish(append([]byte(fmt.Sprint("secret", r)), big...), Subject(fmt.Sprint(sp, r%2)))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompactKeepsStateAndFreesStorage(t *testing.T) {
	for name, mk := range backends(t) {
		t.Run(name, func(t *testing.T) {
			st := mk()
			db := mustOpen(t, st, WithSnapshotEvery(7))
			db.RegisterIndex(ctx, "d", "f")
			fill(t, db, 40)
			if err := db.CreateBranch(ctx, "feat", "main", 0); err != nil {
				t.Fatal(err)
			}
			db.Branch("feat").Update(ctx, func(tx *Tx) error { return tx.Put("k1", []byte("feat")) })
			fill(t, db, 20)
			h, _ := db.Branch("main").Head()
			if err := db.Rewind(ctx, "main", h-5); err != nil { // leaves a retired log
				t.Fatal(err)
			}
			if _, err := db.Shred(ctx, "s1"); err != nil {
				t.Fatal(err)
			}
			want := map[string]string{"main": dump(t, db, "main", 0), "feat": dump(t, db, "feat", 0)}
			dg := map[string]map[string][32]byte{}
			for br := range want {
				dg[br], _, _ = db.LensDigests(br)
			}
			nBefore, szBefore := objects(t, st, "")
			cs, err := db.Compact(ctx, CompactOptions{})
			if err != nil {
				t.Fatal(err)
			}
			nAfter, szAfter := objects(t, st, "")
			t.Logf("%+v; objects %d -> %d, bytes %d -> %d", cs, nBefore, nAfter, szBefore, szAfter)
			if szAfter >= szBefore/2 {
				t.Fatalf("storage %d -> %d: compaction freed too little", szBefore, szAfter)
			}
			check := func(db *DB, when string) {
				t.Helper()
				for br, w := range want {
					if got := dump(t, db, br, 0); got != w {
						t.Fatalf("%s %s: state differs\nwant:\n%s\ngot:\n%s", when, br, w, got)
					}
					d1, _, _ := db.LensDigests(br)
					d2, err := db.RebuildDigests(br)
					if err != nil || fmt.Sprint(d1) != fmt.Sprint(d2) {
						t.Fatalf("%s %s: lens != fold (%v)", when, br, err)
					}
					for _, l := range []string{"vector", "docindex:d.f"} {
						if d1[l] != dg[br][l] {
							t.Fatalf("%s %s: lens %s digest changed by compaction", when, br, l)
						}
					}
				}
				if r, err := db.Verify(ctx); err != nil || !r.OK {
					t.Fatalf("%s: verify %+v %v", when, r, err)
				}
			}
			check(db, "after compact")
			if cs2, err := db.Compact(ctx, CompactOptions{}); err != nil || cs2.Logs != 0 {
				t.Fatalf("second compact of an unchanged db: %+v %v", cs2, err)
			}
			db.Close()
			db = mustOpen(t, st, WithSnapshotEvery(7))
			check(db, "after reopen")

			// history below the base is gone, the base itself is readable
			h, _ = db.Branch("main").Head()
			if err := db.Branch("main").ViewAt(ctx, h-1, func(*Tx) error { return nil }); !errors.Is(err, ErrCompacted) {
				t.Fatalf("time travel below base: %v", err)
			}
			if err := db.CreateBranch(ctx, "old", "main", h-1); !errors.Is(err, ErrCompacted) {
				t.Fatalf("branch below base: %v", err)
			}
			if err := db.Rewind(ctx, "main", h-1); !errors.Is(err, ErrCompacted) {
				t.Fatalf("rewind below base: %v", err)
			}
			if err := db.CreateBranch(ctx, "at", "main", h); err != nil {
				t.Fatal(err)
			}
			if got := dump(t, db, "at", 0); got != want["main"] {
				t.Fatal("branch at the base differs")
			}

			// keep going: write, compact again, promote across bases
			fill(t, db, 10, "t")
			db.Branch("at").Update(ctx, func(tx *Tx) error { return tx.Put("on-at", []byte("1")) })
			want = map[string]string{"main": dump(t, db, "main", 0), "feat": dump(t, db, "feat", 0), "at": dump(t, db, "at", 0)}
			mid, _ := db.Branch("main").Head()
			fill(t, db, 3, "t")
			midDump := dump(t, db, "main", mid)
			want["main"] = dump(t, db, "main", 0)
			for br := range want {
				dg[br], _, _ = db.LensDigests(br)
			}
			if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
				t.Fatal(err)
			}
			check(db, "after second compact")
			if err := db.Promote(ctx, "at", "main"); !errors.Is(err, ErrNotFastForward) {
				t.Fatalf("promote of a diverged branch: %v", err)
			}
			db.Close()
			db = mustOpen(t, st)
			defer db.Close()
			check(db, "after second reopen")
			if err := db.Branch("main").ViewAt(ctx, mid, func(*Tx) error { return nil }); !errors.Is(err, ErrCompacted) {
				t.Fatalf("time travel to %d after second compact: %v", mid, err)
			}
			_ = midDump
		})
	}
}

// Promote must still see ancestry through a compacted log.
func TestCompactPromoteFastForward(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	fill(t, db, 3)
	db.CreateBranch(ctx, "f", "main", 0)
	db.Branch("f").Update(ctx, func(tx *Tx) error { return tx.Put("x", []byte("f")) })
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Promote(ctx, "f", "main"); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(ctx, "x"); err != nil || string(v) != "f" {
		t.Fatalf("%q %v", v, err)
	}
}

// Commits running during compaction all land exactly once.
func TestCompactConcurrentCommits(t *testing.T) {
	db := mustOpen(t, store.NewMem(), WithSnapshotEvery(20))
	defer db.Close()
	var ok [4]atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range ok {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 1; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				err := db.Update(ctx, func(tx *Tx) error {
					v, _ := tx.Get(fmt.Sprint("c", g))
					n := 0
					fmt.Sscan(string(v), &n)
					tx.Put(fmt.Sprint("c", g), []byte(fmt.Sprint(n+1)))
					return tx.Topic("t").Publish([]byte(fmt.Sprint(g, ":", i)))
				})
				if err != nil {
					t.Errorf("g%d: %v", g, err)
					return
				}
				ok[g].Add(1)
			}
		}(g)
	}
	for i := 0; i < 20; i++ {
		if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
			t.Fatal(err)
		}
		db.View(ctx, func(tx *Tx) error { _, err := tx.Get("c0"); _ = err; return nil })
	}
	close(stop)
	wg.Wait()
	total := int64(0)
	db.View(ctx, func(tx *Tx) error {
		for g := range ok {
			v, _ := tx.Get(fmt.Sprint("c", g))
			n := int64(0)
			fmt.Sscan(string(v), &n)
			if n != ok[g].Load() {
				t.Fatalf("g%d: counter %d, %d commits acknowledged", g, n, ok[g].Load())
			}
			total += n
		}
		if l := tx.Topic("t").Len(); int64(l) != total {
			t.Fatalf("%d messages, %d commits", l, total)
		}
		return nil
	})
}

// With a grace period nothing is deleted until it has passed.
func TestCompactGrace(t *testing.T) {
	st := store.NewMem()
	var now atomic.Int64
	now.Store(1)
	db := mustOpen(t, st, WithClock(func() int64 { return now.Add(1) }))
	defer db.Close()
	fill(t, db, 10)
	cs, err := db.Compact(ctx, CompactOptions{Grace: time.Hour})
	if err != nil || cs.Deleted != 0 || cs.Pending == 0 {
		t.Fatalf("%+v %v", cs, err)
	}
	if n, _ := objects(t, st, "log/L1/00000000000000000001"); n != 1 {
		t.Fatal("first segment deleted before the grace period")
	}
	now.Add(int64(2 * time.Hour))
	cs, err = db.Compact(ctx, CompactOptions{Grace: time.Hour})
	if err != nil || cs.Deleted == 0 || cs.Pending != 0 || len(db.Manifest().Garbage) != 0 {
		t.Fatalf("%+v %v garbage=%v", cs, err, db.Manifest().Garbage)
	}
	if n, _ := objects(t, st, "log/L1/00000000000000000001"); n != 0 {
		t.Fatal("first segment kept after the grace period")
	}
}

// failStore fails manifest puts (after the bases were written) or deletes.
type failStore struct {
	store.Store
	failManifest, failDelete atomic.Bool
}

func (f *failStore) PutIfAbsent(ctx context.Context, k string, b []byte) error {
	if f.failManifest.Load() && strings.HasPrefix(k, "manifest/") {
		return errors.New("injected: manifest put failed")
	}
	return f.Store.PutIfAbsent(ctx, k, b)
}
func (f *failStore) Delete(ctx context.Context, k string) error {
	if f.failDelete.Load() {
		return errors.New("injected: delete failed")
	}
	return f.Store.Delete(ctx, k)
}
func (f *failStore) Keys(ctx context.Context) ([]string, error) { return store.Keys(ctx, f.Store) }

// A compaction that dies before its commit leaves the database as it was;
// its orphaned bases are swept by the next epoch. Failed deletes stay on
// the garbage list and are retried.
func TestCompactCrashAndSweepRetry(t *testing.T) {
	mem := store.NewMem()
	fs := &failStore{Store: mem}
	db := mustOpen(t, fs)
	fill(t, db, 10)
	want := dump(t, db, "main", 0)
	fs.failManifest.Store(true)
	if _, err := db.Compact(ctx, CompactOptions{}); err == nil {
		t.Fatal("compaction with a failing manifest put succeeded")
	}
	db.Crash()
	fs.failManifest.Store(false)
	orphans, _ := objects(t, mem, "base/")
	if orphans == 0 {
		t.Fatal("expected orphaned base objects")
	}
	db = mustOpen(t, fs)
	if got := dump(t, db, "main", 0); got != want {
		t.Fatal("state changed by a failed compaction")
	}
	fs.failDelete.Store(true)
	cs, err := db.Compact(ctx, CompactOptions{})
	if err != nil || cs.Pending == 0 {
		t.Fatalf("%+v %v", cs, err)
	}
	if n, _ := objects(t, mem, "base/"); n <= orphans {
		t.Fatalf("base objects %d", n)
	}
	fs.failDelete.Store(false)
	db.Close()
	db = mustOpen(t, fs)
	defer db.Close()
	if got := dump(t, db, "main", 0); got != want {
		t.Fatal("state changed")
	}
	fill(t, db, 1)
	cs, err = db.Compact(ctx, CompactOptions{})
	if err != nil || cs.Pending != 0 || len(db.Manifest().Garbage) != 0 {
		t.Fatalf("%+v %v", cs, err)
	}
	bases, _ := objects(t, mem, "base/")
	segs, _ := objects(t, mem, "log/")
	if bases > 2 || segs > 3 {
		t.Fatalf("left %d base objects, %d segments", bases, segs)
	}
	if r, _ := db.Verify(ctx); !r.OK {
		t.Fatalf("%+v", r)
	}
}

// A zombie (fenced) writer cannot commit a compaction.
func TestCompactFenced(t *testing.T) {
	st := store.NewMem()
	db1 := mustOpen(t, st)
	fill(t, db1, 5)
	db2 := mustOpen(t, st)
	defer db2.Close()
	want := dump(t, db2, "main", 0)
	if _, err := db1.Compact(ctx, CompactOptions{}); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie compaction: %v", err)
	}
	db1.Crash()
	cs, err := db2.Compact(ctx, CompactOptions{})
	if err != nil || cs.Logs == 0 {
		t.Fatalf("%+v %v", cs, err)
	}
	if got := dump(t, db2, "main", 0); got != want {
		t.Fatal("state differs")
	}
}

// Random programs run on two databases; one also compacts (and reopens)
// at random points. Every branch reads the same at its head, and reads
// at or above the base agree; Verify passes.
func TestPropCompactEqualsUncompacted(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		seed := rapid.Int64().Draw(t, "seed")
		inline := rapid.SampledFrom([]int{0, 4, 64}).Draw(t, "inline")
		stA, stB := store.NewMem(), store.NewMem()
		mk := func(st store.Store) *DB {
			db, err := OpenStore(ctx, st, quiet(), WithRand(rand.New(rand.NewSource(seed))), WithInlineMax(inline),
				WithSnapshotEvery(rapid.IntRange(1, 10).Draw(t, "snapEvery")))
			if err != nil {
				t.Fatal(err)
			}
			return db
		}
		a, b := mk(stA), mk(stB)
		defer func() { a.Close(); b.Close() }()
		both := func(f func(db *DB) error) (error, error) { return f(a), f(b) }
		both(func(db *DB) error { return db.RegisterIndex(ctx, "d", "f") })
		branches := []string{"main"}
		n := rapid.IntRange(1, 50).Draw(t, "n")
		for i := 0; i < n; i++ {
			br := rapid.SampledFrom(branches).Draw(t, "branch")
			subj := rapid.SampledFrom([]string{"", "s1", "s2"}).Draw(t, "subj")
			var o []WriteOpt
			if subj != "" {
				o = append(o, Subject(subj))
			}
			k := rapid.IntRange(0, 3).Draw(t, "k")
			size := rapid.SampledFrom([]int{1, 10, 100}).Draw(t, "size")
			val := []byte(fmt.Sprint(i, strings.Repeat("x", size)))
			switch rapid.IntRange(0, 11).Draw(t, "op") {
			case 0, 1, 2:
				both(func(db *DB) error {
					return db.Branch(br).Update(ctx, func(tx *Tx) error { return tx.Put(fmt.Sprint("k", k), val, o...) })
				})
			case 3:
				both(func(db *DB) error {
					return db.Branch(br).Update(ctx, func(tx *Tx) error { return tx.Topic("t").Publish(val, o...) })
				})
			case 4:
				both(func(db *DB) error {
					return db.Branch(br).Update(ctx, func(tx *Tx) error {
						return tx.Vector("v").Put(fmt.Sprint("x", k), []float32{float32(i % 3), 1}, o...)
					})
				})
			case 5:
				both(func(db *DB) error {
					return db.Branch(br).Update(ctx, func(tx *Tx) error {
						tx.Delete(fmt.Sprint("k", k))
						return tx.Doc("d").Put(fmt.Sprint("d", k), map[string]any{"f": i % 2, "s": string(val)}, o...)
					})
				})
			case 6:
				both(func(db *DB) error {
					return db.Branch(br).Update(ctx, func(tx *Tx) error {
						tx.Vector("v").Delete(fmt.Sprint("x", k))
						return tx.Doc("d").Delete(fmt.Sprint("d", k))
					})
				})
			case 7:
				name := fmt.Sprint("b", i)
				h, _ := a.Branch(br).Head()
				at := uint64(rapid.IntRange(0, int(h)).Draw(t, "bat"))
				ea := a.CreateBranch(ctx, name, br, at)
				if errors.Is(ea, ErrCompacted) {
					continue
				}
				if eb := b.CreateBranch(ctx, name, br, at); (ea == nil) != (eb == nil) {
					t.Fatalf("branch: %v vs %v", ea, eb)
				}
				if ea == nil {
					branches = append(branches, name)
				}
			case 8:
				h, _ := a.Branch(br).Head()
				at := uint64(rapid.IntRange(0, int(h)).Draw(t, "at"))
				if ea := a.Rewind(ctx, br, at); errors.Is(ea, ErrCompacted) {
					continue
				}
				b.Rewind(ctx, br, at)
			case 9:
				if subj != "" {
					both(func(db *DB) error { _, err := db.Shred(ctx, subj); return err })
				}
			case 10:
				if _, err := a.Compact(ctx, CompactOptions{}); err != nil {
					t.Fatal(err)
				}
			case 11:
				a.Close()
				a = mk(stA)
			}
		}
		for _, br := range branches {
			if da, db := dump(t, a, br, 0), dump(t, b, br, 0); da != db {
				t.Fatalf("branch %s differs\ncompacted:\n%s\nreference:\n%s", br, da, db)
			}
			h, _ := a.Branch(br).Head()
			at := uint64(rapid.IntRange(0, int(h)).Draw(t, "tt"))
			var errA error
			if errA = a.Branch(br).ViewAt(ctx, at, func(*Tx) error { return nil }); errA == nil {
				if da, db := dump(t, a, br, at), dump(t, b, br, at); da != db {
					t.Fatalf("branch %s @%d differs\ncompacted:\n%s\nreference:\n%s", br, at, da, db)
				}
			} else if !errors.Is(errA, ErrCompacted) {
				t.Fatal(errA)
			}
			d1, _, _ := a.LensDigests(br)
			d2, _ := a.RebuildDigests(br)
			if fmt.Sprint(d1) != fmt.Sprint(d2) {
				t.Fatalf("branch %s: lens != fold", br)
			}
		}
		if r, err := a.Verify(ctx); err != nil || !r.OK {
			t.Fatalf("verify %+v %v", r, err)
		}
	})
}
