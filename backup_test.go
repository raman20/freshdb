package freshdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/freshdb/store"
)

// checkPrefix: the restored database holds k0..k(m-1) with their values
// and nothing after (writes were sequential, so any consistent cut is a
// prefix); m must be at least min.
func checkPrefix(t *testing.T, db *DB, min int) int {
	t.Helper()
	m := 0
	for ; ; m++ {
		v, err := db.Get(ctx, fmt.Sprint("k", m))
		if errors.Is(err, ErrNotFound) {
			break
		}
		if err != nil || string(v) != fmt.Sprint("v", m) {
			t.Fatalf("k%d: %q %v", m, v, err)
		}
	}
	for j := m + 1; j < m+20; j++ {
		if _, err := db.Get(ctx, fmt.Sprint("k", j)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("gap: k%d missing but k%d present", m, j)
		}
	}
	if m < min {
		t.Fatalf("restored %d keys, but %d were acknowledged before the backup started", m, min)
	}
	return m
}

func restoreOpen(t *testing.T, bk store.Store) (*DB, *RestoreReport) {
	t.Helper()
	dst := store.NewMem()
	rep, err := Restore(ctx, bk, dst)
	if err != nil {
		t.Fatal(err)
	}
	db := mustOpen(t, dst)
	t.Cleanup(func() { db.Close() })
	if vr, err := db.Verify(ctx); err != nil || !vr.OK {
		t.Fatalf("verify restored: %+v %v", vr, err)
	}
	return db, rep
}

// TestBackupDrill: online backup while a writer, compaction and GC run;
// the restore is a consistent prefix holding every write acknowledged
// before the backup started; an incremental backup copies only new
// objects; a shred between backups leaves the next backup.
func TestBackupDrill(t *testing.T) {
	for _, mk := range []func(t *testing.T) store.Store{
		func(*testing.T) store.Store { return store.NewMem() },
		func(t *testing.T) store.Store {
			j, err := store.NewJournal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { j.Close() })
			return j
		},
	} {
		src := mk(t)
		db := mustOpen(t, src)
		if err := db.Put(ctx, "secret", []byte("pii"), Subject("alice")); err != nil {
			t.Fatal(err)
		}
		var acked atomic.Int64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { // sequential writer
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := db.Put(ctx, fmt.Sprint("k", i), []byte(fmt.Sprint("v", i))); err != nil {
					t.Error(err)
					return
				}
				acked.Store(int64(i + 1))
			}
		}()
		go func() { // compaction and GC with no grace: deletes as early as possible
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				case <-time.After(2 * time.Millisecond):
				}
				var err error
				if n%2 == 0 {
					_, err = db.Compact(ctx, CompactOptions{})
				} else {
					_, err = db.GC(ctx, GCOptions{})
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
		for acked.Load() < 300 {
			time.Sleep(time.Millisecond)
		}
		bk := store.NewMem()
		min := int(acked.Load())
		min1 := min
		rep, err := db.Backup(ctx, bk)
		if err != nil {
			t.Fatal(err)
		}
		r, _ := restoreOpen(t, bk)
		m1 := checkPrefix(t, r, min)

		if _, err := db.Shred(ctx, "alice"); err != nil {
			t.Fatal(err)
		}
		for acked.Load() < int64(m1+300) {
			time.Sleep(time.Millisecond)
		}
		min = int(acked.Load())
		rep2, err := db.Backup(ctx, bk)
		if err != nil {
			t.Fatal(err)
		}
		close(stop)
		wg.Wait()
		if rep2.Copied >= rep2.Objects {
			t.Errorf("incremental backup copied %d of %d objects", rep2.Copied, rep2.Objects)
		}
		r2, _ := restoreOpen(t, bk)
		m2 := checkPrefix(t, r2, min)
		if v, err := r2.Get(ctx, "secret"); !errors.Is(err, ErrShredded) && !errors.Is(err, ErrNotFound) {
			t.Fatalf("shredded subject readable after restore of a later backup: %q %v", v, err)
		}
		if _, err := bk.Get(ctx, "keys/dek/"+SubjectHash("alice")); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("shredded subject's key still in the updated backup: %v", err)
		}
		t.Logf("backup 1: %d objects %d bytes, %d keys restored (≥%d acked); backup 2: copied %d/%d, removed %d, %d keys",
			rep.Objects, rep.Bytes, m1, min1, rep2.Copied, rep2.Objects, rep2.Removed, m2)
		db.Close()
	}
}

// blockStore blocks the first PutIfAbsent until released.
type blockStore struct {
	store.Store
	once            sync.Once
	entered, resume chan struct{}
}

func (b *blockStore) Keys(c context.Context) ([]string, error) { return store.Keys(c, b.Store) }
func (f *failAfter) Keys(c context.Context) ([]string, error)  { return store.Keys(c, f.Store) }

func (b *blockStore) PutIfAbsent(c context.Context, k string, v []byte) error {
	b.once.Do(func() { close(b.entered); <-b.resume })
	return b.Store.PutIfAbsent(c, k, v)
}

// TestBackupHoldsDeletes: a compaction that runs in the middle of a
// backup and deletes everything it can must not break the backup.
func TestBackupHoldsDeletes(t *testing.T) {
	src := store.NewMem()
	db := mustOpen(t, src)
	defer db.Close()
	for i := 0; i < 200; i++ {
		if err := db.Put(ctx, fmt.Sprint("k", i%20), []byte(fmt.Sprint("v", i))); err != nil {
			t.Fatal(err)
		}
	}
	bk := &blockStore{Store: store.NewMem(), entered: make(chan struct{}), resume: make(chan struct{})}
	done := make(chan error)
	go func() { _, err := db.Backup(ctx, bk); done <- err }()
	select {
	case <-bk.entered:
	case err := <-done:
		t.Fatalf("backup ended before copying: %v", err)
	}
	before, _ := store.Keys(ctx, src)
	cs, err := db.Compact(ctx, CompactOptions{})
	if err != nil || cs.Deleted == 0 {
		t.Fatalf("compaction during backup: %+v %v (want deletes)", cs, err)
	}
	if mid, _ := store.Keys(ctx, src); len(mid) < len(before) {
		t.Fatalf("objects deleted during the backup: %d -> %d", len(before), len(mid))
	}
	close(bk.resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	after, _ := store.Keys(ctx, src)
	if len(after) >= len(before) {
		t.Fatalf("deferred deletes not applied after the backup: %d -> %d", len(before), len(after))
	}
	r, _ := restoreOpen(t, bk.Store)
	for i := 180; i < 200; i++ {
		if v, err := r.Get(ctx, fmt.Sprint("k", i%20)); err != nil || string(v) != fmt.Sprint("v", i) {
			t.Fatalf("k%d: %q %v", i%20, v, err)
		}
	}
}

// failAfter fails every PutIfAbsent after n.
type failAfter struct {
	store.Store
	n atomic.Int64
}

func (f *failAfter) PutIfAbsent(c context.Context, k string, v []byte) error {
	if f.n.Add(-1) < 0 {
		return errors.New("injected: destination failed")
	}
	return f.Store.PutIfAbsent(c, k, v)
}

func TestBackupFailures(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	for i := 0; i < 50; i++ {
		db.Put(ctx, fmt.Sprint("k", i), []byte("v"))
	}
	// interrupted: no record, restore refuses
	bad := &failAfter{Store: store.NewMem()}
	bad.n.Store(5)
	if _, err := db.Backup(ctx, bad); err == nil {
		t.Fatal("backup into a failing destination succeeded")
	}
	if _, err := Restore(ctx, bad.Store, store.NewMem()); err == nil || !strings.Contains(err.Error(), "did not finish") {
		t.Fatalf("restore of an interrupted backup: %v", err)
	}
	// complete, then an interrupted update: the old record is gone too
	bk := store.NewMem()
	if _, err := db.Backup(ctx, bk); err != nil {
		t.Fatal(err)
	}
	db.Put(ctx, "more", []byte("x"))
	upd := &failAfter{Store: bk}
	upd.n.Store(1)
	if _, err := db.Backup(ctx, upd); err == nil {
		t.Fatal("want failure")
	}
	if _, err := Restore(ctx, bk, store.NewMem()); err == nil {
		t.Fatal("restore of a half-updated backup must refuse")
	}
	if _, err := db.Backup(ctx, bk); err != nil { // rerun completes it
		t.Fatal(err)
	}
	// corruption: one flipped byte anywhere fails the digest
	keys, _ := store.Keys(ctx, bk)
	var victim string
	for _, k := range keys {
		if strings.HasPrefix(k, "log/") {
			victim = k
		}
	}
	v, _ := bk.Get(ctx, victim)
	v = append([]byte{}, v...)
	v[len(v)/2] ^= 1
	bk.Delete(ctx, victim)
	bk.PutIfAbsent(ctx, victim, v)
	if _, err := Restore(ctx, bk, store.NewMem()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("restore of a corrupted backup: %v", err)
	}
	// non-empty destination; another database's backup
	full := store.NewMem()
	full.PutIfAbsent(ctx, "x", []byte("y"))
	if _, err := Restore(ctx, bk, full); err == nil {
		t.Fatal("restore into a non-empty destination")
	}
	if _, err := db.Backup(ctx, full); err == nil {
		t.Fatal("backup into a non-empty non-database destination")
	}
	other := mustOpen(t, store.NewMem(), WithKEK(make([]byte, 32)))
	defer other.Close()
	ob := store.NewMem()
	mine := mustOpen(t, store.NewMem(), WithKEK(append(make([]byte, 31), 1)))
	defer mine.Close()
	if _, err := other.Backup(ctx, ob); err != nil {
		t.Fatal(err)
	}
	if _, err := mine.Backup(ctx, ob); err == nil {
		t.Fatal("backup over another database's backup")
	}
}

// TestRestorePITR: restore, then rewind to a log position inside the
// backup's history.
func TestRestorePITR(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	var at uint64
	for i := 0; i < 100; i++ {
		db.Put(ctx, fmt.Sprint("k", i), []byte(fmt.Sprint("v", i)))
		if i == 41 {
			at = db.Branches()["main"]
		}
	}
	bk := store.NewMem()
	if _, err := db.Backup(ctx, bk); err != nil {
		t.Fatal(err)
	}
	r, _ := restoreOpen(t, bk)
	if err := r.Rewind(ctx, "main", at); err != nil {
		t.Fatal(err)
	}
	if n := checkPrefix(t, r, 42); n != 42 {
		t.Fatalf("PITR to k41: %d keys", n)
	}
}
