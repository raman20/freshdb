//go:build unix

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func mustJournal(t *testing.T, d string) *Journal {
	t.Helper()
	j, err := NewJournal(d)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestJournalConformance(t *testing.T) { conformance(t, mustJournal(t, t.TempDir())) }

// Two instances on one directory behave like two processes: flock-guarded
// create-only, and each sees the other's puts and deletes.
func TestJournalTwoInstances(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	a, b := mustJournal(t, d), mustJournal(t, d)
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := []*Journal{a, b}[i%2]
			if s.PutIfAbsent(ctx, "race/k", []byte{byte(i)}) == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
			s.PutIfAbsent(ctx, fmt.Sprintf("seg/%03d", i), []byte(fmt.Sprint(i)))
		}(i)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d creators won", wins)
	}
	for _, s := range []*Journal{a, b} {
		l, err := s.List(ctx, "seg/")
		if err != nil || len(l) != 32 {
			t.Fatalf("list %d %v", len(l), err)
		}
		if v, err := s.Get(ctx, "seg/007"); err != nil || string(v) != "7" {
			t.Fatalf("get %q %v", v, err)
		}
	}
	if err := a.Delete(ctx, "seg/007"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, "seg/007"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete not seen by other instance: %v", err)
	}
	if _, err := b.GetRange(ctx, "seg/007", 0, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("range after delete by other instance: %v", err)
	}
	if err := b.PutIfAbsent(ctx, "seg/007", []byte("again")); err != nil {
		t.Fatalf("re-create after delete: %v", err)
	}
	if v, err := a.Get(ctx, "seg/007"); err != nil || string(v) != "again" {
		t.Fatalf("re-created %q %v", v, err)
	}
}

// Reopen keeps live objects, drops deleted ones, and a deleted object's
// bytes are gone from the file (crypto-shredded keys must not linger).
func TestJournalReopenAndErasure(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	j := mustJournal(t, d)
	secret := bytes.Repeat([]byte("TOPSECRETKEY"), 50)
	j.PutIfAbsent(ctx, "keys/u1", secret)
	j.PutIfAbsent(ctx, "keys/u2", []byte("keep"))
	if err := j.Delete(ctx, "keys/u1"); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(d, jrnFile))
	if bytes.Contains(raw, []byte("TOPSECRETKEY")) {
		t.Fatal("deleted bytes still in the journal file")
	}
	j2 := mustJournal(t, d)
	if _, err := j2.Get(ctx, "keys/u1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted object back after reopen: %v", err)
	}
	if v, err := j2.Get(ctx, "keys/u2"); err != nil || string(v) != "keep" {
		t.Fatalf("live object lost: %q %v", v, err)
	}
}

// A crash between the delete record and the erase is finished on open.
func TestJournalInterruptedDelete(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	j := mustJournal(t, d)
	j.PutIfAbsent(ctx, "keys/u1", []byte("SECRET-SECRET-SECRET"))
	unlock, err := j.lockWrite()
	if err != nil {
		t.Fatal(err)
	}
	if err := j.appendRec(j.record(j.end, recDel, "keys/u1", nil)); err != nil { // no erase: "crash"
		t.Fatal(err)
	}
	unlock()
	j2 := mustJournal(t, d)
	if _, err := j2.Get(ctx, "keys/u1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	raw, _ := os.ReadFile(filepath.Join(d, jrnFile))
	if bytes.Contains(raw, []byte("SECRET")) {
		t.Fatal("open did not finish the interrupted erase")
	}
}

// Torn tails (a crash mid-append) are ignored and overwritten.
func TestJournalTornTail(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	j := mustJournal(t, d)
	j.PutIfAbsent(ctx, "a", []byte("1"))
	full := j.record(j.end, recPut, "b", bytes.Repeat([]byte{7}, 100))
	for _, cut := range []int{3, 12, 20, len(full) - 1} {
		f, _ := os.OpenFile(filepath.Join(d, jrnFile), os.O_RDWR, 0)
		f.WriteAt(full[:cut], j.end) // a partial record past the end
		f.Close()
		j2 := mustJournal(t, d)
		if _, err := j2.Get(ctx, "b"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cut %d: torn record visible: %v", cut, err)
		}
		if v, err := j2.Get(ctx, "a"); err != nil || string(v) != "1" {
			t.Fatalf("cut %d: %q %v", cut, v, err)
		}
	}
	j3 := mustJournal(t, d)
	if err := j3.PutIfAbsent(ctx, "c", []byte("3")); err != nil {
		t.Fatal(err)
	}
	j4 := mustJournal(t, d)
	if v, err := j4.Get(ctx, "c"); err != nil || string(v) != "3" {
		t.Fatalf("append after torn tail: %q %v", v, err)
	}
	if _, err := j4.Get(ctx, "b"); !errors.Is(err, ErrNotFound) {
		t.Fatal("torn record resurrected")
	}
}

func TestJournalNotAJournal(t *testing.T) {
	d := t.TempDir()
	os.WriteFile(filepath.Join(d, jrnFile), []byte("something else entirely, not a journal"), 0o644)
	if _, err := NewJournal(d); err == nil {
		t.Fatal("opened a foreign file")
	}
}
