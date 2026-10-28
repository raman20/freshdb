//go:build unix

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func fillJournal(t *testing.T, d string) (map[string][]byte, int64) {
	t.Helper()
	ctx := context.Background()
	j := mustJournal(t, d)
	defer j.Close()
	want := map[string][]byte{}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("o/%04d", i)
		v := bytes.Repeat([]byte{byte(i)}, 400000+i)
		if err := j.PutIfAbsent(ctx, k, v); err != nil {
			t.Fatal(err)
		}
		if i%4 != 0 {
			if err := j.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
		} else {
			want[k] = v
		}
	}
	fi, _ := os.Stat(filepath.Join(d, jrnFile))
	return want, fi.Size()
}

func checkJournal(t *testing.T, d string, want map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	j := mustJournal(t, d)
	defer j.Close()
	keys, _ := j.Keys(ctx)
	if len(keys) != len(want) {
		t.Fatalf("%d keys, want %d", len(keys), len(want))
	}
	for k, v := range want {
		if got, err := j.Get(ctx, k); err != nil || !bytes.Equal(got, v) {
			t.Fatalf("%s: %v", k, err)
		}
	}
	// still writable after a shrink
	if err := j.PutIfAbsent(ctx, "new", []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func TestShrinkJournal(t *testing.T) {
	d := t.TempDir()
	want, size := fillJournal(t, d)
	st, err := ShrinkJournal(d)
	if err != nil {
		t.Fatal(err)
	}
	if st.Objects != len(want) || st.SizeBefore != size || st.SizeAfter >= st.SizeBefore || st.DeadRecsBefore != 150 {
		t.Fatalf("%+v (size %d)", st, size)
	}
	checkJournal(t, d, want)
	if _, err := os.Stat(filepath.Join(d, jrnShrink)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file left: %v", err)
	}
}

// Shrink refuses while the journal is open; an open waits for none.
func TestShrinkJournalInUse(t *testing.T) {
	d := t.TempDir()
	want, _ := fillJournal(t, d)
	j := mustJournal(t, d)
	if _, err := ShrinkJournal(d); err == nil {
		t.Fatal("shrink of an open journal must fail")
	}
	j.Close()
	if _, err := ShrinkJournal(d); err != nil {
		t.Fatal(err)
	}
	checkJournal(t, d, want)
}

// A crash before the rename leaves the old journal and a temp copy, which
// the next open removes.
func TestShrinkJournalCrashBeforeRename(t *testing.T) {
	d := t.TempDir()
	want, size := fillJournal(t, d)
	shrinkHook = func(s string) error {
		if s == "before-rename" {
			// simulate the crash: keep the temp file
			b, _ := os.ReadFile(filepath.Join(d, jrnShrink))
			os.WriteFile(filepath.Join(d, jrnShrink+".keep"), b, 0o644)
			return errors.New("injected crash")
		}
		return nil
	}
	defer func() { shrinkHook = func(string) error { return nil } }()
	if _, err := ShrinkJournal(d); err == nil {
		t.Fatal("want the injected error")
	}
	if err := os.Rename(filepath.Join(d, jrnShrink+".keep"), filepath.Join(d, jrnShrink)); err != nil {
		t.Fatal(err)
	}
	if l, _ := DetectLayout(d); l != LayoutJournal {
		t.Fatalf("layout %v", l)
	}
	if fi, _ := os.Stat(filepath.Join(d, jrnFile)); fi.Size() < size {
		t.Fatal("journal replaced before the rename")
	}
	checkJournal(t, d, want)
	if _, err := os.Stat(filepath.Join(d, jrnShrink)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("open must remove the leftover copy: %v", err)
	}
}

// A directory holding only journal.lock (crash right after creating it) is
// still a new journal, not a files-layout directory.
func TestJournalLockOnlyDir(t *testing.T) {
	d := t.TempDir()
	os.WriteFile(filepath.Join(d, jrnLock), nil, 0o644)
	if l, err := DetectLayout(d); err != nil || l != LayoutNone {
		t.Fatalf("%v %v", l, err)
	}
	mustJournal(t, d).Close()
}
