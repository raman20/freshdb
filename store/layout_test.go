package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDetectLayout(t *testing.T) {
	base := t.TempDir()
	mk := func(name string, files ...string) string {
		d := filepath.Join(base, name)
		os.MkdirAll(d, 0o755)
		for _, f := range files {
			p := filepath.Join(d, f)
			os.MkdirAll(filepath.Dir(p), 0o755)
			if f[len(f)-1] != '/' {
				os.WriteFile(p, []byte("x"), 0o644)
			}
		}
		return d
	}
	cases := []struct {
		dir  string
		want Layout
	}{
		{filepath.Join(base, "missing"), LayoutNone},
		{mk("empty"), LayoutNone},
		{mk("emptytmp", ".tmp/"), LayoutNone},
		{mk("tmpfile", ".tmp/x"), LayoutFiles},
		{mk("files", "log/main/0001"), LayoutFiles},
		{mk("journal", jrnFile), LayoutJournal},
		{mk("both", jrnFile, "log/x"), LayoutJournal},
	}
	for _, c := range cases {
		got, err := DetectLayout(c.dir)
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", filepath.Base(c.dir), got, err, c.want)
		}
	}
}

func TestOpenLocalDefaultAndExisting(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	s, err := Open(d)
	if err != nil {
		t.Fatal(err)
	}
	_, isJ := s.(*Journal)
	if isJ != (runtime.GOOS != "windows" && journalSupported) {
		t.Fatalf("new dir opened as %T", s)
	}
	closeStore(s)

	// A files-layout directory keeps its layout under the default URL.
	f := t.TempDir()
	fs1, err := Open("fs://" + f)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs1.PutIfAbsent(ctx, "log/a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{f, "file://" + f} {
		s, err := Open(u)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := s.(*FS); !ok {
			t.Fatalf("%s: existing files dir opened as %T", u, s)
		}
		if v, err := s.Get(ctx, "log/a"); err != nil || string(v) != "1" {
			t.Fatalf("%s: get: %q %v", u, v, err)
		}
	}
	if !journalSupported {
		return
	}
	// Explicit schemes refuse the other layout (invariant L1).
	if _, err := Open("journal://" + f); err == nil {
		t.Fatal("journal:// opened a files directory")
	}
	if _, err := Open("fs://" + d); err == nil {
		t.Fatal("fs:// opened a journal directory")
	}
}

// snapshot reads every object of s.
func snapshot(t *testing.T, s Store) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	keys, err := Keys(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string][]byte{}
	for _, k := range keys {
		v, err := s.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		m[k] = v
	}
	return m
}

func sameObjects(t *testing.T, what string, a, b map[string][]byte) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("%s: %d vs %d objects", what, len(a), len(b))
	}
	for k, v := range a {
		if !bytes.Equal(b[k], v) {
			t.Fatalf("%s: object %s differs", what, k)
		}
	}
}

func fillFiles(t *testing.T, dir string, rng *rand.Rand, n int) map[string][]byte {
	t.Helper()
	s, err := NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.SetUnsafeNoSync(true)
	ctx := context.Background()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("p%d/q%d/k%05d", rng.Intn(3), rng.Intn(3), i)
		v := make([]byte, rng.Intn(3000))
		rng.Read(v)
		if err := s.PutIfAbsent(ctx, k, v); err != nil {
			t.Fatal(err)
		}
	}
	if rng.Intn(2) == 0 { // deleted objects must not reappear
		s.Delete(ctx, fmt.Sprintf("p0/q0/k%05d", 0))
	}
	return snapshot(t, s)
}

// Property: random object sets survive files → journal → files unchanged,
// with the original kept as a backup each time.
func TestMigrateRoundTrip(t *testing.T) {
	if !journalSupported {
		t.Skip("journal unsupported")
	}
	ctx := context.Background()
	for seed := int64(1); seed <= 8; seed++ {
		rng := rand.New(rand.NewSource(seed))
		dir := filepath.Join(t.TempDir(), "db")
		want := fillFiles(t, dir, rng, rng.Intn(200))
		if err := Migrate(ctx, dir, LayoutJournal, nil); err != nil {
			t.Fatal(err)
		}
		if l, _ := DetectLayout(dir); l != LayoutJournal {
			t.Fatalf("seed %d: layout %q after migrate", seed, l)
		}
		j, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		sameObjects(t, "journal", want, snapshot(t, j))
		closeStore(j)
		b, _ := NewFS(dir + ".bak-files")
		sameObjects(t, "backup", want, snapshot(t, b))
		if err := Migrate(ctx, dir, LayoutJournal, nil); err != nil { // no-op
			t.Fatal(err)
		}
		if err := Migrate(ctx, dir, LayoutFiles, nil); err != nil {
			t.Fatal(err)
		}
		f, _ := Open(dir)
		if _, ok := f.(*FS); !ok {
			t.Fatalf("seed %d: back-migrated dir opens as %T", seed, f)
		}
		sameObjects(t, "files again", want, snapshot(t, f))
		if !exists(dir + ".bak-journal") {
			t.Fatal("journal backup missing")
		}
	}
}

var errCrash = errors.New("injected crash")

// Stopping Migrate after any step and running it again converges to the
// same result (the resume rules of spec/FreshMigrate.tla).
func TestMigrateResumeAfterEveryStep(t *testing.T) {
	if !journalSupported {
		t.Skip("journal unsupported")
	}
	ctx := context.Background()
	defer func() { migrateHook = nil }()
	for _, step := range []string{"clean", "copy", "verify", "sync", "r1", "f1", "bak", "f2", "final"} {
		for crashes := 1; crashes <= 2; crashes++ {
			dir := filepath.Join(t.TempDir(), "db")
			want := fillFiles(t, dir, rand.New(rand.NewSource(int64(len(step)))), 50)
			n := 0
			migrateHook = func(s string) error {
				if s == step && n < crashes {
					n++
					return errCrash
				}
				return nil
			}
			for i := 0; ; i++ {
				err := Migrate(ctx, dir, LayoutJournal, nil)
				if err == nil {
					break
				}
				if !errors.Is(err, errCrash) || i > crashes {
					t.Fatalf("step %s: %v", step, err)
				}
				// Invariants M1/M2 hold at every stop.
				if !exists(dir) && !exists(dir+".migrated") {
					t.Fatalf("step %s: neither dir nor .migrated exists", step)
				}
			}
			j, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := j.(*Journal); !ok {
				t.Fatalf("step %s: %T", step, j)
			}
			sameObjects(t, "after resume at "+step, want, snapshot(t, j))
			closeStore(j)
			b, _ := NewFS(dir + ".bak-files")
			sameObjects(t, "backup after "+step, want, snapshot(t, b))
			if exists(dir+".migrate-tmp") || exists(dir+".migrated") {
				t.Fatalf("step %s: leftovers", step)
			}
		}
	}
}
