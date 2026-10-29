package freshdb

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/raman20/freshdb/store"
)

// Forward-compatibility refusal: an object stamped with a version this
// build does not know (written by a newer freshdb) makes open fail with
// ErrVersion and an "upgrade the binary" message, and changes nothing.
func TestNewerFormatsRefused(t *testing.T) {
	setup := func(t *testing.T, compact bool) store.Store {
		st := store.NewMem()
		db := mustOpen(t, st)
		for i := 0; i < 20; i++ {
			db.Put(ctx, "k"+strings.Repeat("x", i), make([]byte, 100))
		}
		if compact {
			if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		db.Close()
		return st
	}
	latest := func(st store.Store, prefix string) string {
		ks, _ := st.List(ctx, prefix)
		return ks[len(ks)-1]
	}
	rewrite := func(st store.Store, k string, f func([]byte) []byte) {
		b, _ := st.Get(ctx, k)
		st.Delete(ctx, k)
		st.PutIfAbsent(ctx, k, f(b))
	}
	t.Run("manifest", func(t *testing.T) {
		st := setup(t, false)
		k := latest(st, "manifest/")
		rewrite(st, k, func(b []byte) []byte {
			m, _ := decodeManifest(b)
			m.Format = 3
			return encodeManifest(m)
		})
		_, err := OpenStore(ctx, st, quiet())
		if !errors.Is(err, ErrVersion) || !strings.Contains(err.Error(), "upgrade the binary") {
			t.Fatalf("manifest format 3: %v", err)
		}
		if latest(st, "manifest/") != k {
			t.Fatal("a refused open wrote a manifest")
		}
	})
	t.Run("base", func(t *testing.T) {
		st := setup(t, true)
		ks, _ := st.List(ctx, "base/L1/")
		var bk string
		for _, k := range ks {
			if strings.HasSuffix(k, ".base") {
				bk = k
			}
		}
		if bk == "" {
			t.Fatal("no base")
		}
		rewrite(st, bk, func(b []byte) []byte { b[7] = '2'; return b })
		_, err := OpenStore(ctx, st, quiet())
		if !errors.Is(err, ErrVersion) {
			t.Fatalf("base version 2: %v", err)
		}
	})
	t.Run("journal", func(t *testing.T) {
		d := t.TempDir()
		j, err := store.NewJournal(d)
		if err != nil {
			t.Skip(err) // no journal on this platform
		}
		j.Close()
		p := filepath.Join(d, "journal.fj")
		b, _ := os.ReadFile(p)
		b[6] = '2'
		os.WriteFile(p, b, 0o644)
		_, err = Open(ctx, "journal://"+d, quiet())
		if !errors.Is(err, ErrVersion) || !strings.Contains(err.Error(), "upgrade the binary") {
			t.Fatalf("journal version 2: %v", err)
		}
		if c, _ := os.ReadFile(p); string(c) != string(b) {
			t.Fatal("refused open changed the journal")
		}
	})
}
