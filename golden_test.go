package freshdb_test

// Golden files (docs/compatibility.md). testdata/golden/<version>/db is a
// database written by that version's code with goldenWorkload, and dump.txt
// what that version read back from it.
//
//   - TestGoldenRead: this build opens a copy of every golden database and
//     must read exactly dump.txt (old formats stay readable).
//   - TestGoldenBytes: this build rewrites the "main" golden database and
//     every object must be byte-identical (the on-storage format cannot
//     change silently). An intended format change regenerates it with
//     FRESHDB_UPDATE_GOLDEN=1, bumps the version stamp, and keeps the old
//     directory for TestGoldenRead.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/raman20/freshdb"
	"github.com/raman20/freshdb/store"
)

var gctx = context.Background()

// goldenMain writes a current-format golden database: the workload, an
// HNSW graph (saved on close), and with compact a compaction (manifest
// format 2, base, value chunk) followed by more writes.
func goldenMain(t *testing.T, dir string, compact bool) string {
	t.Helper()
	db, err := freshdb.Open(gctx, "fs://"+dir, goldenOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	if err := goldenWorkload(gctx, db); err != nil {
		t.Fatal(err)
	}
	if err := db.EnableANN(gctx, "emb", freshdb.ANNOptions{}); err != nil {
		t.Fatal(err)
	}
	if compact {
		if _, err := db.Compact(gctx, freshdb.CompactOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	err = db.Update(gctx, func(tx *freshdb.Tx) error {
		tx.Put("after", bytes.Repeat([]byte("z"), 100))
		return tx.Vector("emb").Put("y1", []float32{1, 2, 3, 4})
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err := goldenDump(gctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestGoldenRead(t *testing.T) {
	vers, _ := filepath.Glob("testdata/golden/*/dump.txt")
	if len(vers) < 2 {
		t.Fatalf("golden versions missing: %v", vers)
	}
	layouts := []string{"files"}
	if runtime.GOOS != "windows" {
		layouts = append(layouts, "journal") // after `freshdb migrate`
	}
	for _, dump := range vers {
		for _, layout := range layouts {
			ver := filepath.Base(filepath.Dir(dump))
			t.Run(ver+"/"+layout, func(t *testing.T) { goldenRead(t, dump, layout) })
		}
	}
}

func goldenRead(t *testing.T, dump, layout string) {
	ver := filepath.Base(filepath.Dir(dump))
	want, _ := os.ReadFile(dump)
	dir := filepath.Join(t.TempDir(), "db")
	copyDir(t, filepath.Join(filepath.Dir(dump), "db"), dir)
	if layout == "journal" {
		if err := store.Migrate(gctx, dir, store.LayoutJournal, t.Logf); err != nil {
			t.Fatal(err)
		}
	}
	db, err := freshdb.Open(gctx, "file://"+dir, goldenOpts()...)
	if err != nil {
		t.Fatalf("open %s golden database: %v", ver, err)
	}
	defer db.Close()
	got, err := goldenDump(gctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("%s golden database reads differently:\n%s", ver, diffLines(string(want), got))
	}
}

func TestGoldenBytes(t *testing.T) {
	for _, compact := range []bool{false, true} {
		gold := "testdata/golden/main"
		if compact {
			gold += "-compacted"
		}
		t.Run(filepath.Base(gold), func(t *testing.T) { goldenBytes(t, gold, compact) })
	}
}

func goldenBytes(t *testing.T, gold string, compact bool) {
	dir := t.TempDir()
	dump := goldenMain(t, dir, compact)
	if os.Getenv("FRESHDB_UPDATE_GOLDEN") == "1" {
		os.RemoveAll(gold)
		copyDir(t, dir, filepath.Join(gold, "db"))
		os.WriteFile(filepath.Join(gold, "dump.txt"), []byte(dump), 0o644)
		t.Logf("rewrote %s", gold)
		return
	}
	want, got := tree(t, filepath.Join(gold, "db")), tree(t, dir)
	for k, w := range want {
		if strings.HasPrefix(k, "ann/") && runtime.GOARCH != "amd64" {
			// float results of the HNSW build may differ in the last bit
			// where the compiler fuses multiply-adds (arm64): a cache only
			delete(got, k)
			continue
		}
		g, ok := got[k]
		switch {
		case !ok:
			t.Errorf("%s: no longer written", k)
		case !bytes.Equal(g, w):
			t.Errorf("%s: bytes changed (%d → %d bytes); on-storage format changed? see docs/compatibility.md", k, len(w), len(g))
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("%s: new object not in the golden database", k)
		}
	}
	if w, _ := os.ReadFile(filepath.Join(gold, "dump.txt")); string(w) != dump {
		t.Errorf("reader-visible state changed:\n%s", diffLines(string(w), dump))
	}
}

// TestGoldenWorkloadInSync: the v0.1.0 generator runs the same workload.
func TestGoldenWorkloadInSync(t *testing.T) {
	a, _ := os.ReadFile("golden_test.go")
	b, _ := os.ReadFile("testdata/golden/gen/workload.go")
	cut := func(s []byte) string {
		x := string(s)
		i, j := strings.LastIndex(x, "// --- workload begin ---"), strings.LastIndex(x, "// --- workload end ---")
		if i < 0 || j < 0 {
			return ""
		}
		return x[i:j]
	}
	if cut(a) == "" || cut(a) != cut(b) {
		t.Fatal("golden_test.go and testdata/golden/gen/workload.go workloads differ")
	}
}

func tree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	m := map[string][]byte{}
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		m[filepath.ToSlash(rel)] = b
		return nil
	})
	return m
}

func copyDir(t *testing.T, from, to string) {
	t.Helper()
	for k, b := range tree(t, from) {
		p := filepath.Join(to, filepath.FromSlash(k))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func diffLines(want, got string) string {
	w, g := strings.Split(want, "\n"), strings.Split(got, "\n")
	var out []string
	for i := 0; i < len(w) || i < len(g); i++ {
		var a, b string
		if i < len(w) {
			a = w[i]
		}
		if i < len(g) {
			b = g[i]
		}
		if a != b {
			out = append(out, fmt.Sprintf("line %d\n  want %s\n  got  %s", i+1, a, b))
		}
		if len(out) > 10 {
			break
		}
	}
	return strings.Join(out, "\n")
}

// --- workload begin ---
// (testdata/golden/gen/workload.go holds an identical copy; it builds the
// v0.1.0 golden database with the v0.1.0 code. Only API present in v0.1.0.)

func goldenOpts() []freshdb.Option {
	var now int64 = 1_800_000_000_000_000_000
	return []freshdb.Option{
		freshdb.WithClock(func() int64 { now += 1_000_000; return now }),
		freshdb.WithRand(rand.New(rand.NewSource(1))),
		freshdb.WithSnapshotEvery(10),
		freshdb.WithLogf(func(string, ...any) {}),
	}
}

func goldenWorkload(ctx context.Context, db *freshdb.DB) error {
	for i := 0; i < 40; i++ {
		err := db.Update(ctx, func(tx *freshdb.Tx) error {
			v := []byte(fmt.Sprint("v", i))
			if i%3 == 0 {
				v = bytes.Repeat(v, 40) // over 64 B: kept out of line
			}
			if err := tx.Put(fmt.Sprintf("k%02d", i%25), v); err != nil {
				return err
			}
			if i%7 == 6 {
				if err := tx.Delete(fmt.Sprintf("k%02d", i%25-1)); err != nil {
					return err
				}
			}
			if err := tx.Topic("events").Publish([]byte(fmt.Sprint("e", i))); err != nil {
				return err
			}
			doc := map[string]any{"team": fmt.Sprint("t", i%2), "n": i}
			if err := tx.Doc("users").Put(fmt.Sprint("u", i%5), doc); err != nil {
				return err
			}
			return tx.Vector("emb").Put(fmt.Sprint("x", i%10), []float32{float32(i), float32(i % 3), 1, -float32(i % 5)})
		})
		if err != nil {
			return err
		}
		if i == 10 {
			if err := db.RegisterIndex(ctx, "users", "team"); err != nil {
				return err
			}
		}
	}
	err := db.Update(ctx, func(tx *freshdb.Tx) error {
		tx.Put("alice/email", []byte("alice@example.com"), freshdb.Subject("alice"))
		tx.Put("bob/email", []byte("bob@example.com"), freshdb.Subject("bob"))
		return tx.Topic("events").Publish([]byte("alice-event"), freshdb.Subject("alice"))
	})
	if err != nil {
		return err
	}
	if _, err := db.Shred(ctx, "alice"); err != nil {
		return err
	}
	err = db.Update(ctx, func(tx *freshdb.Tx) error {
		g := tx.Topic("events").Group("g1")
		ms, err := g.Fetch(5)
		if err != nil || len(ms) == 0 {
			return fmt.Errorf("fetch: %d %v", len(ms), err)
		}
		return g.Ack(ms[len(ms)-1])
	})
	if err != nil {
		return err
	}
	if err := db.CreateBranch(ctx, "exp", "main", 20); err != nil {
		return err
	}
	return db.Branch("exp").Update(ctx, func(tx *freshdb.Tx) error {
		tx.Put("exp-only", []byte("x"))
		return tx.Delete("k01")
	})
}

// goldenDump prints everything a reader can see, in a fixed order.
func goldenDump(ctx context.Context, db *freshdb.DB) (string, error) {
	var b strings.Builder
	for _, br := range []string{"main", "exp"} {
		head, err := db.Branch(br).Head()
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "branch %s head %d\n", br, head)
		err = db.Branch(br).View(ctx, func(tx *freshdb.Tx) error {
			kvs, err := tx.Scan("")
			if err != nil {
				return err
			}
			for _, kv := range kvs {
				fmt.Fprintf(&b, "kv %s = %q\n", kv.Key, kv.Value)
			}
			for _, k := range []string{"alice/email", "bob/email"} {
				v, err := tx.Get(k)
				fmt.Fprintf(&b, "get %s = %q shredded=%v\n", k, v, errors.Is(err, freshdb.ErrShredded))
			}
			ms, err := tx.Topic("events").Read(0, 0)
			if err != nil {
				return err
			}
			for _, m := range ms {
				fmt.Fprintf(&b, "msg %d lsn %d ts %d %q redacted=%v\n", m.Offset, m.LSN, m.TS, m.Value, m.Redacted)
			}
			c, err := tx.Topic("events").Group("g1").Cursor()
			fmt.Fprintf(&b, "cursor g1 %d %v\n", c, err)
			for i := 0; i < 5; i++ {
				d, err := tx.Doc("users").Get(fmt.Sprint("u", i))
				fmt.Fprintf(&b, "doc u%d %s %v\n", i, d, err)
			}
			ids, err := tx.Doc("users").Find("team", "t1")
			fmt.Fprintf(&b, "find team=t1 %v %v\n", ids, err)
			hits, err := tx.Vector("emb").Search([]float32{3, 1, 1, -2}, 3)
			for _, h := range hits {
				fmt.Fprintf(&b, "hit %s %.4f\n", h.ID, h.Score)
			}
			return err
		})
		if err != nil {
			return "", err
		}
	}
	fmt.Fprintf(&b, "shredded alice=%v bob=%v\n", db.IsShredded("alice"), db.IsShredded("bob"))
	rep, err := db.Verify(ctx)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(&b, "verify ok=%v\n", rep.OK)
	return b.String(), nil
}

// --- workload end ---
