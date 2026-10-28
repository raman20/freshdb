package freshdb

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/raman20/freshdb/store"
)

// v010DB builds a database in the v0.1.0 files layout with KV, topic and
// branch data, and returns a digest of its state.
func v010DB(t *testing.T, dir string) string {
	t.Helper()
	db, err := Open(ctx, "fs://"+dir, quiet())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		err := db.Update(ctx, func(tx *Tx) error {
			tx.Put(fmt.Sprintf("k%03d", i%120), []byte(fmt.Sprintf("v%d-%0200d", i, i)))
			tx.Topic("t").Publish([]byte(fmt.Sprint(i)))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	d := digestDB(t, db)
	db.Close()
	return d
}

func digestDB(t *testing.T, db *DB) string {
	t.Helper()
	var out string
	db.View(ctx, func(tx *Tx) error {
		for i := 0; i < 120; i++ {
			v, _ := tx.Get(fmt.Sprintf("k%03d", i))
			out += string(v) + ";"
		}
		ms, _ := tx.Topic("t").Read(0, 0)
		out += fmt.Sprint(len(ms))
		return nil
	})
	return out
}

// A v0.1.0 directory opens unchanged under the default URL, and after
// `freshdb migrate` opens as a journal with identical lens state.
func TestMigrateDatabase(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("journal is Unix-only")
	}
	dir := filepath.Join(t.TempDir(), "db")
	want := v010DB(t, dir)
	db, err := Open(ctx, dir, quiet())
	if err != nil {
		t.Fatal(err)
	}
	if got := digestDB(t, db); got != want {
		t.Fatal("files dir changed under the default URL")
	}
	db.Close()
	if l, _ := store.DetectLayout(dir); l != store.LayoutFiles {
		t.Fatalf("opening changed the layout to %q", l)
	}
	if err := store.Migrate(ctx, dir, store.LayoutJournal, nil); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, dir, quiet())
	if err != nil {
		t.Fatal(err)
	}
	if got := digestDB(t, db); got != want {
		t.Fatal("state differs after migrate")
	}
	if err := db.Update(ctx, func(tx *Tx) error { tx.Put("after", []byte("1")); return nil }); err != nil {
		t.Fatal(err)
	}
	if r, err := db.Verify(ctx); err != nil || !r.OK {
		t.Fatalf("verify: %v %+v", err, r)
	}
	db.Close()
}

// SIGKILL a migrating child at random points; rerunning Migrate must
// finish with identical state and an intact backup.
func TestCrashKill9Migrate(t *testing.T) {
	if testing.Short() || runtime.GOOS == "windows" {
		t.Skip("short or no journal")
	}
	base := t.TempDir()
	tmpl := filepath.Join(base, "tmpl")
	want := v010DB(t, tmpl)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Time one in-process migration so kills land inside the window.
	probe := filepath.Join(base, "probe")
	exec.Command("cp", "-r", tmpl, probe).Run()
	t0 := time.Now()
	if err := store.Migrate(ctx, probe, store.LayoutJournal, nil); err != nil {
		t.Fatal(err)
	}
	window := time.Since(t0)
	interrupted := 0
	for it := 0; it < 12; it++ {
		dir := filepath.Join(base, fmt.Sprint("db", it))
		if err := exec.Command("cp", "-r", tmpl, dir).Run(); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), "FRESHDB_MIGRATE_CHILD="+dir)
		out, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(out)
		sc.Scan() // START
		time.Sleep(time.Duration(rng.Int63n(int64(window) + 1)))
		cmd.Process.Kill()
		for sc.Scan() {
			if txt := sc.Text(); txt != "" {
				t.Errorf("child: %s", txt)
			}
		}
		cmd.Wait()
		l, _ := store.DetectLayout(dir)
		if l != store.LayoutJournal || fileExists(dir+".migrate-tmp") || fileExists(dir+".migrated") {
			interrupted++
		}
		if err := store.Migrate(ctx, dir, store.LayoutJournal, nil); err != nil {
			t.Fatalf("iter %d: resume: %v", it, err)
		}
		db, err := Open(ctx, dir, quiet())
		if err != nil {
			t.Fatalf("iter %d: open: %v", it, err)
		}
		if got := digestDB(t, db); got != want {
			t.Fatalf("iter %d: state differs", it)
		}
		db.Close()
		bak, err := Open(ctx, "fs://"+dir+".bak-files", quiet())
		if err != nil {
			t.Fatal(err)
		}
		if got := digestDB(t, bak); got != want {
			t.Fatalf("iter %d: backup differs", it)
		}
		bak.Close()
	}
	t.Logf("%d/12 kills interrupted the migration", interrupted)
	if interrupted == 0 {
		t.Error("no kill landed mid-migration; the test is not exercising resume")
	}
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }
