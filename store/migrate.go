package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// migrateHook, if set, runs after each named step of Migrate (tests use it
// to inject crashes). A non-nil error aborts Migrate at that point.
var migrateHook func(step string) error

func hook(step string) error {
	if migrateHook != nil {
		return migrateHook(step)
	}
	return nil
}

// Migrate converts the local directory dir to layout `to`, offline: no
// process may use dir meanwhile. It follows the protocol model-checked in
// spec/FreshMigrate.tla:
//
//  1. copy every object into dir.migrate-tmp and verify it byte-for-byte;
//  2. fsync it, then rename it to dir.migrated (a complete copy);
//  3. rename dir to dir.bak-<old layout>;
//  4. rename dir.migrated to dir.
//
// Each rename is followed by an fsync of the parent directory. If a crash
// interrupts it, running Migrate again finishes the job. The backup is
// never deleted. Migrating a directory already in layout `to` is a no-op.
func Migrate(ctx context.Context, dir string, to Layout, logf func(string, ...any)) (err error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if to != LayoutJournal && to != LayoutFiles {
		return fmt.Errorf("store: migrate: unknown layout %q", to)
	}
	if to == LayoutJournal && !journalSupported {
		return errors.New("store: migrate: the journal layout needs a Unix-like OS")
	}
	dir = filepath.Clean(dir)
	parent := filepath.Dir(dir)
	tmp, mig := dir+".migrate-tmp", dir+".migrated"
	// Resume rules: decided only by which names exist.
	if exists(mig) {
		if exists(dir) {
			logf("migrate: resuming: moving the original aside")
			return finish(dir, mig, parent, true)
		}
		logf("migrate: resuming: moving the converted copy into place")
		return finish(dir, mig, parent, false)
	}
	from, err := DetectLayout(dir)
	if err != nil {
		return err
	}
	switch from {
	case LayoutNone:
		return fmt.Errorf("store: migrate: %s is missing or empty", dir)
	case to:
		logf("migrate: %s already uses the %s layout", dir, to)
		return nil
	}
	if bak := backupName(dir, from); exists(bak) {
		return fmt.Errorf("store: migrate: backup %s already exists; move it away first", bak)
	}
	if err := os.RemoveAll(tmp); err != nil { // never trusted
		return err
	}
	if err := hook("clean"); err != nil {
		return err
	}
	src, err := openLayout(dir, from)
	if err != nil {
		return err
	}
	defer closeStore(src)
	dst, err := openLayout(tmp, to)
	if err != nil {
		return err
	}
	defer closeStore(dst)
	if ns, ok := dst.(interface{ SetUnsafeNoSync(bool) }); ok && to == LayoutJournal {
		ns.SetUnsafeNoSync(true) // one Sync at the end instead of one per object
	}
	keys, err := Keys(ctx, src)
	if err != nil {
		return err
	}
	var bytesCopied int64
	for i, k := range keys {
		v, err := src.Get(ctx, k)
		if err != nil {
			return fmt.Errorf("store: migrate: read %s: %w", k, err)
		}
		if err := dst.PutIfAbsent(ctx, k, v); err != nil {
			return fmt.Errorf("store: migrate: write %s: %w", k, err)
		}
		bytesCopied += int64(len(v))
		if i > 0 && i%100000 == 0 {
			logf("migrate: copied %d/%d objects", i, len(keys))
		}
		if i == len(keys)/2 {
			if err := hook("copy"); err != nil {
				return err
			}
		}
	}
	// Verify: same key set, byte-equal values, read back through dst.
	got, err := Keys(ctx, dst)
	if err != nil {
		return err
	}
	if len(got) != len(keys) {
		return fmt.Errorf("store: migrate: verify: %d objects copied, %d found", len(keys), len(got))
	}
	for i, k := range keys {
		if got[i] != k {
			return fmt.Errorf("store: migrate: verify: key %q missing", k)
		}
		a, err := src.Get(ctx, k)
		if err != nil {
			return err
		}
		b, err := dst.Get(ctx, k)
		if err != nil {
			return err
		}
		if !bytes.Equal(a, b) {
			return fmt.Errorf("store: migrate: verify: %s differs", k)
		}
	}
	if err := hook("verify"); err != nil {
		return err
	}
	if s, ok := dst.(interface{ Sync() error }); ok {
		if err := s.Sync(); err != nil {
			return err
		}
	}
	if err := syncDir(tmp); err != nil {
		return err
	}
	if err := syncDir(parent); err != nil {
		return err
	}
	if err := hook("sync"); err != nil {
		return err
	}
	closeStore(src)
	closeStore(dst)
	logf("migrate: copied and verified %d objects (%d bytes)", len(keys), bytesCopied)
	if err := os.Rename(tmp, mig); err != nil {
		return err
	}
	if err := hook("r1"); err != nil {
		return err
	}
	if err := syncDir(parent); err != nil {
		return err
	}
	if err := hook("f1"); err != nil {
		return err
	}
	return finish(dir, mig, parent, true)
}

// finish performs steps 3 and 4 (or only 4 if moveOriginal is false).
func finish(dir, mig, parent string, moveOriginal bool) error {
	if moveOriginal {
		from, err := DetectLayout(dir)
		if err != nil {
			return err
		}
		bak := backupName(dir, from)
		if exists(bak) {
			return fmt.Errorf("store: migrate: backup %s already exists; move it away first", bak)
		}
		if err := os.Rename(dir, bak); err != nil {
			return err
		}
		if err := hook("bak"); err != nil {
			return err
		}
		if err := syncDir(parent); err != nil {
			return err
		}
		if err := hook("f2"); err != nil {
			return err
		}
	}
	if err := os.Rename(mig, dir); err != nil {
		return err
	}
	if err := hook("final"); err != nil {
		return err
	}
	return syncDir(parent)
}

func backupName(dir string, from Layout) string { return dir + ".bak-" + string(from) }

func exists(p string) bool {
	_, err := os.Stat(p)
	return !errors.Is(err, fs.ErrNotExist)
}

func closeStore(s Store) {
	if c, ok := s.(io.Closer); ok {
		c.Close()
	}
}
