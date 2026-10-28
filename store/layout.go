package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// jrnFile is the journal layout's single data file.
const jrnFile = "journal.fj"

// Layout names an on-disk format for a local directory.
type Layout string

const (
	// LayoutNone is a missing or empty directory.
	LayoutNone Layout = ""
	// LayoutFiles is one file per object (FS; the v0.1.0 layout).
	LayoutFiles Layout = "files"
	// LayoutJournal is one append-only journal.fj (Journal).
	LayoutJournal Layout = "journal"
)

// DefaultLayout is the layout a new local directory gets: journal where it
// is supported (Unix), files elsewhere.
func DefaultLayout() Layout {
	if journalSupported {
		return LayoutJournal
	}
	return LayoutFiles
}

// DetectLayout reports which layout dir already holds. A directory holding
// journal.fj is a journal; any other entry (besides an empty .tmp/) makes
// it a files directory; a missing or empty directory is LayoutNone.
func DetectLayout(dir string) (Layout, error) {
	if _, err := os.Stat(filepath.Join(dir, jrnFile)); err == nil {
		return LayoutJournal, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return LayoutNone, err
	}
	ents, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return LayoutNone, nil
	}
	if err != nil {
		return LayoutNone, err
	}
	for _, e := range ents {
		if e.Name() == tmpDir && e.IsDir() {
			sub, err := os.ReadDir(filepath.Join(dir, tmpDir))
			if err != nil || len(sub) > 0 {
				return LayoutFiles, nil
			}
			continue
		}
		return LayoutFiles, nil
	}
	return LayoutNone, nil
}

// OpenLocal opens dir in the layout it already has, or DefaultLayout if it
// is new. Existing v0.1.0 directories keep the files layout; convert them
// with Migrate.
func OpenLocal(dir string) (Store, error) {
	if dir == "" {
		return nil, errors.New("store: empty path")
	}
	l, err := DetectLayout(dir)
	if err != nil {
		return nil, err
	}
	if l == LayoutNone {
		l = DefaultLayout()
	}
	return openLayout(dir, l)
}

func openLayout(dir string, l Layout) (Store, error) {
	switch l {
	case LayoutJournal:
		return NewJournal(dir)
	case LayoutFiles:
		return NewFS(dir)
	}
	return nil, fmt.Errorf("store: unknown layout %q", l)
}

// Walker is implemented by stores that can enumerate every key.
type Walker interface {
	// Keys returns every key in the store, sorted.
	Keys(ctx context.Context) ([]string, error)
}

// Keys returns every key of a local store (FS, Journal or Mem), sorted.
func Keys(ctx context.Context, s Store) ([]string, error) {
	w, ok := s.(Walker)
	if !ok {
		return nil, fmt.Errorf("store: %T cannot enumerate keys", s)
	}
	return w.Keys(ctx)
}

// Keys walks the directory tree (excluding .tmp/ and a journal file).
func (s *FS) Keys(context.Context) ([]string, error) {
	var out []string
	err := filepath.WalkDir(s.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(s.root, p)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel == tmpDir {
				return filepath.SkipDir
			}
			return nil
		}
		if rel == jrnFile || strings.HasPrefix(rel, tmpDir+"/") {
			return nil
		}
		out = append(out, rel)
		return nil
	})
	sort.Strings(out)
	return out, err
}
