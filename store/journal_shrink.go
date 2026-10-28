//go:build unix

package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
)

// ShrinkStats reports what ShrinkJournal did. Sizes are the file's
// apparent size and the bytes allocated on disk.
type ShrinkStats struct {
	Objects        int   `json:"objects"`
	SizeBefore     int64 `json:"size_before"`
	AllocBefore    int64 `json:"alloc_before"`
	SizeAfter      int64 `json:"size_after"`
	AllocAfter     int64 `json:"alloc_after"`
	DeadRecsBefore int   `json:"dead_records_before"`
}

// ShrinkJournal rewrites dir/journal.fj with only its live objects
// (docs/design/08-gc-retention.md). Deleted objects' data is already
// hole-punched; this also drops their record headers and the delete
// records, and makes the file's size match its contents. It needs the
// directory to itself: it fails if any process has the journal open
// (each open Journal holds journal.lock shared; binaries older than this
// change do not, so do not run them during a shrink).
//
// Crash safety: the copy goes to journal.fj.shrink, is fsynced, and is
// renamed over journal.fj (then the directory is fsynced). A crash before
// the rename leaves the old journal (the copy is removed on the next
// open); after it, the new one is complete.
func ShrinkJournal(dir string) (ShrinkStats, error) {
	var st ShrinkStats
	if l, err := DetectLayout(dir); err != nil {
		return st, err
	} else if l != LayoutJournal {
		return st, fmt.Errorf("store: %s is not a journal directory", dir)
	}
	lk, err := os.OpenFile(filepath.Join(dir, jrnLock), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return st, err
	}
	defer lk.Close()
	if err := syscall.Flock(int(lk.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return st, fmt.Errorf("store: %s is open in another process (stop it first)", dir)
		}
		return st, err
	}
	tmp := filepath.Join(dir, jrnShrink)
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return st, err
	}
	if f, err := os.Open(filepath.Join(dir, jrnFile)); err != nil {
		return st, err
	} else {
		st.SizeBefore, st.AllocBefore, err = fileSize(f)
		f.Close()
		if err != nil {
			return st, err
		}
	}
	old, err := newJournal(dir, false)
	if err != nil {
		return st, err
	}
	defer old.Close()
	old.mu.RLock()
	ents := make([]struct {
		key string
		e   jent
	}, 0, len(old.idx))
	for k, e := range old.idx {
		ents = append(ents, struct {
			key string
			e   jent
		}{k, e})
	}
	old.mu.RUnlock()
	sort.Slice(ents, func(a, b int) bool { return ents[a].e.off < ents[b].e.off })
	recs := 0
	old.mu.Lock()
	old.end = jrnHead
	old.scan(func(byte, string, jent) { recs++ })
	old.mu.Unlock()
	st.DeadRecsBefore, st.Objects = recs-len(ents), len(ents)

	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return st, err
	}
	defer func() {
		if f != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()
	nj := &Journal{dir: dir, f: f, idx: map[string]jent{}}
	if err := nj.open(); err != nil { // fresh header and salt, preallocated
		return st, err
	}
	at := int64(jrnHead)
	for _, x := range ents {
		data := make([]byte, x.e.dlen)
		if _, err := old.f.ReadAt(data, x.e.off+jrnHdr+int64(x.e.klen)); err != nil {
			return st, err
		}
		rec := nj.record(at, recPut, x.key, data)
		if _, err := f.WriteAt(rec, at); err != nil {
			return st, err
		}
		at += int64(len(rec))
	}
	// the usual half-chunk of preallocation after the records
	if err := nj.reserve(at + jrnChunk/2); err != nil {
		return st, err
	}
	if err := f.Sync(); err != nil {
		return st, err
	}
	if err := shrinkHook("before-rename"); err != nil {
		return st, err
	}
	if err := os.Rename(tmp, filepath.Join(dir, jrnFile)); err != nil {
		return st, err
	}
	if err := syncDir(dir); err != nil {
		return st, err
	}
	st.SizeAfter, st.AllocAfter, err = fileSize(f)
	f.Close()
	f = nil
	return st, err
}

// shrinkHook lets tests stop ShrinkJournal at a step.
var shrinkHook = func(string) error { return nil }

func fileSize(f *os.File) (size, alloc int64, err error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	if s, ok := fi.Sys().(*syscall.Stat_t); ok {
		alloc = s.Blocks * 512
	}
	return fi.Size(), alloc, nil
}
