package freshdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/raman20/freshdb/store"
)

// ---- deletion hold ----

// holdStore wraps the database's store so a running backup sees a stable
// object set: while held, deletes are queued and applied when the last
// hold is released. Deletes under keys/ (crypto-shredding) are never
// delayed: a shred must not wait for a backup, so a backup may or may not
// contain a key shredded while it runs (see docs/operations.md).
type holdStore struct {
	store.Store
	mu      sync.Mutex
	holds   int
	pending map[string]bool
}

func (h *holdStore) Delete(ctx context.Context, key string) error {
	h.mu.Lock()
	if h.holds > 0 && !strings.HasPrefix(key, "keys/") {
		h.pending[key] = true
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()
	return h.Store.Delete(ctx, key)
}

// PutIfAbsent of a key whose delete is queued applies the delete first,
// so a re-created object (caches) is not removed by the queued delete.
func (h *holdStore) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	h.mu.Lock()
	queued := h.pending[key]
	delete(h.pending, key)
	h.mu.Unlock()
	if queued {
		if err := h.Store.Delete(ctx, key); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return h.Store.PutIfAbsent(ctx, key, data)
}

// Keys enumerates the wrapped store.
func (h *holdStore) Keys(ctx context.Context) ([]string, error) { return store.Keys(ctx, h.Store) }

func (h *holdStore) Close() error {
	if c, ok := h.Store.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

func (h *holdStore) hold() func(ctx context.Context, logf func(string, ...any)) {
	h.mu.Lock()
	h.holds++
	if h.pending == nil {
		h.pending = map[string]bool{}
	}
	h.mu.Unlock()
	return func(ctx context.Context, logf func(string, ...any)) {
		h.mu.Lock()
		h.holds--
		var keys []string
		if h.holds == 0 {
			for k := range h.pending {
				keys = append(keys, k)
			}
			h.pending = map[string]bool{}
		}
		h.mu.Unlock()
		sort.Strings(keys)
		for _, k := range keys {
			// Best effort: a failure leaves garbage (space), never damage;
			// a later compaction or GC sweeps it again.
			if err := h.Store.Delete(ctx, k); err != nil && !errors.Is(err, store.ErrNotFound) {
				logf("freshdb: backup: deferred delete of %s: %v", k, err)
			}
		}
	}
}

// ---- backup ----

// BackupInfoKey is the object a complete backup ends with. Restore
// refuses a destination without it (an interrupted backup).
const BackupInfoKey = "backup/info.json"

// BackupInfo describes a backup (stored at BackupInfoKey).
type BackupInfo struct {
	Version  int    `json:"version"` // of this record: 1
	Formats  string `json:"formats"` // Formats() of the writer
	Manifest uint64 `json:"manifest"`
	// Heads are the branch heads when the copy started; the backup holds
	// at least these commits (commits acknowledged during the copy may
	// also be in it).
	Heads   map[string]uint64 `json:"heads"`
	Objects int               `json:"objects"`
	Bytes   int64             `json:"bytes"`
	// Digest is SHA-256 over every (key, SHA-256 of value) in key order.
	Digest   string    `json:"digest"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
}

// BackupReport is what Backup did.
type BackupReport struct {
	BackupInfo
	Copied  int   `json:"copied"` // objects written (the rest were already in dst)
	CopiedB int64 `json:"copied_bytes"`
	Removed int   `json:"removed"` // objects in dst no longer in the source
}

// Backup copies a consistent snapshot of the database into dst, online:
// commits continue; compaction and GC run but their deletes wait until
// the copy ends. dst may be empty or hold an earlier backup of the same
// database (only new objects are copied; objects the source no longer has
// are removed, so a shredded subject's key leaves the backup too).
//
// Consistency: the newest manifest is fixed first; every object it can
// reference exists then and is not deleted before the copy ends, and
// manifests are written to dst after everything else, so dst opens at
// that manifest or later commits. The backup is complete only once
// BackupInfoKey is written (last).
func (db *DB) Backup(ctx context.Context, dst store.Store) (*BackupReport, error) {
	hs, ok := db.st.(*holdStore)
	if !ok {
		return nil, errors.New("freshdb: backup: store not wrapped")
	}
	release := hs.hold()
	defer release(context.WithoutCancel(ctx), db.logf)
	if err := db.Healthy(); err != nil && !errors.Is(err, ErrUnknownOutcome) {
		return nil, err
	}
	rep := &BackupReport{BackupInfo: BackupInfo{Version: 1, Formats: Formats(), Started: time.Now().UTC(), Heads: db.Branches()}}
	db.mu.RLock()
	rep.Manifest = db.man.ID
	db.mu.RUnlock()
	// An earlier backup's marker goes first: from here until the new one
	// is written, dst is "incomplete".
	if err := dst.Delete(ctx, BackupInfoKey); err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("freshdb: backup: %w", err)
	}
	mans, err := hs.Store.List(ctx, "manifest/")
	if err != nil {
		return nil, err
	}
	all, err := store.Keys(ctx, hs.Store)
	if err != nil {
		return nil, err
	}
	keep := map[string]bool{}
	var data []string
	for _, k := range all {
		if !strings.HasPrefix(k, "manifest/") && !strings.HasPrefix(k, "backup/") && !strings.HasPrefix(k, "quarantine/") {
			data = append(data, k)
			keep[k] = true
		}
	}
	for _, k := range mans {
		keep[k] = true
	}
	have, err := store.Keys(ctx, dst)
	if err != nil {
		return nil, err
	}
	in := map[string]bool{}
	for _, k := range have {
		in[k] = true
	}
	if len(have) > 0 {
		// Only an earlier backup of this database may be updated in place:
		// its per-database random objects must match ours.
		if !anyPrefix(have, "manifest/") {
			return nil, fmt.Errorf("freshdb: backup: destination is not empty and holds no database (%d objects)", len(have))
		}
		for _, k := range []string{"keys/_kekcheck", "keys/_kek"} {
			if !in[k] {
				continue
			}
			a, err1 := hs.Store.Get(ctx, k)
			b, err2 := dst.Get(ctx, k)
			if err1 != nil || err2 != nil || !bytes.Equal(a, b) {
				return nil, errors.New("freshdb: backup: destination holds a backup of a different database")
			}
		}
	}
	nosync(dst, true)
	// data first, manifests last
	var repMu sync.Mutex
	copyOne := func(k string) error {
		if in[k] && !strings.HasPrefix(k, "manifest/") {
			return nil
		}
		v, err := hs.Store.Get(ctx, k)
		if err != nil {
			return fmt.Errorf("freshdb: backup: read %s: %w", k, err)
		}
		if err := dst.PutIfAbsent(ctx, k, v); err != nil {
			if !errors.Is(err, store.ErrExists) {
				return fmt.Errorf("freshdb: backup: write %s: %w", k, err)
			}
			old, gerr := dst.Get(ctx, k)
			if gerr != nil || !bytes.Equal(old, v) {
				return fmt.Errorf("freshdb: backup: %s differs in the destination (a backup of another database?)", k)
			}
			return nil
		}
		repMu.Lock()
		rep.Copied++
		rep.CopiedB += int64(len(v))
		repMu.Unlock()
		return nil
	}
	if err := parallel(ctx, data, 8, copyOne); err != nil {
		return nil, err
	}
	if err := parallel(ctx, mans, 1, copyOne); err != nil {
		return nil, err
	}
	for _, k := range have {
		if !keep[k] && k != BackupInfoKey {
			if err := dst.Delete(ctx, k); err != nil && !errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("freshdb: backup: remove stale %s: %w", k, err)
			}
			rep.Removed++
		}
	}
	nosync(dst, false)
	if s, ok := dst.(interface{ Sync() error }); ok {
		if err := s.Sync(); err != nil {
			return nil, err
		}
	}
	// Read back and digest what dst now holds.
	if rep.Objects, rep.Bytes, rep.Digest, err = digest(ctx, dst); err != nil {
		return nil, err
	}
	rep.Finished = time.Now().UTC()
	info, _ := json.MarshalIndent(rep.BackupInfo, "", "  ")
	if err := dst.PutIfAbsent(ctx, BackupInfoKey, info); err != nil {
		return nil, fmt.Errorf("freshdb: backup: write %s: %w", BackupInfoKey, err)
	}
	return rep, nil
}

func anyPrefix(keys []string, p string) bool {
	for _, k := range keys {
		if strings.HasPrefix(k, p) {
			return true
		}
	}
	return false
}

func nosync(st store.Store, on bool) {
	if ns, ok := st.(interface{ SetUnsafeNoSync(bool) }); ok {
		ns.SetUnsafeNoSync(on)
	}
}

// digest reads every object of st except backup/ ones.
func digest(ctx context.Context, st store.Store) (n int, size int64, sum string, err error) {
	keys, err := store.Keys(ctx, st)
	if err != nil {
		return 0, 0, "", err
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		if strings.HasPrefix(k, "backup/") {
			continue
		}
		v, err := st.Get(ctx, k)
		if err != nil {
			return 0, 0, "", fmt.Errorf("read %s: %w", k, err)
		}
		addDigest(h, k, v)
		n++
		size += int64(len(v))
	}
	return n, size, hex.EncodeToString(h.Sum(nil)), nil
}

func addDigest(h hash.Hash, k string, v []byte) {
	s := sha256.Sum256(v)
	fmt.Fprintf(h, "%d:%s:", len(k), k)
	h.Write(s[:])
}

// parallel runs f over keys with n workers; the first error stops it.
func parallel(ctx context.Context, keys []string, n int, f func(string) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := make(chan string)
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range ch {
				if err := f(k); err != nil {
					once.Do(func() { first = err; cancel() })
				}
			}
		}()
	}
loop:
	for _, k := range keys {
		select {
		case ch <- k:
		case <-ctx.Done():
			break loop
		}
	}
	close(ch)
	wg.Wait()
	if first == nil && ctx.Err() != nil {
		first = ctx.Err()
	}
	return first
}

// ---- restore ----

// RestoreReport is what Restore did.
type RestoreReport struct {
	Info    BackupInfo `json:"backup"`
	Objects int        `json:"objects"`
	Bytes   int64      `json:"bytes"`
}

// Restore copies a complete backup (one with BackupInfoKey) into dst,
// which must be empty, and checks every object against the backup's
// digest. Open dst afterwards (and Verify it); to restore to an earlier
// log position, Rewind a branch there (history the backup still holds).
func Restore(ctx context.Context, src, dst store.Store) (*RestoreReport, error) {
	raw, err := src.Get(ctx, BackupInfoKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("freshdb: restore: no %s: not a backup, or the backup did not finish", BackupInfoKey)
	}
	if err != nil {
		return nil, err
	}
	var info BackupInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("freshdb: restore: %s: %w", BackupInfoKey, err)
	}
	if info.Version != 1 {
		return nil, fmt.Errorf("%w: backup record version %d (this build reads 1)", ErrVersion, info.Version)
	}
	have, err := store.Keys(ctx, dst)
	if err != nil {
		return nil, err
	}
	if len(have) > 0 {
		return nil, fmt.Errorf("freshdb: restore: destination is not empty (%d objects)", len(have))
	}
	keys, err := store.Keys(ctx, src)
	if err != nil {
		return nil, err
	}
	var data, mans []string
	for _, k := range keys {
		switch {
		case strings.HasPrefix(k, "backup/"):
		case strings.HasPrefix(k, "manifest/"):
			mans = append(mans, k)
		default:
			data = append(data, k)
		}
	}
	rep := &RestoreReport{Info: info}
	h := sha256.New()
	nosync(dst, true)
	for _, k := range append(data, mans...) { // manifests last, as in Backup
		v, err := src.Get(ctx, k)
		if err != nil {
			return nil, fmt.Errorf("freshdb: restore: read %s: %w", k, err)
		}
		if err := dst.PutIfAbsent(ctx, k, v); err != nil {
			return nil, fmt.Errorf("freshdb: restore: write %s: %w", k, err)
		}
		rep.Objects++
		rep.Bytes += int64(len(v))
	}
	nosync(dst, false)
	if s, ok := dst.(interface{ Sync() error }); ok {
		if err := s.Sync(); err != nil {
			return nil, err
		}
	}
	// digest in key order, read back from dst
	sorted := append(append([]string{}, data...), mans...)
	sort.Strings(sorted)
	for _, k := range sorted {
		v, err := dst.Get(ctx, k)
		if err != nil {
			return nil, fmt.Errorf("freshdb: restore: read back %s: %w", k, err)
		}
		addDigest(h, k, v)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != info.Digest || rep.Objects != info.Objects || rep.Bytes != info.Bytes {
		return nil, fmt.Errorf("%w: restore: backup does not match its record (%d objects / %d bytes / digest %.12s, record says %d / %d / %.12s)",
			ErrCorrupt, rep.Objects, rep.Bytes, got, info.Objects, info.Bytes, info.Digest)
	}
	return rep, nil
}
