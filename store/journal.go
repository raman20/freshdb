//go:build unix

package store

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

// Journal is a local-directory Store that keeps every object as a record
// in one preallocated, append-only file (<dir>/journal.fj), instead of one
// file per object like FS. PutIfAbsent appends a record and fdatasyncs it:
// no file creation, rename or directory fsync, so a commit costs one
// data-only sync of already-allocated blocks.
//
// Semantics match FS:
//   - create-only and atomic: a record is visible only once it is complete
//     and durable. Writers append under an exclusive flock held through the
//     fdatasync; readers in other processes catch up under a shared flock,
//     so they never see an in-flight record.
//   - torn tails are harmless: each record's CRC-32C covers a per-file
//     random salt, its own offset, header, key and data, so a partial or
//     stale record never validates and the next append overwrites it.
//   - Delete appends a delete record, then marks the old record dead and
//     punches out (or zeroes) its data in place, so deleted bytes (e.g.
//     shredded keys) do not linger in the file.
//
// Space of deleted objects is not reused (holes are punched where the file
// system supports it). Several processes may open the same directory.
type Journal struct {
	dir    string
	f      *os.File // data + writer flock
	rf     *os.File // second descriptor: reader (shared) flock
	salt   [16]byte
	noSync bool

	wmu  sync.Mutex // one in-process writer at a time (flock is per descriptor)
	rfmu sync.Mutex // one in-process user of rf's shared lock at a time
	mu   sync.RWMutex
	idx  map[string]jent
	end  int64 // offset after the last valid record seen
}

type jent struct {
	off  int64
	klen int
	dlen int
}

const (
	jrnMagic   = "FRESHJ1\x00"
	jrnHead    = 32      // magic(8) salt(16) crc(4) pad(4)
	jrnHdr     = 12      // crc(4) kind(1) pad(1) klen(2) dlen(4)
	jrnChunk   = 8 << 20 // preallocation step (zero-filled, then fsync)
	jrnMaxKey  = 4096
	jrnMaxData = 1 << 30

	recPut  = 1
	recDel  = 2
	recDead = 3 // a put whose data was erased by a later delete
)

var jcrc = crc32.MakeTable(crc32.Castagnoli)

// SetUnsafeNoSync disables every sync (benchmarks only, as for FS).
func (j *Journal) SetUnsafeNoSync(on bool) { j.noSync = on }

// NewJournal opens or creates the journal store in dir.
func NewJournal(dir string) (*Journal, error) {
	if dir == "" {
		return nil, errors.New("store: empty path")
	}
	if l, err := DetectLayout(dir); err != nil {
		return nil, err
	} else if l == LayoutFiles {
		return nil, fmt.Errorf("store: %s holds the one-file-per-object layout; open it as fs:// or convert it with `freshdb migrate`", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, jrnFile)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	rf, err := os.Open(p)
	if err != nil {
		f.Close()
		return nil, err
	}
	j := &Journal{dir: dir, f: f, rf: rf, idx: map[string]jent{}}
	if err := j.open(); err != nil {
		f.Close()
		rf.Close()
		return nil, err
	}
	return j, nil
}

func (j *Journal) open() error {
	if err := flock(j.f, syscall.LOCK_EX); err != nil {
		return err
	}
	defer flock(j.f, syscall.LOCK_UN)
	var h [jrnHead]byte
	n, err := j.f.ReadAt(h[:], 0)
	switch {
	case n == 0: // new file: write the header durably
		copy(h[:8], jrnMagic)
		rand.Read(h[8:24])
		binary.LittleEndian.PutUint32(h[24:28], crc32.Checksum(h[:24], jcrc))
		if _, err := j.f.WriteAt(h[:], 0); err != nil {
			return err
		}
		if err := j.f.Sync(); err != nil {
			return err
		}
		if err := syncDir(j.dir); err != nil {
			return err
		}
	case n < jrnHead || string(h[:8]) != jrnMagic ||
		binary.LittleEndian.Uint32(h[24:28]) != crc32.Checksum(h[:24], jcrc):
		return fmt.Errorf("store: %s: not a journal file (%v)", jrnFile, err)
	}
	copy(j.salt[:], h[8:24])
	j.end = jrnHead
	j.mu.Lock()
	defer j.mu.Unlock()
	// Finish deletes a crash interrupted: a put followed by its delete
	// record but not yet marked dead.
	live := map[string]jent{}
	var unerased []jent
	j.scan(func(kind byte, key string, e jent) {
		switch kind {
		case recPut:
			live[key] = e
		case recDel:
			if p, ok := live[key]; ok {
				unerased = append(unerased, p)
				delete(live, key)
			}
		}
	})
	for _, e := range unerased {
		if err := j.erase(e); err != nil {
			return err
		}
	}
	// Preallocate ahead now rather than in the first commit.
	if err := j.reserve(j.end + jrnChunk/2); err != nil {
		return err
	}
	if len(unerased) > 0 {
		return j.sync()
	}
	return nil
}

// scan applies valid records from j.end onwards to the index (j.mu held).
func (j *Journal) scan(fn func(kind byte, key string, e jent)) {
	var hb [jrnHdr]byte
	for {
		if n, _ := j.f.ReadAt(hb[:], j.end); n < jrnHdr {
			return
		}
		kind, klen := hb[4], int(binary.LittleEndian.Uint16(hb[6:8]))
		dlen := int(binary.LittleEndian.Uint32(hb[8:12]))
		if kind < recPut || kind > recDead || klen == 0 || klen > jrnMaxKey || dlen > jrnMaxData ||
			(kind == recDel && dlen != 0) {
			return
		}
		body := klen
		if kind == recPut {
			body += dlen
		}
		b := make([]byte, body)
		if n, _ := j.f.ReadAt(b, j.end+jrnHdr); n < body {
			return
		}
		if binary.LittleEndian.Uint32(hb[:4]) != j.sum(j.end, hb[4:], b) {
			return
		}
		key := string(b[:klen])
		e := jent{off: j.end, klen: klen, dlen: dlen}
		switch kind {
		case recPut:
			j.idx[key] = e
		case recDel:
			delete(j.idx, key)
		}
		if fn != nil {
			fn(kind, key, e)
		}
		j.end += int64(jrnHdr + klen + dlen)
	}
}

func (j *Journal) sum(off int64, hdr, body []byte) uint32 {
	var o [8]byte
	binary.LittleEndian.PutUint64(o[:], uint64(off))
	c := crc32.Update(0, jcrc, j.salt[:])
	c = crc32.Update(c, jcrc, o[:])
	c = crc32.Update(c, jcrc, hdr[:jrnHdr-4])
	return crc32.Update(c, jcrc, body)
}

// catchUp reads records other processes appended, under a shared flock so
// that none is still being written or synced.
func (j *Journal) catchUp() error {
	j.rfmu.Lock()
	defer j.rfmu.Unlock()
	if err := flock(j.rf, syscall.LOCK_SH); err != nil {
		return err
	}
	defer flock(j.rf, syscall.LOCK_UN)
	j.mu.Lock()
	j.scan(nil)
	j.mu.Unlock()
	return nil
}

func (j *Journal) record(off int64, kind byte, key string, data []byte) []byte {
	b := make([]byte, jrnHdr+len(key)+len(data))
	b[4] = kind
	binary.LittleEndian.PutUint16(b[6:8], uint16(len(key)))
	binary.LittleEndian.PutUint32(b[8:12], uint32(len(data)))
	copy(b[jrnHdr:], key)
	copy(b[jrnHdr+len(key):], data)
	binary.LittleEndian.PutUint32(b[:4], j.sum(off, b[4:jrnHdr], b[jrnHdr:]))
	return b
}

// lockWrite takes the writer locks and catches up; the caller must unlock.
func (j *Journal) lockWrite() (func(), error) {
	j.wmu.Lock()
	if err := flock(j.f, syscall.LOCK_EX); err != nil {
		j.wmu.Unlock()
		return nil, err
	}
	j.mu.Lock()
	j.scan(nil)
	j.mu.Unlock()
	return func() { flock(j.f, syscall.LOCK_UN); j.wmu.Unlock() }, nil
}

// appendRec writes rec at the end (writer locks held) and makes it durable.
func (j *Journal) appendRec(rec []byte) error {
	j.mu.RLock()
	at := j.end
	j.mu.RUnlock()
	if err := j.reserve(at + int64(len(rec))); err != nil {
		return err
	}
	if _, err := j.f.WriteAt(rec, at); err != nil {
		return err
	}
	return j.sync()
}

// reserve makes sure [0,upto) is allocated and written (zeros), so later
// appends change no file metadata and fdatasync stays cheap.
func (j *Journal) reserve(upto int64) error {
	st, err := j.f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	if size >= upto {
		return nil
	}
	newSize := (upto + jrnChunk - 1) / jrnChunk * jrnChunk
	zero := make([]byte, 1<<20)
	for off := size; off < newSize; off += int64(len(zero)) {
		n := min(int64(len(zero)), newSize-off)
		if _, err := j.f.WriteAt(zero[:n], off); err != nil {
			return err
		}
	}
	if j.noSync {
		return nil
	}
	return j.f.Sync() // size changed: full sync, once per chunk
}

// Sync makes every record appended so far durable, even with
// SetUnsafeNoSync (used by Migrate after a bulk copy).
func (j *Journal) Sync() error {
	unlock, err := j.lockWrite()
	if err != nil {
		return err
	}
	defer unlock()
	if err := j.f.Sync(); err != nil {
		return err
	}
	return syncDir(j.dir)
}

// Close releases the journal's file handles. The Journal is unusable after.
func (j *Journal) Close() error {
	j.wmu.Lock()
	j.rfmu.Lock()
	defer j.rfmu.Unlock()
	defer j.wmu.Unlock()
	e1, e2 := j.f.Close(), j.rf.Close()
	if e1 != nil {
		return e1
	}
	return e2
}

// Keys returns every live key, sorted.
func (j *Journal) Keys(context.Context) ([]string, error) {
	if err := j.catchUp(); err != nil {
		return nil, err
	}
	j.mu.RLock()
	out := make([]string, 0, len(j.idx))
	for k := range j.idx {
		out = append(out, k)
	}
	j.mu.RUnlock()
	sort.Strings(out)
	return out, nil
}

func (j *Journal) sync() error {
	if j.noSync {
		return nil
	}
	return datasync(j.f)
}

func checkKey(key string) error {
	if key == "" || len(key) > jrnMaxKey {
		return fmt.Errorf("store: bad key %q", key)
	}
	return nil
}

func (j *Journal) PutIfAbsent(_ context.Context, key string, data []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if len(data) > jrnMaxData {
		return fmt.Errorf("store: object %s too large (%d bytes)", key, len(data))
	}
	unlock, err := j.lockWrite()
	if err != nil {
		return err
	}
	defer unlock()
	j.mu.RLock()
	_, exists := j.idx[key]
	at := j.end
	j.mu.RUnlock()
	if exists {
		return ErrExists
	}
	rec := j.record(at, recPut, key, data)
	if err := j.appendRec(rec); err != nil {
		return err
	}
	j.mu.Lock()
	j.idx[key] = jent{off: at, klen: len(key), dlen: len(data)}
	j.end = at + int64(len(rec))
	j.mu.Unlock()
	return nil
}

// lookup finds key, catching up with other processes on a miss.
func (j *Journal) lookup(key string, refresh bool) (jent, bool, error) {
	if refresh {
		if err := j.catchUp(); err != nil {
			return jent{}, false, err
		}
	}
	j.mu.RLock()
	e, ok := j.idx[key]
	j.mu.RUnlock()
	if !ok && !refresh {
		return j.lookup(key, true)
	}
	return e, ok, nil
}

// live re-reads a record's header and key and reports whether it is still
// a valid put of key (a concurrent delete marks it dead before erasing).
func (j *Journal) live(e jent, key string) bool {
	var buf [jrnHdr + 128]byte // most keys fit: no allocation per read
	var b []byte
	if n := jrnHdr + e.klen; n <= len(buf) {
		b = buf[:n]
	} else {
		b = make([]byte, n)
	}
	if n, _ := j.f.ReadAt(b, e.off); n < len(b) {
		return false
	}
	return b[4] == recPut && string(b[jrnHdr:]) == key
}

func (j *Journal) Get(ctx context.Context, key string) ([]byte, error) {
	for attempt := 0; attempt < 2; attempt++ {
		e, ok, err := j.lookup(key, attempt > 0)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrNotFound
		}
		b := make([]byte, jrnHdr+e.klen+e.dlen)
		if n, err := j.f.ReadAt(b, e.off); n < len(b) {
			return nil, fmt.Errorf("store: read %s: %v", key, err)
		}
		if b[4] == recPut && binary.LittleEndian.Uint32(b[:4]) == j.sum(e.off, b[4:jrnHdr], b[jrnHdr:]) {
			return b[jrnHdr+e.klen:], nil
		}
		// Deleted by another process meanwhile (or corrupt): look again.
	}
	return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
}

// GetRange reads n bytes at off of key's data without reading the rest.
// Callers verify the bytes (the engine checks a CRC per value).
func (j *Journal) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	for attempt := 0; attempt < 2; attempt++ {
		e, ok, err := j.lookup(key, attempt > 0)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrNotFound
		}
		if off < 0 || n < 0 || off+n > int64(e.dlen) {
			return nil, fmt.Errorf("store: range %d+%d outside %s (%d bytes)", off, n, key, e.dlen)
		}
		b := make([]byte, n)
		if m, err := j.f.ReadAt(b, e.off+int64(jrnHdr+e.klen)+off); int64(m) < n {
			return nil, fmt.Errorf("store: range read %s: %v", key, err)
		}
		if j.live(e, key) { // checked after the read: erasure marks dead first
			return b, nil
		}
	}
	return nil, ErrNotFound
}

func (j *Journal) List(_ context.Context, prefix string) ([]string, error) {
	if !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("store: list prefix must end in /: %q", prefix)
	}
	if err := j.catchUp(); err != nil {
		return nil, err
	}
	j.mu.RLock()
	var out []string
	for k := range j.idx {
		if strings.HasPrefix(k, prefix) && !strings.Contains(k[len(prefix):], "/") {
			out = append(out, k)
		}
	}
	j.mu.RUnlock()
	sort.Strings(out)
	return out, nil
}

func (j *Journal) Delete(_ context.Context, key string) error {
	if err := checkKey(key); err != nil {
		return err
	}
	unlock, err := j.lockWrite()
	if err != nil {
		return err
	}
	defer unlock()
	j.mu.RLock()
	e, ok := j.idx[key]
	at := j.end
	j.mu.RUnlock()
	if !ok {
		return nil
	}
	rec := j.record(at, recDel, key, nil)
	if err := j.appendRec(rec); err != nil {
		return err
	}
	j.mu.Lock()
	delete(j.idx, key)
	j.end = at + int64(len(rec))
	j.mu.Unlock()
	if err := j.erase(e); err != nil {
		return err
	}
	return j.sync()
}

// erase marks a put record dead, then removes its data bytes.
func (j *Journal) erase(e jent) error {
	h := make([]byte, jrnHdr+e.klen)
	if n, err := j.f.ReadAt(h, e.off); n < len(h) {
		return fmt.Errorf("store: erase at %d: %v", e.off, err)
	}
	h[4] = recDead
	binary.LittleEndian.PutUint32(h[:4], j.sum(e.off, h[4:jrnHdr], h[jrnHdr:]))
	if _, err := j.f.WriteAt(h, e.off); err != nil {
		return err
	}
	return punch(j.f, e.off+int64(len(h)), int64(e.dlen))
}

func flock(f *os.File, how int) error {
	for {
		err := syscall.Flock(int(f.Fd()), how)
		if err != syscall.EINTR {
			return err
		}
	}
}

// zeroRange overwrites [off, off+n) with zeros (fallback for punch).
func zeroRange(f *os.File, off, n int64) error {
	z := make([]byte, min(n, 1<<20))
	for n > 0 {
		k := min(n, int64(len(z)))
		if _, err := f.WriteAt(z[:k], off); err != nil {
			return err
		}
		off, n = off+k, n-k
	}
	return nil
}
