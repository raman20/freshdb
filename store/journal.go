//go:build unix

package store

import (
	"bytes"
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
	"sync/atomic"
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
	lk     *os.File // journal.lock, shared flock for the Journal's lifetime (nil inside ShrinkJournal)
	f      *os.File // data + writer flock
	rf     *os.File // second descriptor: reader (shared) flock
	salt   [16]byte
	noSync bool

	wmu  sync.Mutex // one in-process writer at a time (flock is per descriptor)
	rfmu sync.Mutex // one in-process user of rf's shared lock at a time
	mu   sync.RWMutex
	idx  map[string]jent
	end  int64 // offset after the last valid record seen

	growing  atomic.Bool    // a background preallocation is running
	stopGrow atomic.Bool    // set by Close
	growWG   sync.WaitGroup // the background preallocation
}

type jent struct {
	off  int64
	klen int
	dlen int
}

const (
	jrnMagic   = "FRESHJ1\x00" // byte 6 is the version
	jrnHead    = 32            // magic(8) salt(16) crc(4) pad(4)
	jrnHdr     = 12            // crc(4) kind(1) pad(1) klen(2) dlen(4)
	jrnMaxKey  = 4096
	jrnMaxData = 1 << 30

	recPut  = 1
	recDel  = 2
	recDead = 3 // a put whose data was erased by a later delete
)

// Preallocation sizes (vars: the power-loss test shrinks them).
var (
	jrnChunk   int64 = 8 << 20   // preallocation step (zero-filled, then fsync)
	jrnGrowAt  int64 = 4 << 20   // start the next chunk in the background when this little is left
	jrnGrowBit int64 = 256 << 10 // background zero-fill step (one writer-lock hold each)
)

var jcrc = crc32.MakeTable(crc32.Castagnoli)

// SetUnsafeNoSync disables every sync (benchmarks only, as for FS).
func (j *Journal) SetUnsafeNoSync(on bool) { j.noSync = on }

// NewJournal opens or creates the journal store in dir. It holds a shared
// lock on dir/journal.lock until Close, so ShrinkJournal cannot run
// underneath it (and it waits for a running shrink to finish).
func NewJournal(dir string) (*Journal, error) {
	return newJournal(dir, true)
}

func newJournal(dir string, lock bool) (*Journal, error) {
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
	var lk *os.File
	if lock {
		var err error
		if lk, err = os.OpenFile(filepath.Join(dir, jrnLock), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
			return nil, err
		}
		if err := flock(lk, syscall.LOCK_SH); err != nil {
			lk.Close()
			return nil, err
		}
		// no shrink can be running now: a leftover copy is from a crash
		if err := os.Remove(filepath.Join(dir, jrnShrink)); err != nil && !errors.Is(err, os.ErrNotExist) {
			lk.Close()
			return nil, err
		}
	}
	p := filepath.Join(dir, jrnFile)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		closeLock(lk)
		return nil, err
	}
	rf, err := os.Open(p)
	if err != nil {
		f.Close()
		closeLock(lk)
		return nil, err
	}
	j := &Journal{dir: dir, lk: lk, f: f, rf: rf, idx: map[string]jent{}}
	if err := j.open(); err != nil {
		f.Close()
		rf.Close()
		closeLock(lk)
		return nil, err
	}
	return j, nil
}

func closeLock(lk *os.File) {
	if lk != nil {
		lk.Close() // releases the flock
	}
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
		if _, err := j.writeAt(h[:], 0); err != nil {
			return err
		}
		if err := j.fsync(); err != nil {
			return err
		}
		if err := syncDir(j.dir); err != nil {
			return err
		}
	case n == jrnHead && string(h[:6]) == jrnMagic[:6] && string(h[:8]) != jrnMagic:
		return fmt.Errorf("%w: %s has version %q (this build reads %q; written by a newer freshdb? upgrade the binary)",
			ErrVersion, filepath.Join(j.dir, jrnFile), h[6:7], jrnMagic[6:7])
	case n < jrnHead || string(h[:8]) != jrnMagic ||
		binary.LittleEndian.Uint32(h[24:28]) != crc32.Checksum(h[:24], jcrc):
		return fmt.Errorf("store: %s: not a journal file (%v)", jrnFile, err)
	}
	copy(j.salt[:], h[8:24])
	j.end = jrnHead
	j.mu.Lock()
	defer j.mu.Unlock()
	// Finish deletes a crash interrupted: a put followed by its delete
	// record but not yet marked dead (or whose header rewrite was torn).
	unerased, err := j.recover()
	if err != nil {
		return err
	}
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

// recover is open's scan (exclusive flock and j.mu held). It reads every
// record like scan, and also tells the three ways a record can fail its
// CRC apart, instead of stopping at the first one:
//
//   - torn tail: nothing valid follows. The bytes after the last valid
//     record are zeroed (and synced), so the file again ends in zeros.
//   - interrupted delete: a put whose dead-marking or hole punch was cut
//     short by a crash. Its delete record (durable before erasing began)
//     follows; the put is returned to be erased again.
//   - corruption: a damaged record that is not one of the above, followed
//     by a valid record. That is ErrCorrupt: opening would hide the
//     records after it, and the next append would overwrite them.
func (j *Journal) recover() ([]jent, error) {
	st, err := j.f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	// Only the deletes of keys with a damaged record before them matter
	// (a delete cut short); tracking every deleted key cost GiBs on a
	// journal with tens of millions of dead records.
	dels := map[string]int64{} // suspect key -> offset of its last delete record
	var unerased []jent
	type suspect struct {
		e   jent
		key string
	}
	var pending, middle []suspect // damaged records after / before the last valid one
	suspects := map[string]bool{}
	r := &jrnReader{f: j.f, hole: -1}
	off := j.end
	for {
		hb := r.read(off, jrnHdr)
		if hb == nil {
			break
		}
		kind, klen := hb[4], int(binary.LittleEndian.Uint16(hb[6:8]))
		dlen := int(binary.LittleEndian.Uint32(hb[8:12]))
		sum := binary.LittleEndian.Uint32(hb[:4])
		if kind < recPut || kind > recDead || klen == 0 || klen > jrnMaxKey || dlen > jrnMaxData ||
			(kind == recDel && dlen != 0) || off+int64(jrnHdr+klen+dlen) > size {
			break
		}
		var tail [jrnHdr - 4]byte
		copy(tail[:], hb[4:])
		body := klen
		if kind == recPut {
			body += dlen
		}
		b := r.read(off+jrnHdr, body)
		if b == nil {
			break
		}
		e := jent{off: off, klen: klen, dlen: dlen}
		next := off + int64(jrnHdr+klen+dlen)
		if sum != j.sum(off, tail[:], b) {
			if kind == recDel {
				break
			}
			key := string(b[:klen])
			pending = append(pending, suspect{e, key})
			suspects[key] = true
			off = next
			continue
		}
		middle, pending = append(middle, pending...), nil
		key := b[:klen]
		switch kind {
		case recPut:
			j.idx[string(key)] = e
		case recDel:
			if suspects[string(key)] {
				dels[string(key)] = off
			}
			// a put still live here was not erased: the delete was cut short
			if p, ok := j.idx[string(key)]; ok {
				unerased = append(unerased, p)
				delete(j.idx, string(key))
			}
		}
		off = next
		j.end = off
	}
	for _, s := range middle {
		if d, ok := dels[s.key]; ok && d > s.e.off {
			unerased = append(unerased, s.e) // a delete cut short: finish it
			continue
		}
		return nil, fmt.Errorf("%w: %s: record at offset %d (key %q, %d bytes) fails its checksum and is followed by valid records up to offset %d; refusing to open (it would hide them). Restore from a backup", ErrCorrupt, filepath.Join(j.dir, jrnFile), s.e.off, s.key, s.e.dlen, j.end)
	}
	if err := j.checkTail(size); err != nil {
		return nil, err
	}
	return unerased, nil
}

// jrnReader reads a journal front to back through a window, so a scan
// costs one pread per window instead of two per record. A read past the
// window (a skipped dead record's data, a large record) refills it at the
// requested offset; a window ends at the next hole. The bytes returned
// are valid until the next read.
type jrnReader struct {
	f    *os.File
	buf  []byte
	base int64 // file offset of buf[0]
	hole int64 // offset of a hole at or after base (-1: unknown)
}

const jrnWindow = 256 << 10

func (r *jrnReader) read(off int64, n int) []byte {
	if off >= r.base && off+int64(n) <= r.base+int64(len(r.buf)) {
		i := int(off - r.base)
		return r.buf[i : i+n]
	}
	w := max(n, jrnWindow)
	// Stop the window at the next hole: a long-lived journal is mostly
	// punched data of dead records (235 of 244 GB after a 6 h soak), and
	// reading through it cost more than the scan.
	if h := r.hole; h < off || h < 0 {
		r.hole = nextHole(r.f, off)
	}
	if h := r.hole - off; h >= int64(n) && h < int64(w) {
		w = int(h)
	}
	if cap(r.buf) < w {
		r.buf = make([]byte, w)
	}
	m, _ := r.f.ReadAt(r.buf[:w], off)
	r.buf, r.base = r.buf[:m], off
	if m < n {
		return nil
	}
	return r.buf[:n]
}

// checkTail makes sure nothing but zeros follows j.end. Non-zero bytes
// there are a torn append (zeroed now) unless a valid record lies among
// them: then a damaged record header hid it, which is ErrCorrupt.
func (j *Journal) checkTail(size int64) error {
	const win = 4 << 20
	buf := make([]byte, win+jrnHdr)
	first, last := int64(-1), int64(-1)
	zeros := make([]byte, len(buf))
	for at := j.end; at < size; at += win {
		if d := nextData(j.f, at); d > at { // skip holes (sparse files)
			at = max(j.end, (d-jrnHdr)/win*win)
			if at >= size {
				break
			}
		}
		n, _ := j.f.ReadAt(buf, at)
		if bytes.Equal(buf[:n], zeros[:n]) {
			continue // no record starts here: its kind byte would be non-zero
		}
		for i := 0; i < n; i++ {
			if buf[i] != 0 && at+int64(i) < size {
				if first < 0 {
					first = at + int64(i)
				}
				last = max(last, at+int64(i))
			}
		}
		for i := 0; i < min(n, win) && first >= 0; i++ {
			if p := at + int64(i); p > j.end && j.validAt(p, buf[i:n], size) {
				return fmt.Errorf("%w: %s: damaged record at offset %d, but a valid record follows at offset %d; refusing to open (it would hide it). Restore from a backup", ErrCorrupt, filepath.Join(j.dir, jrnFile), j.end, p)
			}
		}
	}
	if first < 0 {
		return nil
	}
	if err := j.zero(first, last+1-first); err != nil {
		return err
	}
	return j.sync()
}

// validAt reports whether a valid record starts at p (b holds the bytes
// from p, at least a header's worth where available).
func (j *Journal) validAt(p int64, b []byte, size int64) bool {
	if len(b) < jrnHdr {
		return false
	}
	kind, pad, klen := b[4], b[5], int(binary.LittleEndian.Uint16(b[6:8]))
	dlen := int(binary.LittleEndian.Uint32(b[8:12]))
	if kind < recPut || kind > recDead || pad != 0 || klen == 0 || klen > jrnMaxKey || dlen > jrnMaxData ||
		(kind == recDel && dlen != 0) || p+int64(jrnHdr+klen+dlen) > size {
		return false
	}
	body := klen
	if kind == recPut {
		body += dlen
	}
	rb := make([]byte, body)
	if n, _ := j.f.ReadAt(rb, p+jrnHdr); n < body {
		return false
	}
	return binary.LittleEndian.Uint32(b[:4]) == j.sum(p, b[4:jrnHdr], rb)
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
	if _, err := j.writeAt(rec, at); err != nil {
		return err
	}
	j.growAhead(at + int64(len(rec)))
	return j.sync()
}

// growAhead starts zero-filling the next chunk in the background once the
// end is within jrnGrowAt of the file size, so commits do not pay for the
// 8 MiB zero-fill and the fsync that follows it (docs/design/09).
// Caller holds the writer locks.
func (j *Journal) growAhead(end int64) {
	st, err := j.f.Stat()
	if err != nil || st.Size()-end >= jrnGrowAt || j.stopGrow.Load() || !j.growing.CompareAndSwap(false, true) {
		return
	}
	target := (st.Size()/jrnChunk + 1) * jrnChunk
	j.growWG.Add(1)
	go j.grow(target)
}

// grow extends the file to target with zeros in jrnGrowBit steps. Each step
// holds the writer locks (in-process and flock) and writes at the current
// size, so it never touches a record (spec/FreshGrow.tla: an unlocked
// grower overwrites one). Each step is written out unlocked, so no commit's
// fdatasync has to flush more than one step; one fsync at the end makes
// the new size durable.
func (j *Journal) grow(target int64) {
	defer j.growWG.Done()
	defer j.growing.Store(false)
	zero := make([]byte, jrnGrowBit)
	grew := false
	for !j.stopGrow.Load() {
		j.wmu.Lock()
		if err := flock(j.f, syscall.LOCK_EX); err != nil {
			j.wmu.Unlock()
			return
		}
		off, n := int64(-1), int64(0)
		st, err := j.f.Stat()
		if err == nil && st.Size() < target {
			off, n = st.Size(), min(jrnGrowBit, target-st.Size())
			_, err = j.writeAt(zero[:n], off)
		}
		flock(j.f, syscall.LOCK_UN)
		j.wmu.Unlock()
		if err != nil || off < 0 {
			break
		}
		grew = true
		if !j.noSync {
			writeOut(j.f, off, n)
		}
	}
	if grew && !j.noSync && !j.stopGrow.Load() {
		j.fsync()
	}
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
		if _, err := j.writeAt(zero[:n], off); err != nil {
			return err
		}
	}
	if j.noSync {
		return nil
	}
	return j.fsync() // size changed: full sync, once per chunk
}

// Sync makes every record appended so far durable, even with
// SetUnsafeNoSync (used by Migrate after a bulk copy).
func (j *Journal) Sync() error {
	unlock, err := j.lockWrite()
	if err != nil {
		return err
	}
	defer unlock()
	if err := j.fsync(); err != nil {
		return err
	}
	return syncDir(j.dir)
}

// Close releases the journal's file handles. The Journal is unusable after.
func (j *Journal) Close() error {
	j.wmu.Lock() // after an in-flight append: later ones see stopGrow
	j.stopGrow.Store(true)
	j.wmu.Unlock()
	j.growWG.Wait()
	j.wmu.Lock()
	j.rfmu.Lock()
	defer j.rfmu.Unlock()
	defer j.wmu.Unlock()
	e1, e2 := j.f.Close(), j.rf.Close()
	closeLock(j.lk)
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
	return j.traced(datasync(j.f))
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
		if attempt == 1 && j.live(e, key) {
			// still the live put of key: its bytes are damaged
			return nil, fmt.Errorf("%w: %s: object %s at offset %d fails its checksum", ErrCorrupt, filepath.Join(j.dir, jrnFile), key, e.off)
		}
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
	if _, err := j.writeAt(h, e.off); err != nil {
		return err
	}
	return j.punch(e.off+int64(len(h)), int64(e.dlen))
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

// jtrace, when set (power-loss tests only), sees every mutation of a
// journal file in order: writes (punches as writes of zeros) and syncs,
// after they succeed. Callers hold the writer locks, so the order is real.
var jtrace func(ev jev)

type jev struct {
	kind byte // 'w' write, 's' sync (a barrier for every earlier write)
	off  int64
	data []byte
}

// jfault, when set (disk-full tests only), can fail a write after letting
// a prefix of it through (op 'w', returns how many bytes land) or fail a
// sync (op 's').
var jfault func(op byte, n int) (int, error)

func (j *Journal) writeAt(b []byte, off int64) (int, error) {
	var n int
	var err error
	if m, ferr := faultAt('w', len(b)); ferr != nil {
		n, _ = j.f.WriteAt(b[:m], off)
		err = &os.PathError{Op: "write", Path: j.f.Name(), Err: ferr}
	} else {
		n, err = j.f.WriteAt(b, off)
	}
	if jtrace != nil && n > 0 {
		jtrace(jev{kind: 'w', off: off, data: append([]byte(nil), b[:n]...)})
	}
	return n, err
}

func faultAt(op byte, n int) (int, error) {
	if jfault == nil {
		return n, nil
	}
	return jfault(op, n)
}

func (j *Journal) traced(err error) error {
	if _, ferr := faultAt('s', 0); err == nil && ferr != nil {
		err = &os.PathError{Op: "sync", Path: j.f.Name(), Err: ferr}
	}
	if jtrace != nil && err == nil {
		jtrace(jev{kind: 's'})
	}
	return err
}

func (j *Journal) fsync() error { return j.traced(j.f.Sync()) }

func (j *Journal) punch(off, n int64) error {
	err := punch(j.f, off, n)
	if jtrace != nil && err == nil && n > 0 {
		jtrace(jev{kind: 'w', off: off, data: make([]byte, n)})
	}
	return err
}

func (j *Journal) zero(off, n int64) error {
	err := zeroRange(j.f, off, n)
	if jtrace != nil && err == nil && n > 0 {
		jtrace(jev{kind: 'w', off: off, data: make([]byte, n)})
	}
	return err
}
