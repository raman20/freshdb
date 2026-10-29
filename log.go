package freshdb

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// logState is the in-memory index of one log (one branch lineage segment).
// Segments are immutable objects log/<id>/<seq>.seg, seq dense from 1.
// Frames are not kept in memory: lenses fold them as they are written or
// recovered, and anything that needs them again (time-travel views of
// other lenses, Entries, Verify, index builds) re-reads the segments.
type logState struct {
	meta    LogMeta
	parent  *logState
	st      store.Store
	mu      sync.RWMutex
	segs    []segIdx // segments holding at least one frame, ascending
	tipCh   [32]byte // chain head after the last indexed frame (when segs != nil)
	n       uint64   // own frames: LSN = meta.ParentLSN+1 ... ParentLSN+n
	lastTS  int64
	base    [32]byte // chain head at ParentLSN (from parent)
	nextSeq uint64
	lastEp  uint64
	repairs int
}

// segIdx locates the frames of one segment. It is 24 bytes, one per
// commit batch, so it does not keep the chain head after the segment: that
// is the next segment's first Prev (or tipCh for the last segment).
type segIdx struct {
	seq   uint64
	first uint64 // LSN of its first frame
	n     uint64 // frame count
}

func segKey(log string, seq uint64) string { return fmt.Sprintf("log/%s/%020d.seg", log, seq) }

func (l *logState) head() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.meta.ParentLSN + l.n
}

// based returns the log in this lineage that has a compaction base
// (lineages stop at a based log), or nil.
func (l *logState) based() *logState {
	for x := l; x != nil; x = x.parent {
		if x.meta.Base != nil {
			return x
		}
	}
	return nil
}

// floor is the LSN of the lineage's compaction base (0 if none): history
// at or below it exists only as the base image.
func (l *logState) floor() uint64 {
	if b := l.based(); b != nil {
		return b.meta.Base.LSN
	}
	return 0
}

// source finds where a version's bytes live and which log sealed them:
// the based log for anything at or below the compaction base (its value
// chunks, re-sealed under its ID), else the segment holding the LSN.
func (l *logState) source(ver lens.Version) (*logState, uint64, bool) {
	if b := l.based(); b != nil && (ver.Chunk != 0 || ver.LSN <= b.meta.Base.LSN) {
		return b, 0, true
	}
	x, s, ok := l.locate(ver.LSN)
	return x, s.seq, ok
}

// tip returns the chain head after the last frame. Caller holds l.mu.
func (l *logState) tip() [32]byte {
	if len(l.segs) > 0 {
		return l.tipCh
	}
	return l.base
}

// addSeg indexes a segment's frames. Caller holds l.mu (write).
func (l *logState) addSeg(seq, first, n uint64, ts int64, chain [32]byte) {
	l.segs = append(l.segs, segIdx{seq: seq, first: first, n: n})
	l.tipCh = chain
	l.n += n
	l.lastTS = ts
}

// locate returns the log (in this lineage) and segment holding lsn.
func (l *logState) locate(lsn uint64) (*logState, segIdx, bool) {
	for x := l; x != nil; x = x.parent {
		if lsn > x.meta.ParentLSN {
			x.mu.RLock()
			defer x.mu.RUnlock()
			i := sort.Search(len(x.segs), func(i int) bool { return x.segs[i].first+x.segs[i].n > lsn })
			if i == len(x.segs) || x.segs[i].first > lsn {
				return nil, segIdx{}, false
			}
			return x, x.segs[i], true
		}
		if lsn == 0 {
			break
		}
	}
	return nil, segIdx{}, false
}

// readSeg reads and decodes one indexed segment of this log.
func (l *logState) readSeg(ctx context.Context, s segIdx) ([]format.Decoded, error) {
	var frames []format.Decoded
	var derr error
	_, err := getStable(ctx, l.st, segKey(l.meta.ID, s.seq), func(b []byte) readVerdict {
		var h format.SegHeader
		h, frames, _, derr = format.DecodeSegment(b)
		switch {
		case !errors.Is(derr, format.ErrHeader) && h.Seq != s.seq,
			len(frames) > 0 && frames[0].Frame.LSN != s.first:
			return readMisread // another segment, of this log or another
		case uint64(len(frames)) >= s.n:
			return readOK
		}
		return readDamaged
	})
	if err != nil {
		return nil, err
	}
	if uint64(len(frames)) < s.n {
		return nil, fmt.Errorf("%w: log %s segment %d: %d of %d frames readable: %v", ErrCorrupt, l.meta.ID, s.seq, len(frames), s.n, derr)
	}
	frames = frames[:s.n]
	if frames[0].Frame.LSN != s.first {
		return nil, fmt.Errorf("%w: log %s segment %d starts at LSN %d, indexed %d", ErrCorrupt, l.meta.ID, s.seq, frames[0].Frame.LSN, s.first)
	}
	return frames, nil
}

func (l *logState) entry(d format.Decoded) *lens.Entry {
	return &lens.Entry{Log: l.meta.ID, LSN: d.Frame.LSN, TS: d.Frame.TS, Ops: d.Frame.Ops}
}

// chainAt returns the hash-chain head after LSN lsn in this lineage.
func (l *logState) chainAt(ctx context.Context, lsn uint64) ([32]byte, error) {
	if lsn == 0 {
		return [32]byte{}, nil
	}
	if b := l.based(); b != nil && lsn <= b.meta.Base.LSN {
		if lsn == b.meta.Base.LSN {
			return b.base, nil
		}
		return [32]byte{}, fmt.Errorf("%w: LSN %d < base %d", ErrCompacted, lsn, b.meta.Base.LSN)
	}
	x, s, ok := l.locate(lsn)
	if !ok {
		return [32]byte{}, fmt.Errorf("%w: LSN %d not in log %s", ErrBadLSN, lsn, l.meta.ID)
	}
	if lsn == s.first+s.n-1 { // the chain after a segment's last frame
		x.mu.RLock()
		i := sort.Search(len(x.segs), func(i int) bool { return x.segs[i].first > lsn })
		tip, last := x.tipCh, i == len(x.segs)
		var next segIdx
		if !last {
			next = x.segs[i]
		}
		x.mu.RUnlock()
		if last {
			return tip, nil
		}
		frames, err := x.readSeg(ctx, next) // is the next segment's first Prev
		if err != nil {
			return [32]byte{}, err
		}
		return frames[0].Frame.Prev, nil
	}
	frames, err := x.readSeg(ctx, s)
	if err != nil {
		return [32]byte{}, err
	}
	return frames[lsn-s.first+1].Frame.Prev, nil // the next frame names it
}

// each calls fn with every entry visible on this log up to LSN upto, oldest
// first (parent's entries up to ParentLSN, then our own), reading segments.
func (l *logState) each(ctx context.Context, upto uint64, fn func(*lens.Entry) error) error {
	if l.parent != nil && l.meta.ParentLSN > 0 {
		if err := l.parent.each(ctx, min(upto, l.meta.ParentLSN), fn); err != nil {
			return err
		}
	}
	return l.eachOwn(ctx, upto, fn)
}

// eachOwn is each restricted to this log's own frames.
func (l *logState) eachOwn(ctx context.Context, upto uint64, fn func(*lens.Entry) error) error {
	l.mu.RLock()
	segs := l.segs
	l.mu.RUnlock()
	for _, s := range segs {
		if s.first > upto {
			break
		}
		frames, err := l.readSeg(ctx, s)
		if err != nil {
			return err
		}
		for _, d := range frames {
			if d.Frame.LSN > upto {
				break
			}
			if err := fn(l.entry(d)); err != nil {
				return err
			}
		}
	}
	return nil
}

// lineage returns every entry visible on this log up to LSN upto.
func (l *logState) lineage(ctx context.Context, upto uint64) ([]*lens.Entry, error) {
	var out []*lens.Entry
	err := l.each(ctx, upto, func(e *lens.Entry) error { out = append(out, e); return nil })
	return out, err
}

// entryAt returns the entry with the given LSN in this lineage.
func (l *logState) entryAt(ctx context.Context, lsn uint64) (*lens.Entry, error) {
	if lsn <= l.floor() {
		return nil, fmt.Errorf("%w: LSN %d", ErrCompacted, lsn)
	}
	x, s, ok := l.locate(lsn)
	if !ok {
		return nil, ErrNotFound
	}
	frames, err := x.readSeg(ctx, s)
	if err != nil {
		return nil, err
	}
	return x.entry(frames[lsn-s.first]), nil
}

// recoverLog reads a log's segments. A damaged segment is an error
// (ErrCorrupt): we never silently drop acknowledged data. Only with
// Options.RepairTail, and only for the final segment of the writable
// (lease-holding) log, is the damage accepted: a create-only repair note
// (repairKey) records the segment's exact bytes and how many frames are
// kept, and the fence that follows makes it a non-tail segment. Nothing is
// deleted or rewritten, so a crash or a racing zombie cannot lose the valid
// prefix or swap in other frames. A damaged non-tail segment is accepted
// only with a note that matches its bytes.
func (db *DB) recoverLog(ctx context.Context, l *logState, writable bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for range 64 {
		if grew, err := db.scanSegments(ctx, l, writable); !grew {
			return err
		}
	}
	return fmt.Errorf("%w: log %s keeps growing during recovery", ErrFenced, l.meta.ID)
}

// scanSegments reads l's segments from l.nextSeq on (l.mu held). grew: a
// segment appeared past the end while we read; scan again.
func (db *DB) scanSegments(ctx context.Context, l *logState, writable bool) (grew bool, err error) {
	st := db.st
	// resume after what we already have (re-recovery after a fencing race)
	expectSeq := l.nextSeq
	if expectSeq == 0 {
		expectSeq = 1
	}
	// Segments are dense, so the log is read by sequence number until one
	// is not found; GETs are authoritative. The LIST only cross-checks
	// that nothing exists past that point (a hole in the log). A stale
	// LIST can omit new keys but not invent them, so it can neither
	// shorten the log nor make a non-tail segment look like the tail.
	for seq := expectSeq; ; seq++ {
		k := segKey(l.meta.ID, seq)
		var (
			h      format.SegHeader
			frames []format.Decoded
			valid  int
			derr   error
		)
		data, err := getStable(ctx, st, k, func(b []byte) readVerdict {
			h, frames, valid, derr = format.DecodeSegment(b)
			return segVerdict(h, derr, seq)
		})
		if errors.Is(err, store.ErrNotFound) {
			break
		}
		if errors.Is(err, store.ErrCorrupt) {
			return false, fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		if err != nil {
			return false, err
		}
		if derr != nil && !(errors.Is(derr, format.ErrTorn) || errors.Is(derr, format.ErrHeader)) {
			return false, fmt.Errorf("%s: %w", k, derr)
		}
		if derr == nil || errors.Is(derr, format.ErrTorn) {
			if h.Seq != seq {
				return false, fmt.Errorf("%w: segment %s header seq %d", ErrCorrupt, k, h.Seq)
			}
			if h.Epoch < l.lastEp {
				return false, fmt.Errorf("%w: segment %s epoch %d < %d (fencing violated)", ErrCorrupt, k, h.Epoch, l.lastEp)
			}
		}
		if derr != nil {
			tail, err := segAbsent(ctx, st, segKey(l.meta.ID, seq+1))
			if err != nil {
				return false, err
			}
			stop, err := db.damagedSeg(ctx, l, k, seq, tail, writable, data, h, len(frames), valid, derr)
			if err != nil {
				return false, err
			}
			if stop { // read-only view ends before this segment
				l.nextSeq = expectSeq
				return false, nil
			}
		}
		if len(frames) > 0 {
			first := l.meta.ParentLSN + l.n + 1
			prev := l.tip()
			for i, d := range frames {
				f := d.Frame
				if want := first + uint64(i); f.LSN != want {
					return false, fmt.Errorf("%w: log %s: LSN %d, expected %d (duplicate or gap)", ErrCorrupt, l.meta.ID, f.LSN, want)
				}
				if f.Prev != prev {
					return false, fmt.Errorf("%w: log %s: hash chain broken at LSN %d", ErrCorrupt, l.meta.ID, f.LSN)
				}
				prev = format.ChainNext(prev, d.Payload)
			}
			l.addSeg(seq, first, uint64(len(frames)), frames[len(frames)-1].Frame.TS, prev)
		}
		if h.Epoch > l.lastEp {
			l.lastEp = h.Epoch
		}
		expectSeq = seq + 1
	}
	l.nextSeq = expectSeq
	// a writer (a zombie, or the next owner) may append while we read
	if grew, err := checkNoHole(ctx, st, l.meta.ID, expectSeq); grew || err != nil {
		return grew, err
	}
	if writable && l.lastEp > db.epoch {
		// A newer writer already fenced this log (found by TLC, FreshLog.tla:
		// a slow opener must not append an older-epoch fence after a newer
		// one, which would make the log unrecoverable).
		return false, fmt.Errorf("%w: log %s has epoch %d > ours %d", ErrFenced, l.meta.ID, l.lastEp, db.epoch)
	}
	return false, nil
}

// segVerdict judges a read of segment seq: a readable header that names
// another sequence number is a misread, not damage.
func segVerdict(h format.SegHeader, derr error, seq uint64) readVerdict {
	switch {
	case errors.Is(derr, format.ErrHeader):
		return readDamaged
	case h.Seq != seq:
		return readMisread
	case derr == nil:
		return readOK
	}
	return readDamaged
}

// segAbsent reports whether key does not exist.
func segAbsent(ctx context.Context, st store.Store, key string) (bool, error) {
	_, err := st.Get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return true, nil
	}
	return false, err
}

// checkNoHole checks that no segment exists past end, the first sequence
// number that was not found. grew: segment end exists now (written after
// our read), so the caller reads on. A listed segment past a still-absent
// end is a hole: ErrCorrupt.
func checkNoHole(ctx context.Context, st store.Store, id string, end uint64) (grew bool, err error) {
	prefix := "log/" + id + "/"
	keys, err := st.List(ctx, prefix)
	if err != nil {
		return false, err
	}
	for _, k := range keys {
		if seq, ok := parseNum(k, prefix, ".seg"); ok && seq >= end {
			absent, err := segAbsent(ctx, st, segKey(id, end))
			if err != nil || !absent {
				return !absent, err
			}
			return false, fmt.Errorf("%w: log %s: missing segment %d (found %d)", ErrCorrupt, id, end, seq)
		}
	}
	return false, nil
}

// damagedSeg decides whether recovery may use the valid prefix (nframes
// frames, valid bytes) of damaged segment k. stop: a read-only view ends
// the log before k. Otherwise nil means "use the prefix".
func (db *DB) damagedSeg(ctx context.Context, l *logState, k string, seq uint64, tail, writable bool, data []byte, h format.SegHeader, nframes, valid int, derr error) (stop bool, err error) {
	note := repairNote(seq, data, nframes, valid)
	isNote := func(b []byte) readVerdict { return okIf(string(b) == string(note)) }
	switch got, err := getStable(ctx, db.st, repairKey(l.meta.ID, seq), isNote); {
	case err == nil && string(got) == string(note):
		return false, nil // repaired by an earlier open
	case err == nil:
		return false, fmt.Errorf("%w: segment %s does not match its repair note (damaged again?)", ErrCorrupt, k)
	case !errors.Is(err, store.ErrNotFound):
		return false, err
	}
	if !tail {
		return false, fmt.Errorf("%w: damaged segment %s is not the tail: %v", ErrCorrupt, k, derr)
	}
	if !db.opts.RepairTail {
		return false, fmt.Errorf("%w: final segment %s of log %s is damaged (%v; %d valid frames in the first %d of %d bytes). Store writes are atomic, so this is corruption and may hide acknowledged commits; the segment was left as is. Restore from a backup, or open with WithRepairTail (CLI: --repair-tail) to keep the valid prefix (the damaged object stays in place)", ErrCorrupt, k, l.meta.ID, derr, nframes, valid, len(data))
	}
	if !writable {
		// read-only view: use the valid prefix, or stop before an
		// unreadable header
		return errors.Is(derr, format.ErrHeader), nil
	}
	if l.lastEp > db.epoch || h.Epoch > db.epoch {
		return false, fmt.Errorf("%w: log %s superseded (epoch %d)", ErrFenced, l.meta.ID, max(l.lastEp, h.Epoch))
	}
	if err := putExactlyOnce(ctx, db.st, repairKey(l.meta.ID, seq), note); err != nil {
		if errors.Is(err, store.ErrExists) {
			return false, fmt.Errorf("%w: segment %s: a different repair note exists", ErrCorrupt, k)
		}
		return false, err
	}
	l.repairs++
	db.logf("freshdb: repaired damaged tail %s: %d valid frames kept, frames after byte %d dropped (note %s)", k, nframes, valid, repairKey(l.meta.ID, seq))
	return false, nil
}

// repairKey names the repair note of segment seq (see recoverLog).
func repairKey(log string, seq uint64) string {
	return fmt.Sprintf("log/%s/%020d.repair", log, seq)
}

// repairNote is the exact content of a repair note: deterministic in the
// segment bytes, so racing openers write identical notes, and a segment
// that changes after its repair no longer matches.
func repairNote(seq uint64, data []byte, nframes, valid int) []byte {
	return fmt.Appendf(nil, "freshdb-repair v1 seq=%d frames=%d valid=%d size=%d sha256=%x\n", seq, nframes, valid, len(data), sha256.Sum256(data))
}

// fence writes an empty segment with our epoch at the next sequence number.
// Any older writer (zombie) will fail its next create-only segment write.
// If a zombie got there first, absorb its segment and retry.
func (db *DB) fence(ctx context.Context, l *logState) error {
	for i := 0; i < 64; i++ {
		l.mu.RLock()
		seq := l.nextSeq
		l.mu.RUnlock()
		hdr := format.EncodeHeader(format.SegHeader{Epoch: db.epoch, Seq: seq})
		err := putExactlyOnce(ctx, db.st, segKey(l.meta.ID, seq), hdr)
		if err == nil {
			l.mu.Lock()
			l.nextSeq = seq + 1
			l.lastEp = db.epoch
			l.mu.Unlock()
			return nil
		}
		if !errors.Is(err, store.ErrExists) {
			return err
		}
		if err := db.recoverLog(ctx, l, true); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: could not fence log %s", ErrFenced, l.meta.ID)
}
