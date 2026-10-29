package freshdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/store"
)

// Segment-index checkpoints let recovery skip reading the segments they
// cover. Recovery otherwise GETs every segment of a log to rebuild its
// in-memory index (segIdx per segment, chain tip, epoch); with a
// checkpoint it reads the newest covered segment (to bind the checkpoint
// to the log's bytes) and the segments after it.
//
// A checkpoint is a cache, like a snapshot: it is written create-only next
// to the segments (log/<id>/<seq>.idx, seq = the last segment it covers)
// with content that is a pure function of the log prefix, so racing
// writers write identical bytes. One that is missing, damaged, stale or
// does not match the log is ignored (with a log line) and the log is
// scanned in full. What it gives up: segments it covers are not re-read
// at open, so latent damage in them is found when they are read (readSeg
// checks every frame's CRC, sequence and first LSN) or by Verify, which
// still reads everything and cross-checks every checkpoint.
//
// Layout (formats.md):
//
//	magic "FRDBIDX1" | idLen u8 | log id | startSeq u64 | start [32]byte
//	| lastEp u64 | lastTS i64 | tip [32]byte | count u32
//	| count x {seq u64 | first u64 | n u64} | crc32c u32
//
// start is the chain head the covered frames extend (the log's base) and
// startSeq the first sequence number scanned: a checkpoint written before
// a compaction does not match the compacted log.

var idxMagic = [8]byte{'F', 'R', 'D', 'B', 'I', 'D', 'X', '1'}

// maxCheckpointTries bounds how many checkpoints (newest first) recovery
// tries before scanning the whole log.
const maxCheckpointTries = 3

type checkpoint struct {
	log      string
	startSeq uint64
	start    [32]byte
	lastEp   uint64
	lastTS   int64
	tip      [32]byte
	segs     []segIdx
}

func ckptKey(log string, seq uint64) string { return fmt.Sprintf("log/%s/%020d.idx", log, seq) }

// last is the last segment the checkpoint covers (segs is never empty).
func (c *checkpoint) last() segIdx { return c.segs[len(c.segs)-1] }

func encodeCheckpoint(c *checkpoint) []byte {
	b := make([]byte, 0, len(idxMagic)+1+len(c.log)+8+32+8+8+32+4+24*len(c.segs)+4)
	b = append(b, idxMagic[:]...)
	b = append(b, byte(len(c.log)))
	b = append(b, c.log...)
	b = binary.LittleEndian.AppendUint64(b, c.startSeq)
	b = append(b, c.start[:]...)
	b = binary.LittleEndian.AppendUint64(b, c.lastEp)
	b = binary.LittleEndian.AppendUint64(b, uint64(c.lastTS))
	b = append(b, c.tip[:]...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(c.segs)))
	for _, s := range c.segs {
		b = binary.LittleEndian.AppendUint64(b, s.seq)
		b = binary.LittleEndian.AppendUint64(b, s.first)
		b = binary.LittleEndian.AppendUint64(b, s.n)
	}
	return binary.LittleEndian.AppendUint32(b, format.CRC(b))
}

var errCheckpoint = errors.New("not a valid segment-index checkpoint")

// decodeCheckpoint checks framing, checksum and the internal invariants
// of the index: at least one segment, ascending sequence numbers from
// startSeq, every segment non-empty, LSNs contiguous.
func decodeCheckpoint(b []byte) (*checkpoint, error) {
	const fixed = len(idxMagic) + 1 + 8 + 32 + 8 + 8 + 32 + 4 + 4
	if len(b) < fixed || !bytes.Equal(b[:len(idxMagic)], idxMagic[:]) {
		return nil, fmt.Errorf("%w: bad magic or size %d", errCheckpoint, len(b))
	}
	body := b[:len(b)-4]
	if format.CRC(body) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, fmt.Errorf("%w: checksum mismatch", errCheckpoint)
	}
	r := body[len(idxMagic):]
	idLen := int(r[0])
	if len(r) < 1+idLen+8+32+8+8+32+4 {
		return nil, fmt.Errorf("%w: truncated header", errCheckpoint)
	}
	c := &checkpoint{log: string(r[1 : 1+idLen])}
	r = r[1+idLen:]
	c.startSeq, r = binary.LittleEndian.Uint64(r), r[8:]
	copy(c.start[:], r)
	r = r[32:]
	c.lastEp, r = binary.LittleEndian.Uint64(r), r[8:]
	c.lastTS, r = int64(binary.LittleEndian.Uint64(r)), r[8:]
	copy(c.tip[:], r)
	r = r[32:]
	count, r := uint64(binary.LittleEndian.Uint32(r)), r[4:]
	if count == 0 || count*24 != uint64(len(r)) {
		return nil, fmt.Errorf("%w: %d entries in %d bytes", errCheckpoint, count, len(r))
	}
	c.segs = make([]segIdx, count)
	for i := range c.segs {
		s := segIdx{seq: binary.LittleEndian.Uint64(r), first: binary.LittleEndian.Uint64(r[8:]), n: binary.LittleEndian.Uint64(r[16:])}
		r = r[24:]
		switch {
		case s.n == 0:
			return nil, fmt.Errorf("%w: segment %d has no frames", errCheckpoint, s.seq)
		case i == 0 && s.seq < c.startSeq,
			i > 0 && s.seq <= c.segs[i-1].seq:
			return nil, fmt.Errorf("%w: segment %d out of order", errCheckpoint, s.seq)
		case i > 0 && s.first != c.segs[i-1].first+c.segs[i-1].n:
			return nil, fmt.Errorf("%w: segment %d starts at LSN %d, not contiguous", errCheckpoint, s.seq, s.first)
		case s.first+s.n < s.first:
			return nil, fmt.Errorf("%w: segment %d LSN overflow", errCheckpoint, s.seq)
		}
		c.segs[i] = s
	}
	return c, nil
}

// firstSeq is the first segment sequence number of l (after compaction,
// the base's).
func (l *logState) firstSeq() uint64 {
	if b := l.meta.Base; b != nil {
		return b.Seq
	}
	return 1
}

// checkpointOf snapshots l's index as a checkpoint, or nil if l has no
// frames of its own.
func (l *logState) checkpointOf() *checkpoint {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segs) == 0 {
		return nil
	}
	return &checkpoint{log: l.meta.ID, startSeq: l.firstSeq(), start: l.base, lastEp: l.lastEp,
		lastTS: l.lastTS, tip: l.tipCh, segs: slices.Clone(l.segs)}
}

// writeCheckpoint writes a checkpoint of l's index. Called by l's writer
// right after a commit, so the last indexed segment is the one it just
// wrote and lastEp is that segment's epoch. ErrExists means another
// writer covered the same prefix: by construction the same bytes, and if
// not, recovery rejects the one that does not match.
func (db *DB) writeCheckpoint(ctx context.Context, l *logState) error {
	c := l.checkpointOf()
	if c == nil {
		return nil
	}
	err := putExactlyOnce(ctx, db.st, ckptKey(c.log, c.last().seq), encodeCheckpoint(c))
	if errors.Is(err, store.ErrExists) {
		return nil
	}
	return err
}

// loadCheckpoint seeds a freshly loaded l (nothing scanned yet) from its
// newest usable checkpoint, so recovery reads only the segments after it.
// Checkpoints that do not check out are skipped with a log line.
func (db *DB) loadCheckpoint(ctx context.Context, l *logState) error {
	if db.fullScan {
		return nil
	}
	dir := "log/" + l.meta.ID + "/"
	keys, err := db.st.List(ctx, dir)
	if err != nil {
		return err
	}
	var seqs []uint64
	for _, k := range keys {
		if seq, ok := parseNum(k, dir, ".idx"); ok && seq >= l.firstSeq() {
			seqs = append(seqs, seq)
		}
	}
	slices.Sort(seqs)
	for i := len(seqs) - 1; i >= 0 && i >= len(seqs)-maxCheckpointTries; i-- {
		c, err := db.checkCheckpoint(ctx, l, seqs[i])
		if err == nil {
			l.mu.Lock()
			l.applyCheckpoint(c)
			l.mu.Unlock()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		db.logf("freshdb: ignoring checkpoint %s (scanning the segments instead): %v", ckptKey(l.meta.ID, seqs[i]), err)
	}
	return nil
}

// checkCheckpoint reads checkpoint seq and checks that it continues l from
// where l starts and that it matches segment seq, the last one it covers,
// byte for byte: frame count, first LSN, chain head, timestamp, epoch.
func (db *DB) checkCheckpoint(ctx context.Context, l *logState, seq uint64) (*checkpoint, error) {
	var (
		c    *checkpoint
		cerr error
	)
	_, err := getStable(ctx, db.st, ckptKey(l.meta.ID, seq), func(b []byte) readVerdict {
		c, cerr = decodeCheckpoint(b)
		return okIf(cerr == nil)
	})
	if err == nil {
		err = cerr // stable damage is returned as a read, not an error
	}
	if err != nil {
		return nil, err
	}
	last := c.last()
	switch {
	case c.log != l.meta.ID || last.seq != seq:
		return nil, fmt.Errorf("%w: it covers log %s up to segment %d", errCheckpoint, c.log, last.seq)
	case c.startSeq != l.firstSeq() || c.start != l.base:
		return nil, fmt.Errorf("%w: it starts at segment %d, not where the log starts (%d)", errCheckpoint, c.startSeq, l.firstSeq())
	case c.segs[0].first != l.meta.ParentLSN+l.n+1:
		return nil, fmt.Errorf("%w: first LSN %d, log continues at %d", errCheckpoint, c.segs[0].first, l.meta.ParentLSN+l.n+1)
	}
	if err := db.matchSegment(ctx, l, c); err != nil {
		return nil, err
	}
	return c, nil
}

// matchSegment checks c against the last segment it covers: frame count,
// first LSN, chain head, timestamp and epoch.
func (db *DB) matchSegment(ctx context.Context, l *logState, c *checkpoint) error {
	last := c.last()
	var (
		h      format.SegHeader
		frames []format.Decoded
		derr   error
	)
	_, err := getStable(ctx, db.st, segKey(l.meta.ID, last.seq), func(b []byte) readVerdict {
		h, frames, _, derr = format.DecodeSegment(b)
		return segVerdict(h, derr, last.seq)
	})
	if err != nil {
		return fmt.Errorf("segment %d: %w", last.seq, err)
	}
	if derr != nil || h.Seq != last.seq || uint64(len(frames)) != last.n || frames[0].Frame.LSN != last.first {
		return fmt.Errorf("%w: segment %d does not hold LSNs %d..%d intact (%d frames, %v)", errCheckpoint, last.seq, last.first, last.first+last.n-1, len(frames), derr)
	}
	tip := frames[0].Frame.Prev
	for _, d := range frames {
		tip = format.ChainNext(tip, d.Payload)
	}
	if tip != c.tip || frames[len(frames)-1].Frame.TS != c.lastTS || h.Epoch != c.lastEp {
		return fmt.Errorf("%w: segment %d: chain head, timestamp or epoch differ", errCheckpoint, last.seq)
	}
	return nil
}

// applyCheckpoint installs c as l's index. Caller holds l.mu (write) and
// checked c against l.
func (l *logState) applyCheckpoint(c *checkpoint) {
	last := c.last()
	l.segs = c.segs
	l.tipCh = c.tip
	l.n = last.first + last.n - 1 - l.meta.ParentLSN
	l.lastTS = c.lastTS
	l.lastEp = c.lastEp
	l.nextSeq = last.seq + 1
}

// verifyCheckpoints cross-checks l's checkpoints (among keys, the objects
// of l's directory) against l as recovered by a full scan. A checkpoint is
// a cache, so a bad one is a warning: open skips it.
func (db *DB) verifyCheckpoints(ctx context.Context, l *logState, keys []string) []string {
	dir := "log/" + l.meta.ID + "/"
	l.mu.RLock()
	full := l.segs
	l.mu.RUnlock()
	var warn []string
	for _, k := range keys {
		seq, ok := parseNum(k, dir, ".idx")
		if !ok || seq < l.firstSeq() {
			continue // below a compaction base: garbage the next GC deletes
		}
		err := db.matchCheckpoint(ctx, l, k, seq, full)
		if err != nil {
			warn = append(warn, fmt.Sprintf("checkpoint %s is unusable (%v; open scans the segments instead)", k, err))
		}
	}
	return warn
}

func (db *DB) matchCheckpoint(ctx context.Context, l *logState, key string, seq uint64, full []segIdx) error {
	b, err := db.st.Get(ctx, key)
	if err != nil {
		return err
	}
	c, err := decodeCheckpoint(b)
	if err != nil {
		return err
	}
	if c.log != l.meta.ID || c.last().seq != seq || c.startSeq != l.firstSeq() || c.start != l.base {
		return fmt.Errorf("%w: it does not belong at this place in this log", errCheckpoint)
	}
	if len(c.segs) > len(full) || !slices.Equal(c.segs, full[:len(c.segs)]) {
		return fmt.Errorf("%w: its index differs from the segments", errCheckpoint)
	}
	last := c.last()
	tip, err := l.chainAt(ctx, last.first+last.n-1)
	if err != nil {
		return err
	}
	if tip != c.tip {
		return fmt.Errorf("%w: chain head differs", errCheckpoint)
	}
	return db.matchSegment(ctx, l, c)
}
