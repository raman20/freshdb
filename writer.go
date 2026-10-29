package freshdb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

type rkey struct{ space, key string }

type commitReq struct {
	at       uint64
	ops      []format.Op
	reads    map[rkey]bool
	prefixes []rkey
	attempt  int // conflicts this transaction already lost (fairness)
	// exec, if set, is the transaction body to run inside the writer at the
	// applied head (see escalateAfter); ops/reads/at are filled from it.
	exec func(*Tx) error
	done chan commitRes
}

// size estimates the bytes r adds to a segment (an escalated transaction's
// ops are not known until it runs: counted as small).
func (r *commitReq) size() int {
	n := 64
	for _, op := range r.ops {
		n += len(op.Key) + len(op.Value) + len(op.Space) + len(op.Subject) + 16
	}
	return n
}

type commitRes struct {
	lsn      uint64
	err      error
	panicked any // exec panicked; re-raised in the caller
}

type recentCommit struct {
	lsn    uint64
	writes []rkey
}

// writer is the single sequencer of one log: it validates transactions
// (OCC, serializable), assigns LSNs, encrypts subject data, and group-
// commits a batch as ONE immutable segment. A transaction is acknowledged
// only after the segment is durable (fsync'd file / S3 PUT returned 200).
type writer struct {
	db *DB
	l  *logState
	v  *view

	mu      sync.RWMutex
	closed  bool
	paused  bool // stopped for compaction: submit returns errPaused
	ch      chan *commitReq
	done    chan struct{}
	crashed atomic.Bool

	failed      error
	head        uint64
	lastTS      int64
	recent      []recentCommit
	carry       []*commitReq // escalated requests deferred to the next batch
	recentFloor uint64
	sinceSnap   int
}

const recentMax = 8192

// maxSegBytes caps one group-commit segment: transactions that would push
// a segment past it wait for the next one (the journal store takes objects
// up to 1 GiB; one transaction alone is at most MaxTxBytes).
var maxSegBytes = 256 << 20 // var: tests shrink it

func newWriter(db *DB, l *logState, v *view) *writer {
	w := &writer{db: db, l: l, v: v, ch: make(chan *commitReq, 1024), done: make(chan struct{})}
	w.head = l.head()
	w.recentFloor = w.head
	l.mu.RLock()
	w.lastTS = l.lastTS
	l.mu.RUnlock()
	go w.loop()
	return w
}

func (w *writer) submit(r *commitReq) commitRes {
	r.done = make(chan commitRes, 1)
	w.mu.RLock()
	if w.closed {
		err := ErrClosed
		if w.paused {
			err = errPaused
		}
		w.mu.RUnlock()
		return commitRes{err: err}
	}
	t0 := time.Now()
	w.ch <- r
	w.mu.RUnlock()
	res := <-r.done
	m := &w.db.met
	switch {
	case res.err == nil:
		m.commits.Add(1)
		m.commitLat.Observe(time.Since(t0))
	case errors.Is(res.err, ErrConflict):
		m.conflicts.Add(1)
	case errors.Is(res.err, errPaused):
	default:
		m.commitErrors.Add(1)
		m.commitLat.Observe(time.Since(t0))
	}
	return res
}

func (w *writer) stop() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.ch)
	}
	w.mu.Unlock()
	<-w.done
}

// pause stops the writer for compaction; later submits get errPaused
// and retry on the writer that replaces it.
func (w *writer) pause() {
	w.mu.Lock()
	w.paused = true
	w.mu.Unlock()
	w.stop()
}

func (w *writer) crash() {
	w.crashed.Store(true)
	w.stop()
}

// CommitPolicy is the group-commit budget: after taking what is queued, a
// batch waits up to MaxDelay (from its first transaction's arrival) for
// more transactions, until it holds TargetBytes of keys and values or
// MaxBatch transactions. Waiting trades commit latency for fewer, larger
// segment writes: on an object store each segment is one billed PUT.
type CommitPolicy struct {
	MaxDelay    time.Duration // 0: never wait (the default)
	TargetBytes int           // 0 with MaxDelay set: 1 MiB
}

const defaultTargetBytes = 1 << 20

func (w *writer) loop() {
	defer close(w.done)
	var timer *time.Timer
	if w.db.opts.Commit.MaxDelay > 0 {
		timer = time.NewTimer(time.Hour)
		timer.Stop()
	}
	for {
		batch := w.carry
		w.carry = nil
		if len(batch) == 0 {
			r, ok := <-w.ch
			if !ok {
				return
			}
			batch = append(batch, r)
		}
		start := time.Now()
		batch = w.drain(batch)
		if timer != nil {
			batch = w.linger(batch, start, timer)
		}
		w.process(batch)
	}
}

// drain adds the requests already queued, up to MaxBatch.
func (w *writer) drain(batch []*commitReq) []*commitReq {
	for len(batch) < w.db.opts.MaxBatch {
		select {
		case r, ok := <-w.ch:
			if !ok {
				return batch
			}
			batch = append(batch, r)
		default:
			return batch
		}
	}
	return batch
}

// linger waits for more requests within the CommitPolicy budget. A closed
// queue ends the wait; loop sees the close on its next receive.
func (w *writer) linger(batch []*commitReq, start time.Time, timer *time.Timer) []*commitReq {
	p := w.db.opts.Commit
	size := 0
	for _, r := range batch {
		size += r.size()
	}
	for len(batch) < w.db.opts.MaxBatch && size < p.TargetBytes {
		wait := p.MaxDelay - time.Since(start)
		if wait <= 0 {
			break
		}
		timer.Reset(wait)
		select {
		case r, ok := <-w.ch:
			timer.Stop()
			if !ok {
				return batch
			}
			batch = append(batch, r)
			size += r.size()
		case <-timer.C:
			return batch
		}
	}
	return batch
}

// runExec runs an escalated transaction body at the applied head. It
// reports whether r now carries ops to validate and commit; otherwise r
// has been answered (error, panic, or read-only).
func (w *writer) runExec(r *commitReq) (ok bool) {
	tx := &Tx{db: w.db, v: w.v, at: w.head, writable: true, reads: map[rkey]bool{}, local: map[rkey]*localVal{}}
	defer func() {
		if p := recover(); p != nil {
			r.done <- commitRes{panicked: p}
			ok = false
		}
	}()
	if err := r.exec(tx); err != nil {
		r.done <- commitRes{err: err}
		return false
	}
	if len(tx.ops) == 0 {
		r.done <- commitRes{lsn: tx.at}
		return false
	}
	r.at, r.ops, r.reads, r.prefixes = tx.at, tx.ops, tx.reads, tx.prefixes
	return true
}

func (w *writer) conflict(r *commitReq, pending []recentCommit) bool {
	if len(r.reads) == 0 && len(r.prefixes) == 0 {
		return false // blind writes are always serializable
	}
	if r.at < w.recentFloor {
		return true // history needed for validation was trimmed
	}
	check := func(c recentCommit) bool {
		if c.lsn <= r.at {
			return false
		}
		for _, wr := range c.writes {
			if r.reads[wr] {
				return true
			}
			for _, p := range r.prefixes {
				if wr.space == p.space && strings.HasPrefix(wr.key, p.key) {
					return true
				}
			}
		}
		return false
	}
	for i := len(w.recent) - 1; i >= 0 && w.recent[i].lsn > r.at; i-- {
		if check(w.recent[i]) {
			return true
		}
	}
	for _, c := range pending {
		if check(c) {
			return true
		}
	}
	return false
}

func (w *writer) seal(ctx context.Context, ops []format.Op, lsn uint64) ([]format.Op, error) {
	out := make([]format.Op, len(ops))
	for i, op := range ops {
		if op.Subject != "" && op.Kind != format.OpShred && op.Kind != format.OpDel {
			a, err := w.db.keys.dek(ctx, op.Subject)
			if err != nil {
				return nil, err
			}
			ct, err := w.db.keys.seal(a, op.Value, aad(w.l.meta.ID, lsn, i, op.Space, op.Key))
			if err != nil {
				return nil, err
			}
			op.Value = ct
			op.Subject = SubjectHash(op.Subject)
		} else if op.Subject != "" {
			op.Subject = SubjectHash(op.Subject)
		}
		out[i] = op
	}
	return out, nil
}

func (w *writer) process(batch []*commitReq) {
	ctx := context.Background()
	fail := func(rs []*commitReq, err error) {
		for _, r := range rs {
			r.done <- commitRes{err: err}
		}
	}
	if w.crashed.Load() {
		fail(batch, ErrClosed)
		return
	}
	if w.failed == nil {
		w.db.mu.Lock()
		w.failed = w.db.failed
		w.db.mu.Unlock()
	}
	if w.failed != nil {
		fail(batch, w.failed)
		return
	}
	w.l.mu.RLock()
	seq := w.l.nextSeq
	chain := w.l.tip()
	w.l.mu.RUnlock()

	seg := format.EncodeHeader(format.SegHeader{Epoch: w.db.epoch, Seq: seq})
	head := w.head
	ts := w.lastTS
	var accepted []*commitReq
	var entries []*lens.Entry
	var pending []recentCommit
	// Validate transactions that already lost conflicts first: within a
	// batch the oldest retry wins a contended key, so a hot key cannot
	// starve one client while newcomers keep committing. The order only
	// chooses which of several conflicting transactions commits; each
	// accepted one is still validated against everything before it.
	sort.SliceStable(batch, func(i, j int) bool { return batch[i].attempt > batch[j].attempt })
	// Escalated transactions run here, at the applied head, before the batch
	// is validated. The first one is validated first and cannot conflict;
	// a later one that conflicts with what this batch already accepted is
	// carried to the next batch (and re-run there) instead of failing.
	kept := batch[:0]
	for _, r := range batch {
		if r.exec == nil || w.runExec(r) {
			kept = append(kept, r)
		}
	}
	batch = kept
	for _, r := range batch {
		if w.conflict(r, pending) {
			if r.exec != nil {
				w.carry = append(w.carry, r)
				continue
			}
			r.done <- commitRes{err: ErrConflict}
			continue
		}
		lsn := head + 1
		ops, err := w.seal(ctx, r.ops, lsn)
		if err != nil {
			r.done <- commitRes{err: err}
			continue
		}
		if now := w.db.opts.Clock(); now > ts {
			ts = now
		}
		f := &format.Frame{LSN: lsn, TS: ts, Prev: chain, Ops: ops}
		prev := len(seg)
		seg2, payload := format.AppendFrame(seg, f)
		if len(payload) > format.MaxFrameSize || len(ops) > format.MaxOps {
			// The decoder rejects such a frame: writing it would make the
			// log unreadable (docs/design/13-durability.md).
			seg = seg2[:prev]
			r.done <- commitRes{err: fmt.Errorf("%w: %d bytes, %d ops encoded (limits %d bytes, %d ops)",
				ErrTooLarge, len(payload), len(ops), format.MaxFrameSize, format.MaxOps)}
			continue
		}
		if len(seg2) > maxSegBytes && len(accepted) > 0 {
			seg = seg2[:prev] // full: commit it in the next segment
			w.carry = append(w.carry, r)
			continue
		}
		seg = seg2
		chain = format.ChainNext(chain, payload)
		head = lsn
		accepted = append(accepted, r)
		entries = append(entries, &lens.Entry{Log: w.l.meta.ID, LSN: lsn, TS: ts, Ops: ops})
		rc := recentCommit{lsn: lsn}
		for _, op := range ops {
			if op.Kind == format.OpPut || op.Kind == format.OpDel {
				rc.writes = append(rc.writes, rkey{op.Space, op.Key})
			}
		}
		pending = append(pending, rc)
	}
	if len(accepted) == 0 {
		return
	}
	if w.crashed.Load() {
		fail(accepted, ErrClosed)
		return
	}
	// Durability point: the whole batch becomes visible atomically or not at all.
	t0 := time.Now()
	err := putExactlyOnce(ctx, w.db.st, segKey(w.l.meta.ID, seq), seg)
	w.db.met.segmentLat.Observe(time.Since(t0))
	if err != nil {
		if errors.Is(err, store.ErrExists) {
			w.failed = w.db.fenced()
		} else if errors.Is(err, ErrUnknownOutcome) {
			w.failed = fmt.Errorf("segment %d, writer halted until reopen: %w", seq, err)
		} else {
			w.failed = fmt.Errorf("segment %d, writer halted until reopen: %w: %w", seq, ErrUnknownOutcome, err)
		}
		halt := w.failed
		w.db.met.halted.CompareAndSwap(nil, &halt)
		w.db.logf("freshdb: writer for %s halted: %v", w.l.meta.ID, w.failed)
		fail(accepted, w.failed)
		return
	}
	w.db.met.segments.Add(1)
	w.db.met.segmentBytes.Add(uint64(len(seg)))
	w.l.mu.Lock()
	w.l.addSeg(seq, entries[0].LSN, uint64(len(entries)), ts, chain)
	w.l.nextSeq = seq + 1
	w.l.lastEp = w.db.epoch
	w.l.mu.Unlock()
	w.v.mu.Lock()
	for _, e := range entries {
		w.v.apply(e)
	}
	w.v.mu.Unlock()
	w.db.bump()
	w.head, w.lastTS = head, ts
	w.recent = append(w.recent, pending...)
	// Trim to recentMax only once twice that is held, so the copy is
	// amortized instead of moving recentMax entries on every batch.
	if over := len(w.recent) - recentMax; over >= recentMax {
		w.recentFloor = w.recent[over-1].lsn
		w.recent = append([]recentCommit{}, w.recent[over:]...)
	}
	// Snapshots are taken before acking so that, with one client, every
	// store call happens in a deterministic order (simulation replayability).
	w.sinceSnap += len(accepted)
	if w.db.opts.SnapshotEvery > 0 && w.sinceSnap >= w.db.opts.SnapshotEvery {
		w.sinceSnap = 0
		if err := w.db.writeSnapshot(ctx, w.l, w.v); err != nil {
			w.db.met.snapshotErrors.Add(1)
			w.db.logf("freshdb: snapshot failed (non-fatal): %v", err)
		} else {
			w.db.met.snapshots.Add(1)
		}
	}
	for i, r := range accepted {
		r.done <- commitRes{lsn: entries[i].LSN}
	}
}
