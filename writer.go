package freshdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

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
		w.mu.RUnlock()
		return commitRes{err: ErrClosed}
	}
	w.ch <- r
	w.mu.RUnlock()
	return <-r.done
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

func (w *writer) crash() {
	w.crashed.Store(true)
	w.stop()
}

func (w *writer) loop() {
	defer close(w.done)
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
	drain:
		for len(batch) < w.db.opts.MaxBatch {
			select {
			case r2, ok := <-w.ch:
				if !ok {
					break drain
				}
				batch = append(batch, r2)
			default:
				break drain
			}
		}
		w.process(batch)
	}
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
		var payload []byte
		seg, payload = format.AppendFrame(seg, f)
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
	err := putExactlyOnce(ctx, w.db.st, segKey(w.l.meta.ID, seq), seg)
	if err != nil {
		if errors.Is(err, store.ErrExists) {
			w.failed = ErrFenced
		} else {
			w.failed = fmt.Errorf("%w (segment %d): %v", ErrUnknownOutcome, seq, err)
		}
		w.db.logf("freshdb: writer for %s halted: %v", w.l.meta.ID, w.failed)
		fail(accepted, w.failed)
		return
	}
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
		if err := w.db.writeSnapshot(ctx, w.l.meta.ID, w.v); err != nil {
			w.db.logf("freshdb: snapshot failed (non-fatal): %v", err)
		}
	}
	for i, r := range accepted {
		r.done <- commitRes{lsn: entries[i].LSN}
	}
}

// ---- snapshots (KV + topic checkpoint) ----

type snapshot struct {
	lsn   uint64
	kv    *lens.KV
	topic *lens.Topic
	drop  []string
}

func encodeSnapshot(lsn uint64, kv, topic []byte) []byte {
	b := binary.LittleEndian.AppendUint64(nil, lsn)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(kv)))
	b = append(b, kv...)
	b = append(b, topic...)
	return binary.LittleEndian.AppendUint32(b, format.CRC(b))
}

func (db *DB) writeSnapshot(ctx context.Context, logID string, v *view) error {
	v.mu.RLock()
	lsn := v.head
	b := encodeSnapshot(lsn, v.kv.Snapshot(), v.topic.Snapshot())
	v.mu.RUnlock()
	key := fmt.Sprintf("snap/%s/%020d.snap", logID, lsn)
	if err := putExactlyOnce(ctx, db.st, key, b); err != nil && !errors.Is(err, store.ErrExists) {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.commitManifest(ctx, func(m *Manifest) error {
		if lm := m.Logs[logID]; lm != nil && lm.SnapLSN < lsn {
			lm.SnapLSN, lm.SnapKey = lsn, key
		}
		return nil
	})
}

func (db *DB) loadSnapshot(ctx context.Context, l *logState) *snapshot {
	lm := db.man.Logs[l.meta.ID]
	if lm == nil || lm.SnapKey == "" {
		return nil
	}
	b, err := db.st.Get(ctx, lm.SnapKey)
	if err != nil || len(b) < 20 || binary.LittleEndian.Uint32(b[len(b)-4:]) != format.CRC(b[:len(b)-4]) {
		db.logf("freshdb: ignoring unreadable snapshot %s: %v", lm.SnapKey, err)
		return nil
	}
	lsn := binary.LittleEndian.Uint64(b)
	n := binary.LittleEndian.Uint64(b[8:])
	body := b[16 : len(b)-4]
	if n > uint64(len(body)) || lsn > l.head() {
		return nil
	}
	kv, err1 := lens.LoadKV(body[:n], db.opts.InlineMax)
	tp, err2 := lens.LoadTopic(body[n:])
	if err1 != nil || err2 != nil {
		db.logf("freshdb: ignoring corrupt snapshot %s", lm.SnapKey)
		return nil
	}
	return &snapshot{lsn: lsn, kv: kv, topic: tp, drop: db.keys.shreddedList()}
}
