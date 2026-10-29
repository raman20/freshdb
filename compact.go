package freshdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// Compaction (docs/design/03-compaction.md, spec/FreshCompact.tla).
//
// Compact gives every log a base: the full lens state at its head K,
// written as new objects (an image plus value chunks), and commits a
// manifest that points each log at its base. Older segments, snapshots
// and bases become garbage that a sweep deletes once a grace period has
// passed. Overwritten, deleted and shredded values are not copied into
// the base, so they leave storage with the garbage. History below a
// base is gone: time travel, branching and rewinding below it return
// ErrCompacted.

const (
	baseMagic  = "FRDBBAS1"
	chunkMagic = "FRDBVAL1"
	chunkMax   = 64 << 20
)

var errPaused = errors.New("freshdb: writer paused for compaction")

// CompactOptions configures Compact.
type CompactOptions struct {
	// Grace is how long replaced objects stay before a sweep may delete
	// them, so readers holding an older manifest (another process, a
	// read-only open, a backup) can finish. 0 deletes them right away.
	Grace time.Duration
}

// CompactStats reports what Compact did.
type CompactStats struct {
	Logs      int   `json:"logs"`       // logs given a new base
	Keys      int   `json:"keys"`       // live keys written into bases
	Messages  int   `json:"messages"`   // topic messages written into bases
	BaseBytes int64 `json:"base_bytes"` // bytes written (images + value chunks)
	Deleted   int   `json:"deleted"`    // objects deleted by sweeps
	Pending   int   `json:"pending"`    // garbage items still waiting for their grace period

	// GC only
	LogsDropped      int `json:"logs_dropped,omitempty"`      // unreachable logs removed from the manifest
	ReflogPruned     int `json:"reflog_pruned,omitempty"`     // reflog entries dropped
	ManifestsDeleted int `json:"manifests_deleted,omitempty"` // old manifests deleted
}

func baseObj(key string) string         { return key + ".base" }
func chunkKey(key string, n int) string { return fmt.Sprintf("%s.%06d.val", key, n) }

// baseMsgAAD binds a re-sealed subject message in a base to its log,
// LSN, topic and offset (an op index no frame uses keeps it distinct
// from every frame AAD).
func baseMsgAAD(log, space string, m lens.Msg) []byte {
	return aad(log, m.LSN, -1, space, strconv.FormatUint(m.Offset, 10))
}

// Compact compacts every log (see above). Commits wait while it runs;
// reads continue. It is a no-op for logs unchanged since their base.
func (db *DB) Compact(ctx context.Context, o CompactOptions) (CompactStats, error) {
	return db.runCompaction(ctx, GCOptions{Grace: o.Grace}, false)
}

// runCompaction is Compact (gc false) or GC (gc true).
func (db *DB) runCompaction(ctx context.Context, o GCOptions, gc bool) (stats CompactStats, err error) {
	t0 := time.Now()
	defer func() {
		runs, errs := &db.met.compactions, &db.met.compactErrors
		if gc {
			runs, errs = &db.met.gcs, &db.met.gcErrors
		}
		runs.Add(1)
		if err != nil {
			errs.Add(1)
		}
		db.met.compactLat.Observe(time.Since(t0))
	}()
	db.mu.Lock()
	switch {
	case db.closed:
		db.mu.Unlock()
		return stats, ErrClosed
	case db.failed != nil:
		db.mu.Unlock()
		return stats, db.failed
	case db.compacting:
		db.mu.Unlock()
		return stats, ErrBusy
	}
	db.compacting, db.compactDone = true, make(chan struct{})
	ws := make([]*writer, 0, len(db.writers))
	for _, id := range sortedMapKeys(db.writers) {
		ws = append(ws, db.writers[id])
	}
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		db.compacting = false
		close(db.compactDone)
		db.mu.Unlock()
	}()
	// Stop the writers without db.mu (a draining writer may need it to
	// commit a snapshot). Branch ops wait in lockIdle meanwhile.
	for _, w := range ws {
		w.pause()
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return stats, ErrClosed
	}
	// A writer halted by a storage error (ErrUnknownOutcome) stays halted
	// until reopen: compacting would rebuild from a state that may miss
	// its last segment, and a fresh writer would hide the halt.
	var committed bool
	for _, w := range ws {
		if w.failed != nil && err == nil {
			err = fmt.Errorf("freshdb: compaction refused: %w", w.failed)
		}
	}
	if err == nil {
		committed, err = db.compactLocked(ctx, o, gc, &stats)
	}
	if gc && err == nil {
		defer func() { db.deleteOldManifests(ctx, o.KeepManifests, &stats) }()
	}
	if !committed {
		// nothing changed: resume on the same logs and views
		for _, w := range ws {
			if db.writers[w.l.meta.ID] == w {
				nw := newWriter(db, w.l, w.v)
				nw.failed = w.failed // a halt survives (read after stop)
				db.writers[w.l.meta.ID] = nw
			}
		}
		db.bw.Store(nil)
		return stats, err
	}
	// Reload every branch from its base: frees the history held in RAM.
	// The HNSW graphs are handed over (the vectors are the same), unless
	// a shred ran since they were built: then rebuilding them clears that.
	db.annCarry = map[string]lens.ANNSnapshot{}
	if !db.annTainted {
		for id, v := range db.views {
			if b := v.vec.MarshalANN(); b != nil {
				if s, err := lens.DecodeANN(b); err == nil {
					db.annCarry[id] = s
				}
			}
		}
	}
	defer func() { db.annCarry, db.annTainted = nil, false }()
	db.logs, db.views, db.writers = map[string]*logState{}, map[string]*view{}, map[string]*writer{}
	db.bw.Store(nil)
	for _, name := range sortedMapKeys(db.man.Refs) {
		if _, rerr := db.openWritable(ctx, db.man.Refs[name]); rerr != nil {
			if db.failed == nil {
				db.failed = rerr
			}
			for _, w := range ws { // callers get ErrClosed instead of retrying
				w.mu.Lock()
				w.paused = false
				w.mu.Unlock()
			}
			return stats, rerr
		}
	}
	if err == nil {
		err = db.sweepLocked(ctx, &stats)
	}
	return stats, err
}

// compactLocked builds bases and commits them. Caller holds db.mu with
// all writers stopped. committed reports whether the manifest changed.
// Compact bases every log at its head; GC follows gcPlan (retention cuts,
// reflog pruning, unreachable logs dropped).
func (db *DB) compactLocked(ctx context.Context, o GCOptions, gc bool, stats *CompactStats) (committed bool, err error) {
	if err := db.sweepLocked(ctx, stats); err != nil {
		return false, err
	}
	type built struct {
		id   string
		base *BaseMeta
		head uint64 // log head at the cut: snapshots up to it are replaced
	}
	plan := map[string]cutInfo{}
	var drop []string
	reflog := db.man.Reflog
	if gc {
		if plan, drop, reflog, err = db.gcPlan(ctx, o); err != nil {
			return false, err
		}
	} else {
		for id := range db.man.Logs {
			plan[id] = cutInfo{head: true}
		}
	}
	var bs []built
	for _, id := range sortedMapKeys(plan) {
		c := plan[id]
		l, err := db.loadLog(ctx, id, false)
		if err != nil {
			return false, err
		}
		var bm *BaseMeta
		if c.head {
			head := l.head()
			if l.meta.Base != nil && l.meta.Base.LSN == head {
				continue
			}
			v := db.views[id]
			if v == nil || v.head != head {
				if v, err = db.buildView(ctx, l, head, db.man.Indexes, db.loadSnapshot(ctx, l)); err != nil {
					return false, err
				}
			}
			bm, err = db.writeBase(ctx, l, v, head, nil, stats)
		} else {
			var v *view
			if v, err = db.buildView(ctx, l, c.k, db.man.Indexes, nil); err != nil {
				return false, err
			}
			bm, err = db.writeBase(ctx, l, v, c.k, &c, stats)
		}
		if err != nil {
			return false, err
		}
		bs = append(bs, built{id, bm, l.head()})
	}
	if len(bs) == 0 && len(drop) == 0 && len(reflog) == len(db.man.Reflog) {
		return false, nil
	}
	// An unbased branch reading through a rebased ancestor has snapshots
	// whose versions point into that ancestor's replaced segments: they go
	// too (Compact rebases every log, so this is GC only).
	rebased := map[string]bool{}
	for _, b := range bs {
		rebased[b.id] = true
	}
	stale := map[string]uint64{} // log -> head
	for id, lm := range db.man.Logs {
		if lm.Base != nil || rebased[id] || lm.SnapKey == "" {
			continue
		}
		for x := db.man.Logs[lm.Parent]; x != nil && lm.Parent != ""; x = db.man.Logs[x.Parent] {
			if rebased[x.ID] {
				l, err := db.loadLog(ctx, id, false)
				if err != nil {
					return false, err
				}
				stale[id] = l.head()
				break
			}
			if x.Base != nil || x.Parent == "" {
				break
			}
		}
	}
	pruned := len(db.man.Reflog) - len(reflog)
	after := db.opts.Clock() + int64(o.Grace)
	err = db.commitManifest(ctx, func(m *Manifest) error {
		m.Format = manifestFormatBase
		for id, head := range stale {
			m.Logs[id].SnapLSN, m.Logs[id].SnapKey = 0, ""
			m.Garbage = append(m.Garbage, Garbage{Kind: "snaps", Log: id, Below: head + 1, After: after})
		}
		for _, b := range bs {
			lm := m.Logs[b.id]
			if old := lm.Base; old != nil {
				m.Garbage = append(m.Garbage, Garbage{Kind: "base", Log: b.id, Key: old.Key, Chunks: old.Chunks, After: after})
			}
			m.Garbage = append(m.Garbage,
				Garbage{Kind: "segs", Log: b.id, Below: b.base.Seq, After: after},
				// a snapshot's versions point into segments the base
				// replaces, so every snapshot so far goes (writers are
				// stopped: none is newer than the head)
				Garbage{Kind: "snaps", Log: b.id, Below: b.head + 1, After: after})
			lm.Base = b.base
			lm.SnapLSN, lm.SnapKey = 0, ""
		}
		for _, id := range drop {
			lm := m.Logs[id]
			if lm.Base != nil {
				m.Garbage = append(m.Garbage, Garbage{Kind: "base", Log: id, Key: lm.Base.Key, Chunks: lm.Base.Chunks, After: after})
			}
			m.Garbage = append(m.Garbage,
				Garbage{Kind: "segs", Log: id, Below: math.MaxUint64, After: after},
				Garbage{Kind: "snaps", Log: id, Below: math.MaxUint64, After: after})
			delete(m.Logs, id)
		}
		m.Reflog = reflog
		return nil
	})
	if err != nil {
		// The new bases may be orphans now; a later epoch's sweep deletes
		// them (we cannot: the put of the manifest may yet be visible).
		return false, err
	}
	stats.Logs, stats.LogsDropped, stats.ReflogPruned = len(bs), len(drop), pruned
	return true, nil
}

// writeBase writes the base of log l at head from its view v.
// at is nil for a base at the log's head; for a retention cut below the
// head it gives the segment after the cut and the cut frame's time.
func (db *DB) writeBase(ctx context.Context, l *logState, v *view, head uint64, at *cutInfo, stats *CompactStats) (*BaseMeta, error) {
	nonce, err := db.keys.randBytes(4)
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("base/%s/%020d-e%d-%s", l.meta.ID, head, db.epoch, hex.EncodeToString(nonce))
	id := l.meta.ID
	kv, tp := lens.NewKVInline(db.opts.InlineMax), lens.NewTopic()
	cw := &chunkWriter{db: db, key: key}
	v.mu.RLock()
	defer v.mu.RUnlock()
	err = v.kv.Latest(func(space, k string, ver lens.Version) error {
		if ver.Del {
			return nil
		}
		val := ver.Val
		if ver.Ref() || ver.Subject != "" {
			x, seq, ok := l.source(ver)
			if !ok {
				return fmt.Errorf("%w: LSN %d of %s/%s not in the log", ErrCorrupt, ver.LSN, space, k)
			}
			if ver.Ref() {
				if val, err = db.readValue(x, seq, ver); err != nil {
					return err
				}
			}
			if ver.Subject != "" {
				p, ok := db.keys.open(ver.Subject, val, aad(x.meta.ID, ver.LSN, int(ver.Op), space, k))
				if !ok {
					return nil // shredded since the lens last saw it
				}
				ct, ok, err := db.keys.reseal(ver.Subject, p, aad(id, ver.LSN, int(ver.Op), space, k))
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
				val = ct
			}
		}
		nv := lens.Version{LSN: ver.LSN, Op: ver.Op, Subject: ver.Subject}
		if len(val) > kv.Inline() {
			n, off, err := cw.add(ctx, val)
			if err != nil {
				return err
			}
			nv.Chunk, nv.Off, nv.Len, nv.Sum = uint32(n+1), uint32(off), uint32(len(val)), crc32.Checksum(val, lens.Castagnoli)
		} else {
			nv.Val = bytes.Clone(val)
		}
		kv.SetLatest(space, k, nv)
		stats.Keys++
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, space := range v.topic.Spaces() {
		src := v.topic.All(space)
		ms := make([]lens.Msg, 0, len(src))
		for _, m := range src {
			if m.LSN > head {
				break
			}
			nm := m
			if m.Subject != "" && !m.Redacted {
				nm.Val, nm.Redacted = nil, true
				if p, err := db.openMsg(l, space, m); err == nil {
					if ct, ok, err := db.keys.reseal(m.Subject, p, baseMsgAAD(id, space, m)); err != nil {
						return nil, err
					} else if ok {
						nm.Val, nm.Redacted = ct, false
					}
				} else if !errors.Is(err, ErrShredded) {
					return nil, err
				}
			} else {
				nm.Val = bytes.Clone(m.Val)
			}
			ms = append(ms, nm)
		}
		tp.SetAll(space, ms)
		stats.Messages += len(ms)
	}
	if err := cw.flush(ctx); err != nil {
		return nil, err
	}
	chain, err := l.chainAt(ctx, head)
	if err != nil {
		return nil, err
	}
	l.mu.RLock()
	ts, seq := l.lastTS, l.nextSeq
	l.mu.RUnlock()
	if at != nil {
		ts, seq = at.ts, at.seq
	} else {
		// every segment that exists now is covered (a read-only load may
		// stop before a torn tail)
		keys, err := db.st.List(ctx, "log/"+id+"/")
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			if s, ok := parseNum(k, "log/"+id+"/", ".seg"); ok && s >= seq {
				seq = s + 1
			}
		}
	}
	img := encodeBase(head, kv.Snapshot(), tp.Snapshot())
	if err := putExactlyOnce(ctx, db.st, baseObj(key), img); err != nil {
		return nil, err
	}
	stats.BaseBytes += int64(len(img)) + cw.bytes
	return &BaseMeta{LSN: head, Seq: seq, Key: key, Chunks: cw.n, Chain: hex.EncodeToString(chain[:]), TS: ts}, nil
}

func encodeBase(lsn uint64, kv, topic []byte) []byte {
	b := append([]byte(baseMagic), make([]byte, 16)...)
	binary.LittleEndian.PutUint64(b[8:], lsn)
	binary.LittleEndian.PutUint64(b[16:], uint64(len(kv)))
	b = append(append(b, kv...), topic...)
	return binary.LittleEndian.AppendUint32(b, format.CRC(b))
}

// chunkWriter packs base values into chunk objects of about chunkMax.
type chunkWriter struct {
	db    *DB
	key   string
	buf   []byte
	n     int // chunks written
	bytes int64
}

func (c *chunkWriter) add(ctx context.Context, val []byte) (int, int, error) {
	if len(c.buf) > len(chunkMagic) && len(c.buf)+len(val) > chunkMax {
		if err := c.flush(ctx); err != nil {
			return 0, 0, err
		}
	}
	if len(c.buf) == 0 {
		c.buf = append(c.buf, chunkMagic...)
	}
	off := len(c.buf)
	c.buf = append(c.buf, val...)
	return c.n, off, nil
}

func (c *chunkWriter) flush(ctx context.Context) error {
	if len(c.buf) == 0 {
		return nil
	}
	if err := putExactlyOnce(ctx, c.db.st, chunkKey(c.key, c.n), c.buf); err != nil {
		return err
	}
	c.bytes += int64(len(c.buf))
	c.n++
	c.buf = c.buf[:0]
	return nil
}

// baseImage is a decoded base: fresh lens state (callers may modify it).
type baseImage struct {
	log   *logState
	lsn   uint64
	kv    *lens.KV
	topic *lens.Topic
}

// loadBase reads and checks the base of based log b.
func (db *DB) loadBase(ctx context.Context, b *logState) (*baseImage, error) {
	bm := b.meta.Base
	data, err := db.st.Get(ctx, baseObj(bm.Key))
	if err != nil {
		return nil, fmt.Errorf("freshdb: log %s base %s: %w", b.meta.ID, bm.Key, err)
	}
	bad := func(why string) error {
		return fmt.Errorf("%w: log %s base %s: %s", ErrCorrupt, b.meta.ID, bm.Key, why)
	}
	if len(data) >= 8 && string(data[:7]) == baseMagic[:7] && data[7] != baseMagic[7] {
		return nil, fmt.Errorf("%w: log %s base %s has version %q (this build reads %q; upgrade the binary)",
			ErrVersion, b.meta.ID, bm.Key, data[7], baseMagic[7])
	}
	if len(data) < 28 || string(data[:8]) != baseMagic {
		return nil, bad("bad header")
	}
	if binary.LittleEndian.Uint32(data[len(data)-4:]) != format.CRC(data[:len(data)-4]) {
		return nil, bad("checksum mismatch")
	}
	if binary.LittleEndian.Uint64(data[8:]) != bm.LSN {
		return nil, bad("LSN mismatch")
	}
	n := binary.LittleEndian.Uint64(data[16:])
	body := data[24 : len(data)-4]
	if n > uint64(len(body)) {
		return nil, bad("bad length")
	}
	kv, err := lens.LoadKVBase(body[:n], db.opts.InlineMax)
	if err != nil {
		return nil, bad(err.Error())
	}
	tp, err := lens.LoadTopic(body[n:])
	if err != nil {
		return nil, bad(err.Error())
	}
	return &baseImage{log: b, lsn: bm.LSN, kv: kv, topic: tp}, nil
}

// eachBase feeds the vector and index lenses the base's vec:/doc: values
// as synthetic entries: grouped by LSN, each op at its original index
// (other slots empty) so env.Open finds the AAD it was sealed with.
func (db *DB) eachBase(ctx context.Context, img *baseImage, fn func(*lens.Entry) error) error {
	type item struct {
		space, key string
		ver        lens.Version
	}
	var items []item
	_ = img.kv.Latest(func(space, key string, ver lens.Version) error {
		if !ver.Del && (strings.HasPrefix(space, "vec:") || strings.HasPrefix(space, "doc:")) {
			items = append(items, item{space, key, ver})
		}
		return nil
	})
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i].ver, items[j].ver
		return a.LSN < b.LSN || a.LSN == b.LSN && a.Op < b.Op
	})
	for i := 0; i < len(items); {
		j := i
		for j < len(items) && items[j].ver.LSN == items[i].ver.LSN {
			j++
		}
		e := &lens.Entry{Log: img.log.meta.ID, LSN: items[i].ver.LSN, Ops: make([]format.Op, items[j-1].ver.Op+1)}
		for _, it := range items[i:j] {
			val := it.ver.Val
			if it.ver.Ref() {
				var err error
				if val, err = db.readValue(img.log, 0, it.ver); err != nil {
					return err
				}
			}
			e.Ops[it.ver.Op] = format.Op{Kind: format.OpPut, Subject: it.ver.Subject, Space: it.space, Key: it.key, Value: val}
		}
		if err := fn(e); err != nil {
			return err
		}
		i = j
	}
	return nil
}

// fold calls fn with every entry of l's lineage up to upto, the base's
// synthetic entries first (for lenses other than KV and topic).
func (db *DB) fold(ctx context.Context, l *logState, upto uint64, fn func(*lens.Entry) error) error {
	if b := l.based(); b != nil {
		img, err := db.loadBase(ctx, b)
		if err != nil {
			return err
		}
		if err := db.eachBase(ctx, img, fn); err != nil {
			return err
		}
	}
	return l.each(ctx, upto, fn)
}

// sweepLocked deletes due garbage and unreferenced bases of older epochs,
// then commits the shorter garbage list. Caller holds db.mu.
func (db *DB) sweepLocked(ctx context.Context, stats *CompactStats) error {
	now := db.opts.Clock()
	var keep []Garbage
	for _, g := range db.man.Garbage {
		if g.After > now {
			keep = append(keep, g)
			continue
		}
		n, err := db.deleteGarbage(ctx, g)
		stats.Deleted += n
		if err != nil {
			db.logf("freshdb: sweep %s %s: %v", g.Kind, g.Log, err)
			keep = append(keep, g)
		}
	}
	stats.Pending = len(keep)
	n, err := db.sweepOrphans(ctx)
	stats.Deleted += n
	if err != nil {
		db.logf("freshdb: orphan sweep: %v", err)
	}
	if len(keep) == len(db.man.Garbage) {
		return nil
	}
	return db.commitManifest(ctx, func(m *Manifest) error {
		m.Garbage = keep
		return nil
	})
}

func (db *DB) deleteGarbage(ctx context.Context, g Garbage) (int, error) {
	var keys []string
	switch g.Kind {
	case "segs", "snaps":
		dir, suf := "log/"+g.Log+"/", ".seg"
		if g.Kind == "snaps" {
			dir, suf = "snap/"+g.Log+"/", ".snap"
		}
		all, err := db.st.List(ctx, dir)
		if err != nil {
			return 0, err
		}
		for _, k := range all {
			if n, ok := parseNum(k, dir, suf); ok && n < g.Below {
				keys = append(keys, k)
			}
		}
		if g.Kind == "segs" && g.Below == math.MaxUint64 { // dropped log: its saved graphs too
			ann, err := db.st.List(ctx, annDir(g.Log))
			if err != nil {
				return 0, err
			}
			keys = append(keys, ann...)
		}
	case "base":
		keys = append(keys, baseObj(g.Key))
		for i := 0; i < g.Chunks; i++ {
			keys = append(keys, chunkKey(g.Key, i))
		}
	default:
		return 0, fmt.Errorf("unknown garbage kind %q", g.Kind)
	}
	n := 0
	for _, k := range keys {
		if err := db.st.Delete(ctx, k); err != nil && !errors.Is(err, store.ErrNotFound) {
			return n, err
		}
		n++
	}
	return n, nil
}

// sweepOrphans deletes base objects that no manifest from this one on
// can refer to: written in an older epoch and not in the manifest (a
// compaction whose commit lost to a newer opener).
func (db *DB) sweepOrphans(ctx context.Context) (int, error) {
	live := map[string]bool{}
	for _, lm := range db.man.Logs {
		if lm.Base != nil {
			live[lm.Base.Key] = true
		}
	}
	for _, g := range db.man.Garbage {
		if g.Kind == "base" {
			live[g.Key] = true
		}
	}
	n := 0
	ids := map[string]bool{}
	for id := range db.man.Logs {
		ids[id] = true
	}
	for _, g := range db.man.Garbage {
		ids[g.Log] = true // dropped by GC: its orphans too
	}
	for _, id := range sortedMapKeys(ids) {
		keys, err := db.st.List(ctx, "base/"+id+"/")
		if err != nil {
			return n, err
		}
		for _, k := range keys {
			bk, ok := baseKeyOf(k)
			if !ok || live[bk] {
				continue
			}
			if ep, ok := baseEpoch(bk); !ok || ep >= db.epoch {
				continue
			}
			if err := db.st.Delete(ctx, k); err != nil && !errors.Is(err, store.ErrNotFound) {
				return n, err
			}
			n++
		}
	}
	return n, nil
}

// baseKeyOf maps an object under base/ to its base key.
func baseKeyOf(obj string) (string, bool) {
	if k, ok := strings.CutSuffix(obj, ".base"); ok {
		return k, true
	}
	if k, ok := strings.CutSuffix(obj, ".val"); ok && len(k) > 7 && k[len(k)-7] == '.' {
		return k[:len(k)-7], true
	}
	return "", false
}

// baseEpoch parses the epoch from base/<log>/<lsn>-e<epoch>-<nonce>.
func baseEpoch(key string) (uint64, bool) {
	// fields of the last path element: <lsn>, e<epoch>, <nonce>; the hex
	// nonce may itself start with "e", so split rather than search
	f := strings.Split(key[strings.LastIndexByte(key, '/')+1:], "-")
	if len(f) != 3 || !strings.HasPrefix(f[1], "e") {
		return 0, false
	}
	ep, err := strconv.ParseUint(f[1][1:], 10, 64)
	return ep, err == nil
}
