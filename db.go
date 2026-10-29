package freshdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// Options configure a DB. Every field has a working default.
type Options struct {
	// Clock returns the commit timestamp (unix nanos). Recorded in the log,
	// so lenses never read a clock themselves. Fake it in simulations.
	Clock func() int64
	// Rand is the entropy source for keys and nonces (crypto/rand default).
	Rand io.Reader
	// Holder names this process in its lease record (WithHolder).
	Holder string
	// KEK is the 32-byte key-encryption key. Nil = dev mode: generated and
	// stored in the bucket at keys/_kek (NOT safe against bucket backups).
	KEK []byte
	// Keys, if set, supplies the KEK at open (overrides KEK).
	Keys KeyProvider
	// SnapshotEvery writes a KV/topic checkpoint every N commits per log
	// (<= 0 = never, the default). Snapshots are written synchronously in the
	// commit path, copy the whole lens state, and do not yet make open faster
	// (recovery still verifies every segment), so they are opt-in for now.
	SnapshotEvery int
	// MaxBatch bounds how many transactions one group commit may contain.
	MaxBatch int
	// Commit sets how long a group commit may wait to grow (CommitPolicy).
	// The zero value never waits: a batch is whatever queued up while the
	// previous segment was being written.
	Commit CommitPolicy
	// Logf receives operational messages (default: log.Printf; nil-safe).
	Logf func(format string, args ...any)
	// InlineMax: KV values up to this many bytes are kept in memory; larger
	// ones are kept as (segment, offset, length) positions and read from
	// the log on demand. 0 = default (64); negative = keep none inline.
	InlineMax int
	// ValueCacheBytes bounds the cache of values read from segments. The
	// cache lives off the Go heap (anonymous mmap where available), so RSS
	// grows by about this much once it fills, not by a GC multiple.
	// 0 = default: 1/16 of the memory limit (cgroup v2, else MemTotal),
	// between 16 and 256 MiB; negative = no cache.
	ValueCacheBytes int
	// RepairTail lets open repair a damaged final segment by keeping its
	// valid prefix. The damaged object stays in place; a create-only
	// repair note (log/<id>/<seq>.repair) records which prefix is kept.
	// Every supported store writes objects atomically, so a damaged tail is
	// corruption, not a torn write, and may hold acknowledged commits: by
	// default open refuses with ErrCorrupt and changes nothing. Set it (CLI:
	// --repair-tail) only to salvage a database you cannot restore.
	RepairTail bool
}

type Option func(*Options)

func WithClock(f func() int64) Option { return func(o *Options) { o.Clock = f } }
func WithRand(r io.Reader) Option     { return func(o *Options) { o.Rand = r } }
func WithKEK(k []byte) Option         { return func(o *Options) { o.KEK = k } }

// WithCommitPolicy sets the group-commit budget (see CommitPolicy).
func WithCommitPolicy(p CommitPolicy) Option { return func(o *Options) { o.Commit = p } }

func WithSnapshotEvery(n int) Option         { return func(o *Options) { o.SnapshotEvery = n } }
func WithLogf(f func(string, ...any)) Option { return func(o *Options) { o.Logf = f } }
func WithInlineMax(n int) Option             { return func(o *Options) { o.InlineMax = n } }
func WithValueCacheBytes(n int) Option       { return func(o *Options) { o.ValueCacheBytes = n } }
func WithRepairTail(on bool) Option          { return func(o *Options) { o.RepairTail = on } }

const (
	defaultInlineMax  = 64
	defaultValueCache = 256 << 20
)

// DB is an open database. One process holds the write lease (epoch); a newer
// Open anywhere fences this one, after which writes fail with ErrFenced.
type DB struct {
	st    store.Store
	opts  Options
	epoch uint64

	annTainted bool                         // a shred ran: do not save graphs built before it
	annCarry   map[string]lens.ANNSnapshot  // graphs handed over a compaction reload
	annStats   map[string]lens.ANNLoadStats // last restore per log (tests)
	keys       *keyring
	// devKEK: the KEK lives in the bucket (no key supplied).
	devKEK bool
	// chg is closed when a branch head may have moved (notify.go).
	chgMu sync.Mutex
	chg   chan struct{}
	env   env

	mu      sync.RWMutex // guards manifest, logs, views, writers
	man     *Manifest
	logs    map[string]*logState
	views   map[string]*view   // live views of writable logs
	writers map[string]*writer // by log id
	vcache  *valueCache
	met     metrics
	// bw caches branch name -> writer for the read path; nil = rebuild.
	// Reset (under db.mu held exclusively) whenever refs, writers or
	// closed change, so readers never need db.mu on a hit.
	bw     atomic.Pointer[map[string]*writer]
	failed error
	closed bool
	owned  io.Closer // store opened by Open from a URL; closed by Close
	// compacting is set while Compact runs; branch ops wait on
	// compactDone (lockIdle) and paused writers' callers retry after it.
	compacting  bool
	compactDone chan struct{}
}

// lockIdle takes db.mu once no compaction is running.
func (db *DB) lockIdle() {
	for {
		db.mu.Lock()
		if !db.compacting {
			return
		}
		ch := db.compactDone
		db.mu.Unlock()
		<-ch
	}
}

// waitCompaction blocks until a running compaction finishes.
func (db *DB) waitCompaction(ctx context.Context) error {
	db.mu.RLock()
	ch := db.compactDone
	busy := db.compacting
	db.mu.RUnlock()
	if !busy {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Open opens (or creates) the database at url: a directory path or
// file:///path (its existing layout, else the journal layout on Unix; see
// store.OpenLocal), journal:///path, fs:///path, mem://name or
// s3://bucket/prefix?endpoint=...
func Open(ctx context.Context, url string, opts ...Option) (*DB, error) {
	st, err := store.Open(url)
	if err != nil {
		return nil, err
	}
	if _, isS3 := st.(*store.S3); isS3 {
		if err := store.CheckConditional(ctx, st); err != nil {
			return nil, err
		}
	}
	db, err := OpenStore(ctx, st, opts...)
	if c, ok := st.(io.Closer); ok {
		if err != nil {
			c.Close()
			return nil, err
		}
		db.owned = c
	}
	return db, err
}

// OpenStore opens a database on an explicit Store.
func OpenStore(ctx context.Context, st store.Store, opts ...Option) (*DB, error) {
	o := Options{}
	for _, f := range opts {
		f(&o)
	}
	if o.Clock == nil {
		o.Clock = func() int64 { return time.Now().UnixNano() }
	}
	if o.Rand == nil {
		o.Rand = rand.Reader
	}
	if o.MaxBatch <= 0 {
		o.MaxBatch = 256
	}
	if o.Commit.MaxDelay > 0 && o.Commit.TargetBytes <= 0 {
		o.Commit.TargetBytes = defaultTargetBytes
	}
	if o.Logf == nil {
		o.Logf = log.Printf
	}
	switch {
	case o.InlineMax == 0:
		o.InlineMax = defaultInlineMax
	case o.InlineMax < 0:
		o.InlineMax = 0
	}
	if o.ValueCacheBytes == 0 {
		o.ValueCacheBytes = defaultValueCacheBytes()
	}
	db := &DB{st: &holdStore{Store: st}, opts: o, vcache: newValueCache(o.ValueCacheBytes), logs: map[string]*logState{}, views: map[string]*view{}, writers: map[string]*writer{}, annStats: map[string]lens.ANNLoadStats{}}
	if err := db.start(ctx, st); err != nil {
		// Writers of branches opened before the failure are running, and
		// the value cache holds memory: stop both without further writes.
		db.Crash()
		return nil, err
	}
	return db, nil
}

// start takes the write lease, opens the keyring and starts a writer per
// branch (OpenStore).
func (db *DB) start(ctx context.Context, st store.Store) error {
	o := db.opts
	if err := db.acquire(ctx); err != nil {
		return err
	}
	kek := o.KEK
	if o.Keys != nil {
		var err error
		if kek, err = o.Keys.KEK(ctx); err != nil {
			return err
		}
		if len(kek) != 32 {
			return fmt.Errorf("freshdb: key provider returned %d bytes, want 32", len(kek))
		}
	}
	db.devKEK = kek == nil
	if db.devKEK {
		devKEKWarn.Do(func() {
			db.logf("freshdb: WARNING: dev-mode key: the key-encryption key is stored in the bucket at keys/_kek; anyone with a copy of the bucket can read unshredded data. Supply a KEK (WithKeyProvider, FRESHDB_KEK, --kek-file) in production.")
		})
	}
	var err error
	if db.keys, err = openKeyring(ctx, st, o.Rand, kek); err != nil {
		return err
	}
	db.env = env{db.keys}
	for _, name := range sortedMapKeys(db.man.Refs) {
		if _, err := db.openWritable(ctx, db.man.Refs[name]); err != nil {
			return err
		}
	}
	return nil
}

// devKEKWarn: Open warns once per process (through Logf) in dev-key mode.
var devKEKWarn sync.Once

// DevKEK reports whether this database runs on the dev-mode key stored in
// the bucket (no KEK or key provider was supplied).
func (db *DB) DevKEK() bool { return db.devKEK }

func (db *DB) logf(f string, a ...any) {
	if db.opts.Logf != nil {
		db.opts.Logf(f, a...)
	}
}

// acquire takes the write lease: create manifest id+1 with epoch+1.
func (db *DB) acquire(ctx context.Context) error {
	for i := 0; i < 64; i++ {
		m, err := loadLatestManifest(ctx, db.st)
		if err != nil {
			return err
		}
		var next *Manifest
		if m == nil {
			idb := make([]byte, 16)
			if _, err := io.ReadFull(db.opts.Rand, idb); err != nil {
				return err
			}
			next = &Manifest{Format: manifestFormat, ID: 1, Epoch: 1, DBID: hex.EncodeToString(idb), NextLog: 2,
				Logs: map[string]*LogMeta{"L1": {ID: "L1"}}, Refs: map[string]string{"main": "L1"}}
		} else {
			next = m.clone()
			next.ID, next.Epoch = m.ID+1, m.Epoch+1
		}
		err = putManifest(ctx, db.st, next)
		if errors.Is(err, store.ErrExists) {
			continue // someone else advanced; re-read and try again
		}
		if err != nil {
			return err
		}
		db.man, db.epoch = next, next.Epoch
		db.bw.Store(nil)
		db.writeLease(ctx, next.Epoch-1)
		return nil
	}
	return errors.New("freshdb: could not acquire write lease (manifest contention)")
}

// commitManifest applies change to a copy of the manifest and creates the
// next manifest. ErrExists => a newer writer exists => we are fenced; the
// error also wraps ErrUnknownOutcome when the change may still be committed
// (putManifest). Any failure halts the DB until reopen. Caller holds db.mu.
func (db *DB) commitManifest(ctx context.Context, change func(m *Manifest) error) error {
	if db.failed != nil {
		return db.failed
	}
	// Waiters re-read heads under db.mu, which the caller holds until the
	// views match the new manifest, so waking them here is safe.
	defer db.bump()
	next := db.man.clone()
	if err := change(next); err != nil {
		return err
	}
	next.ID = db.man.ID + 1
	err := putManifest(ctx, db.st, next)
	if errors.Is(err, store.ErrExists) {
		db.failed = db.fenced()
		if errors.Is(err, ErrUnknownOutcome) {
			return fmt.Errorf("%w (change may be committed: %w)", db.failed, err)
		}
		return db.failed
	}
	if err != nil {
		db.failed = err
		return err
	}
	db.man = next
	db.bw.Store(nil)
	return nil
}

// loadLog returns the (cached) logState of id, recovering it from storage.
// Caller holds db.mu.
func (db *DB) loadLog(ctx context.Context, id string, writable bool) (*logState, error) {
	if l, ok := db.logs[id]; ok {
		return l, nil
	}
	meta, ok := db.man.Logs[id]
	if !ok {
		return nil, fmt.Errorf("%w: unknown log %s", ErrCorrupt, id)
	}
	l := &logState{meta: *meta, st: db.st}
	if b := meta.Base; b != nil {
		// A compacted log starts from its base: no parent, no older segments.
		c, err := hex.DecodeString(b.Chain)
		if err != nil || len(c) != 32 || b.LSN < meta.ParentLSN {
			return nil, fmt.Errorf("%w: log %s: bad base", ErrCorrupt, id)
		}
		copy(l.base[:], c)
		l.n, l.lastTS, l.nextSeq = b.LSN-meta.ParentLSN, b.TS, b.Seq
	} else if meta.Parent != "" {
		p, err := db.loadLog(ctx, meta.Parent, false)
		if err != nil {
			return nil, err
		}
		if p.head() < meta.ParentLSN {
			return nil, fmt.Errorf("%w: log %s parent %s shorter than branch point %d", ErrCorrupt, id, meta.Parent, meta.ParentLSN)
		}
		l.parent = p
		if l.base, err = p.chainAt(ctx, meta.ParentLSN); err != nil {
			return nil, err
		}
	}
	if err := db.recoverLog(ctx, l, writable); err != nil {
		return nil, err
	}
	if meta.SnapLSN > l.head() {
		// The snapshot was written after a commit at SnapLSN was durable:
		// the log's newest segments are gone.
		return nil, fmt.Errorf("%w: log %s ends at LSN %d but its snapshot is at LSN %d: newest segments are missing", ErrCorrupt, id, l.head(), meta.SnapLSN)
	}
	db.logs[id] = l
	return l, nil
}

// openWritable recovers, fences and starts the writer for a log. Caller
// holds db.mu, except OpenStore: the DB is not shared yet, so no writer it
// started has requests and nothing else touches the maps.
func (db *DB) openWritable(ctx context.Context, id string) (*writer, error) {
	if w, ok := db.writers[id]; ok {
		return w, nil
	}
	l, err := db.loadLog(ctx, id, true)
	if err != nil {
		return nil, err
	}
	if err := db.fence(ctx, l); err != nil {
		return nil, err
	}
	snap := db.loadSnapshot(ctx, l)
	v, err := db.buildView(ctx, l, l.head(), db.man.Indexes, snap)
	if err != nil {
		return nil, err
	}
	// HNSW graphs only on live views: time-travel views scan exactly
	db.enableANN(ctx, id, v, l.head())
	db.views[id] = v
	w := newWriter(db, l, v)
	db.writers[id] = w
	db.bw.Store(nil)
	return w, nil
}

// branchWriter returns the writer of a named branch.
func (db *DB) branchWriter(name string) (*writer, error) {
	if m := db.bw.Load(); m != nil {
		if w, ok := (*m)[name]; ok {
			return w, nil
		}
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if db.bw.Load() == nil {
		m := make(map[string]*writer, len(db.man.Refs))
		for ref, id := range db.man.Refs {
			if w, ok := db.writers[id]; ok {
				m[ref] = w
			}
		}
		db.bw.Store(&m) // still under db.mu: no reset can interleave
	}
	id, ok := db.man.Refs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoBranch, name)
	}
	w, ok := db.writers[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q not open", ErrNoBranch, name)
	}
	return w, nil
}

// Close stops all writers after draining queued commits.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.bw.Store(nil)
	ws := db.writers
	save := !db.annTainted && db.failed == nil && len(db.man.ANN) > 0
	db.mu.Unlock()
	db.bump()
	for _, id := range sortedMapKeys(ws) {
		w := ws[id]
		w.stop()
		w.mu.RLock()
		ok := save && w.failed == nil && !w.crashed.Load()
		w.mu.RUnlock()
		if ok {
			db.saveANN(context.Background(), w)
		}
	}
	db.vcache.close()
	if db.owned != nil {
		return db.owned.Close()
	}
	return nil
}

// Crash abandons the DB without any further storage writes (for crash tests
// and simulations). Pending commits fail with ErrClosed.
func (db *DB) Crash() {
	db.mu.Lock()
	db.closed = true
	db.bw.Store(nil)
	db.failed = ErrClosed
	ws := db.writers
	db.mu.Unlock()
	db.bump()
	for _, id := range sortedMapKeys(ws) {
		ws[id].crash()
	}
	db.vcache.close()
}

// Epoch returns this writer's fencing epoch.
func (db *DB) Epoch() uint64 { return db.epoch }

// Manifest returns a copy of the current manifest.
func (db *DB) Manifest() *Manifest {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.man.clone()
}

// Branches returns branch name -> head LSN.
func (db *DB) Branches() map[string]uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := map[string]uint64{}
	for n, id := range db.man.Refs {
		if v, ok := db.views[id]; ok {
			v.mu.RLock()
			out[n] = v.head
			v.mu.RUnlock()
		}
	}
	return out
}

// Entries returns the committed log entries visible on a branch (oldest
// first). Values of subject data are ciphertext.
func (db *DB) Entries(branch string) ([]*lens.Entry, error) {
	w, err := db.branchWriter(branch)
	if err != nil {
		return nil, err
	}
	w.v.mu.RLock()
	h := w.v.head
	w.v.mu.RUnlock()
	return w.l.lineage(context.Background(), h)
}
