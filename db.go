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
	// KEK is the 32-byte key-encryption key. Nil = dev mode: generated and
	// stored in the bucket at keys/_kek (NOT safe against bucket backups).
	KEK []byte
	// SnapshotEvery writes a KV/topic checkpoint every N commits per log
	// (<= 0 = never, the default). Snapshots are written synchronously in the
	// commit path, copy the whole lens state, and do not yet make open faster
	// (recovery still verifies every segment), so they are opt-in for now.
	SnapshotEvery int
	// MaxBatch bounds how many transactions one group commit may contain.
	MaxBatch int
	// Logf receives operational messages (default: log.Printf; nil-safe).
	Logf func(format string, args ...any)
	// InlineMax: KV values up to this many bytes are kept in memory; larger
	// ones are kept as (segment, offset, length) positions and read from
	// the log on demand. 0 = default (64); negative = keep none inline.
	InlineMax int
	// ValueCacheBytes bounds the cache of values read from segments.
	// 0 = default (64 MiB); negative = no cache.
	ValueCacheBytes int
}

type Option func(*Options)

func WithClock(f func() int64) Option        { return func(o *Options) { o.Clock = f } }
func WithRand(r io.Reader) Option            { return func(o *Options) { o.Rand = r } }
func WithKEK(k []byte) Option                { return func(o *Options) { o.KEK = k } }
func WithSnapshotEvery(n int) Option         { return func(o *Options) { o.SnapshotEvery = n } }
func WithLogf(f func(string, ...any)) Option { return func(o *Options) { o.Logf = f } }
func WithInlineMax(n int) Option             { return func(o *Options) { o.InlineMax = n } }
func WithValueCacheBytes(n int) Option       { return func(o *Options) { o.ValueCacheBytes = n } }

const (
	defaultInlineMax  = 64
	defaultValueCache = 64 << 20
)

// DB is an open database. One process holds the write lease (epoch); a newer
// Open anywhere fences this one, after which writes fail with ErrFenced.
type DB struct {
	st    store.Store
	opts  Options
	epoch uint64
	keys  *keyring
	env   env

	mu      sync.Mutex // guards manifest, logs, views, writers
	man     *Manifest
	logs    map[string]*logState
	views   map[string]*view   // live views of writable logs
	writers map[string]*writer // by log id
	vcache  *valueCache
	failed  error
	closed  bool
}

// Open opens (or creates) the database at url: a directory path,
// file:///path, mem://name or s3://bucket/prefix?endpoint=...
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
	return OpenStore(ctx, st, opts...)
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
		o.ValueCacheBytes = defaultValueCache
	}
	var err error
	db := &DB{st: st, opts: o, vcache: newValueCache(o.ValueCacheBytes), logs: map[string]*logState{}, views: map[string]*view{}, writers: map[string]*writer{}}
	if err := db.acquire(ctx); err != nil {
		return nil, err
	}
	if db.keys, err = openKeyring(ctx, st, o.Rand, o.KEK); err != nil {
		return nil, err
	}
	db.env = env{db.keys}
	for _, name := range sortedMapKeys(db.man.Refs) {
		if _, err := db.openWritable(ctx, db.man.Refs[name]); err != nil {
			return nil, err
		}
	}
	return db, nil
}

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
		return nil
	}
	return errors.New("freshdb: could not acquire write lease (manifest contention)")
}

// commitManifest applies change to a copy of the manifest and creates the
// next manifest. ErrExists => a newer writer exists => we are fenced.
// Caller holds db.mu.
func (db *DB) commitManifest(ctx context.Context, change func(m *Manifest) error) error {
	if db.failed != nil {
		return db.failed
	}
	next := db.man.clone()
	if err := change(next); err != nil {
		return err
	}
	next.ID = db.man.ID + 1
	err := putManifest(ctx, db.st, next)
	if errors.Is(err, store.ErrExists) {
		db.failed = ErrFenced
		return ErrFenced
	}
	if err != nil {
		db.failed = err
		return err
	}
	db.man = next
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
	if meta.Parent != "" {
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
	db.logs[id] = l
	return l, nil
}

// openWritable recovers, fences and starts the writer for a log.
// Caller must not hold db.mu (Open) or must (branch ops) — see callers.
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
	v, err := buildView(ctx, l, l.head(), db.env, db.man.Indexes, snap, db.opts.InlineMax)
	if err != nil {
		return nil, err
	}
	db.views[id] = v
	w := newWriter(db, l, v)
	db.writers[id] = w
	return w, nil
}

// branchWriter returns the writer of a named branch.
func (db *DB) branchWriter(name string) (*writer, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
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
	ws := db.writers
	db.mu.Unlock()
	for _, id := range sortedMapKeys(ws) {
		ws[id].stop()
	}
	return nil
}

// Crash abandons the DB without any further storage writes (for crash tests
// and simulations). Pending commits fail with ErrClosed.
func (db *DB) Crash() {
	db.mu.Lock()
	db.closed = true
	db.failed = ErrClosed
	ws := db.writers
	db.mu.Unlock()
	for _, id := range sortedMapKeys(ws) {
		ws[id].crash()
	}
}

// Epoch returns this writer's fencing epoch.
func (db *DB) Epoch() uint64 { return db.epoch }

// Manifest returns a copy of the current manifest.
func (db *DB) Manifest() *Manifest {
	db.mu.Lock()
	defer db.mu.Unlock()
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
