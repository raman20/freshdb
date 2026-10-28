package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/maphash"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	badger "github.com/dgraph-io/badger/v4"
	bolt "go.etcd.io/bbolt"
	_ "modernc.org/sqlite"

	"github.com/raman20/freshdb"
	"github.com/raman20/freshdb/store"
)

// KV is the common surface every engine is benchmarked through.
type KV interface {
	Get(k string) ([]byte, error)
	Put(k string, v []byte) error
	// RMW reads k and writes f(old) atomically (isolated from other RMWs).
	RMW(k string, f func([]byte) []byte) error
	// Load bulk-inserts keys [0, n) in batches, the fastest way each engine offers.
	Load(n int, key func(int) string, val func(int) []byte) error
	Close() error
	// Durability is a one-line description of what "fsync on/off" means here.
	Durability() string
}

var kvSystems = []string{"freshdb", "freshdb-files", "freshdb-journal", "badger", "pebble", "bbolt", "sqlite"}

func openKV(system, dir string, fsync bool) (KV, error) {
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	switch system {
	case "freshdb", "freshdb-files", "freshdb-journal":
		return openFresh(dir, fsync, freshLayout(system))
	case "badger":
		return openBadger(dir, fsync)
	case "pebble":
		return openPebble(dir, fsync)
	case "bbolt":
		return openBolt(dir, fsync)
	case "sqlite":
		return openSQLite(dir, fsync)
	}
	return nil, fmt.Errorf("unknown system %q (have %v)", system, kvSystems)
}

// ---- freshDB ----

type freshKV struct {
	db      *freshdb.DB
	fsync   bool
	journal bool
}

// freshLayout maps a system name to a local layout: "freshdb" is the
// default (store.OpenLocal: journal on Unix for a new directory),
// "freshdb-files" the v0.1.0 one-file-per-segment layout, "freshdb-journal"
// the journal explicitly.
func freshLayout(system string) store.Layout {
	switch system {
	case "freshdb-files":
		return store.LayoutFiles
	case "freshdb-journal":
		return store.LayoutJournal
	}
	return store.LayoutNone
}

// openFreshDB opens freshDB on dir in the given layout (LayoutNone = the
// default for a new local directory).
func openFreshDB(dir string, fsync bool, layout store.Layout) (*freshdb.DB, error) {
	var s store.Store
	var err error
	switch layout {
	case store.LayoutJournal:
		s, err = store.NewJournal(dir)
	case store.LayoutFiles:
		s, err = store.NewFS(dir)
	default:
		s, err = store.OpenLocal(dir)
	}
	if err != nil {
		return nil, err
	}
	st := s.(interface {
		store.Store
		SetUnsafeNoSync(bool)
	})
	st.SetUnsafeNoSync(!fsync)
	return freshdb.OpenStore(context.Background(), st, freshdb.WithLogf(func(string, ...any) {}))
}

func openFresh(dir string, fsync bool, layout store.Layout) (KV, error) {
	db, err := openFreshDB(dir, fsync, layout)
	if err != nil {
		return nil, err
	}
	l, _ := store.DetectLayout(dir)
	return &freshKV{db, fsync, l == store.LayoutJournal}, nil
}

var bg = context.Background()

func (f *freshKV) Get(k string) ([]byte, error) { return f.db.Get(bg, k) }
func (f *freshKV) Put(k string, v []byte) error { return f.db.Put(bg, k, v) }
func (f *freshKV) Close() error                 { return f.db.Close() }
func (f *freshKV) RMW(k string, fn func([]byte) []byte) error {
	return f.db.Update(bg, func(tx *freshdb.Tx) error {
		old, err := tx.Get(k)
		if err != nil && !errors.Is(err, freshdb.ErrNotFound) {
			return err
		}
		return tx.Put(k, fn(old))
	})
}
func (f *freshKV) Load(n int, key func(int) string, val func(int) []byte) error {
	const batch = 1000
	for i := 0; i < n; i += batch {
		if err := f.db.Update(bg, func(tx *freshdb.Tx) error {
			for j := i; j < i+batch && j < n; j++ {
				if err := tx.Put(key(j), val(j)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
func (f *freshKV) Durability() string {
	if f.fsync && f.journal {
		return "group commit; journal store: ack after the segment record is appended to one preallocated file + fdatasync"
	}
	if f.fsync {
		return "group commit; ack after segment file fsync + rename + directory fsync"
	}
	return "SetUnsafeNoSync: no fsync at all (page cache only)"
}

// ---- Badger ----

type badgerKV struct {
	db    *badger.DB
	fsync bool
}

func openBadger(dir string, fsync bool) (KV, error) {
	db, err := badger.Open(badger.DefaultOptions(dir).WithSyncWrites(fsync).WithLogger(nil))
	if err != nil {
		return nil, err
	}
	return &badgerKV{db, fsync}, nil
}

func (b *badgerKV) Get(k string) (v []byte, err error) {
	err = b.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get([]byte(k))
		if err != nil {
			return err
		}
		v, err = it.ValueCopy(nil)
		return err
	})
	return
}
func (b *badgerKV) Put(k string, v []byte) error {
	return b.db.Update(func(txn *badger.Txn) error { return txn.Set([]byte(k), v) })
}
func (b *badgerKV) RMW(k string, fn func([]byte) []byte) error {
	for i := 0; ; i++ {
		err := b.db.Update(func(txn *badger.Txn) error {
			var old []byte
			it, err := txn.Get([]byte(k))
			if err == nil {
				old, err = it.ValueCopy(nil)
			}
			if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			return txn.Set([]byte(k), fn(old))
		})
		if errors.Is(err, badger.ErrConflict) && i < 16 {
			continue
		}
		return err
	}
}
func (b *badgerKV) Load(n int, key func(int) string, val func(int) []byte) error {
	wb := b.db.NewWriteBatch()
	for i := 0; i < n; i++ {
		if err := wb.Set([]byte(key(i)), append([]byte(nil), val(i)...)); err != nil {
			return err
		}
	}
	return wb.Flush()
}
func (b *badgerKV) Close() error { return b.db.Close() }
func (b *badgerKV) Durability() string {
	if b.fsync {
		return "SyncWrites=true (value log + WAL synced per commit, commits batched internally)"
	}
	return "SyncWrites=false"
}

// ---- Pebble ----

type pebbleKV struct {
	db    *pebble.DB
	wo    *pebble.WriteOptions
	locks [256]sync.Mutex
	seed  maphash.Seed
}

func openPebble(dir string, fsync bool) (KV, error) {
	db, err := pebble.Open(dir, &pebble.Options{Logger: quietLogger{}})
	if err != nil {
		return nil, err
	}
	wo := pebble.NoSync
	if fsync {
		wo = pebble.Sync
	}
	return &pebbleKV{db: db, wo: wo, seed: maphash.MakeSeed()}, nil
}

type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(f string, a ...any) {
	panic(fmt.Sprintf(f, a...))
}

func (p *pebbleKV) Get(k string) ([]byte, error) {
	v, c, err := p.db.Get([]byte(k))
	if err != nil {
		return nil, err
	}
	out := append([]byte(nil), v...)
	c.Close()
	return out, nil
}
func (p *pebbleKV) Put(k string, v []byte) error { return p.db.Set([]byte(k), v, p.wo) }

// Pebble has no transactions; RMW holds a striped per-key lock (the usual
// application-level pattern), so it is atomic among RMWs only.
func (p *pebbleKV) RMW(k string, fn func([]byte) []byte) error {
	l := &p.locks[maphash.String(p.seed, k)%256]
	l.Lock()
	defer l.Unlock()
	old, err := p.Get(k)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	return p.Put(k, fn(old))
}
func (p *pebbleKV) Load(n int, key func(int) string, val func(int) []byte) error {
	b := p.db.NewBatch()
	for i := 0; i < n; i++ {
		b.Set([]byte(key(i)), val(i), nil)
		if b.Len() > 64<<20 {
			if err := b.Commit(p.wo); err != nil {
				return err
			}
			b = p.db.NewBatch()
		}
	}
	return b.Commit(p.wo)
}
func (p *pebbleKV) Close() error { return p.db.Close() }
func (p *pebbleKV) Durability() string {
	if p.wo == pebble.Sync {
		return "pebble.Sync (WAL fsync per commit, concurrent commits share an fsync)"
	}
	return "pebble.NoSync"
}

// ---- bbolt ----

type boltKV struct {
	db    *bolt.DB
	fsync bool
}

var bucket = []byte("kv")

// boltBatchDelay selects how bbolt writes: 0 = one db.Update per op;
// >0 = db.Batch with that MaxBatchDelay. bbolt's default delay (10ms) makes
// every write at low concurrency wait the full delay for companions that
// never arrive ("Batch is only useful when there are multiple goroutines").
var boltBatchDelay time.Duration

func openBolt(dir string, fsync bool) (KV, error) {
	db, err := bolt.Open(filepath.Join(dir, "bolt.db"), 0o600, &bolt.Options{NoSync: !fsync, NoFreelistSync: true})
	if err != nil {
		return nil, err
	}
	if boltBatchDelay > 0 {
		db.MaxBatchDelay = boltBatchDelay
	}
	err = db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucketIfNotExists(bucket); return err })
	return &boltKV{db, fsync}, err
}

var errBoltNotFound = errors.New("bbolt: not found")

func (b *boltKV) Get(k string) (v []byte, err error) {
	err = b.db.View(func(tx *bolt.Tx) error {
		x := tx.Bucket(bucket).Get([]byte(k))
		if x == nil {
			return errBoltNotFound
		}
		v = append([]byte(nil), x...)
		return nil
	})
	return
}

func (b *boltKV) write(fn func(*bolt.Tx) error) error {
	if boltBatchDelay > 0 {
		return b.db.Batch(fn)
	}
	return b.db.Update(fn)
}

func (b *boltKV) Put(k string, v []byte) error {
	return b.write(func(tx *bolt.Tx) error { return tx.Bucket(bucket).Put([]byte(k), v) })
}
func (b *boltKV) RMW(k string, fn func([]byte) []byte) error {
	return b.write(func(tx *bolt.Tx) error {
		bk := tx.Bucket(bucket)
		old := bk.Get([]byte(k))
		return bk.Put([]byte(k), fn(append([]byte(nil), old...)))
	})
}
func (b *boltKV) Load(n int, key func(int) string, val func(int) []byte) error {
	const batch = 10000
	for i := 0; i < n; i += batch {
		if err := b.db.Update(func(tx *bolt.Tx) error {
			bk := tx.Bucket(bucket)
			bk.FillPercent = 0.9
			for j := i; j < i+batch && j < n; j++ {
				if err := bk.Put([]byte(key(j)), val(j)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
func (b *boltKV) Close() error { return b.db.Close() }
func (b *boltKV) Durability() string {
	how := "one db.Update per write"
	if boltBatchDelay > 0 {
		how = fmt.Sprintf("writes via db.Batch, MaxBatchDelay=%v", boltBatchDelay)
	}
	if b.fsync {
		return "default (fdatasync per transaction), " + how
	}
	return "NoSync=true, " + how
}

// ---- SQLite (modernc.org/sqlite, pure Go) ----

type sqliteKV struct {
	w, r     *sql.DB
	get, put *sql.Stmt
	fsync    bool
}

func openSQLite(dir string, fsync bool) (KV, error) {
	sync := "OFF"
	if fsync {
		sync = "FULL"
	}
	dsn := "file:" + filepath.Join(dir, "kv.db") + "?_pragma=journal_mode(WAL)&_pragma=synchronous(" + sync + ")&_pragma=busy_timeout(60000)"
	w, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	w.SetMaxOpenConns(1) // SQLite has one writer; queue writers in Go, not on SQLITE_BUSY
	if _, err := w.Exec("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v BLOB) WITHOUT ROWID"); err != nil {
		return nil, err
	}
	r, err := sql.Open("sqlite", dsn+"&_pragma=query_only(1)")
	if err != nil {
		return nil, err
	}
	r.SetMaxOpenConns(64)
	r.SetMaxIdleConns(64)
	s := &sqliteKV{w: w, r: r, fsync: fsync}
	if s.get, err = r.Prepare("SELECT v FROM kv WHERE k = ?"); err != nil {
		return nil, err
	}
	if s.put, err = w.Prepare("INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v"); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sqliteKV) Get(k string) (v []byte, err error) {
	err = s.get.QueryRow(k).Scan(&v)
	return
}
func (s *sqliteKV) Put(k string, v []byte) error { _, err := s.put.Exec(k, v); return err }
func (s *sqliteKV) RMW(k string, fn func([]byte) []byte) error {
	tx, err := s.w.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var old []byte
	if err := tx.QueryRow("SELECT v FROM kv WHERE k = ?", k).Scan(&old); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := tx.Stmt(s.put).Exec(k, fn(old)); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *sqliteKV) Load(n int, key func(int) string, val func(int) []byte) error {
	const batch = 10000
	for i := 0; i < n; i += batch {
		tx, err := s.w.Begin()
		if err != nil {
			return err
		}
		st := tx.Stmt(s.put)
		for j := i; j < i+batch && j < n; j++ {
			if _, err := st.Exec(key(j), val(j)); err != nil {
				tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
func (s *sqliteKV) Close() error { s.r.Close(); return s.w.Close() }
func (s *sqliteKV) Durability() string {
	if s.fsync {
		return "WAL, synchronous=FULL (WAL fsync per commit), one writer connection"
	}
	return "WAL, synchronous=OFF, one writer connection"
}
