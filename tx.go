package freshdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/lens"
)

// Space names: every model is a key space in the one log.
const (
	spaceKV     = "kv"
	spaceCursor = "__cursor"
)

func docSpace(c string) string   { return "doc:" + c }
func vecSpace(c string) string   { return "vec:" + c }
func topicSpace(t string) string { return "topic:" + t }

// WriteOpt modifies a write.
type WriteOpt func(*format.Op)

// Subject encrypts the value under the subject's own key, so Shred(subject)
// makes it unreadable everywhere (all branches, snapshots, lenses).
// Subject ids are hashed before they reach the log.
func Subject(s string) WriteOpt { return func(o *format.Op) { o.Subject = s } }

// Tx is a transaction. Update transactions are serializable: reads come
// from a snapshot at LSN At(), and commit fails with ErrConflict if anything
// read (keys, scanned prefixes, consumer cursors) changed since. Vector and
// index queries read the lens state and are not part of the read set.
type Tx struct {
	db       *DB
	v        *view
	at       uint64
	writable bool
	ops      []format.Op
	reads    map[rkey]bool
	prefixes []rkey
	local    map[rkey]*localVal
	// Ephemeral exact view for time travel, built on first use by
	// lensView (read-only branch views only; nil lazyLog = never).
	lazyCtx context.Context
	lazyLog *logState
	eph     *view
}

type localVal struct {
	val []byte
	del bool
}

// At returns the snapshot LSN this transaction reads at.
func (tx *Tx) At() uint64 { return tx.at }

func (tx *Tx) write(op format.Op, opts []WriteOpt) error {
	if !tx.writable {
		return ErrReadOnly
	}
	for _, o := range opts {
		o(&op)
	}
	if n := max(len(op.Key), len(op.Space), len(op.Subject)); n > MaxKeyBytes {
		return fmt.Errorf("%w: key or name of %d bytes (max %d)", ErrTooLarge, n, MaxKeyBytes)
	}
	if op.Subject != "" && tx.db.keys.isShredded(SubjectHash(op.Subject)) {
		return ErrShredded
	}
	// The caller may reuse its buffer once the call returns; small values
	// are kept inline in the lens, so never retain the caller's slice.
	op.Value = bytes.Clone(op.Value)
	tx.ops = append(tx.ops, op)
	if op.Kind == format.OpPut || op.Kind == format.OpDel {
		tx.local[rkey{op.Space, op.Key}] = &localVal{val: op.Value, del: op.Kind == format.OpDel}
	}
	return nil
}

// get reads (space,key) at the snapshot, decrypting subject data.
func (tx *Tx) get(space, key string) ([]byte, error) {
	k := rkey{space, key}
	if l, ok := tx.local[k]; ok {
		if l.del {
			return nil, ErrNotFound
		}
		return l.val, nil
	}
	if tx.writable {
		tx.reads[k] = true
	}
	tx.v.mu.RLock()
	ver, ok := tx.v.kv.Get(space, key, tx.at)
	tx.v.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return tx.open(ver, space, key)
}

// open returns the plaintext of a stored version: it reads the bytes from
// the log when the lens keeps only their position, and decrypts subject
// data. Shredded data is indistinguishable from absent (ErrNotFound).
func (tx *Tx) open(ver lens.Version, space, key string) ([]byte, error) {
	val, owned := ver.Val, false // ver.Val is shared lens state
	var logID string
	if ver.Ref() || ver.Subject != "" {
		x, seq, ok := tx.v.log.source(ver)
		if !ok {
			return nil, fmt.Errorf("%w: LSN %d of %s/%s not in the log", ErrCorrupt, ver.LSN, space, key)
		}
		logID = x.meta.ID
		if ver.Ref() {
			var err error
			if val, err = tx.db.readValue(x, seq, ver); err != nil {
				return nil, err
			}
			owned = true // readValue returns a private copy
		}
	}
	if ver.Subject == "" {
		if owned {
			return val, nil
		}
		return bytes.Clone(val), nil // the caller owns what it gets back
	}
	if p, ok := tx.db.keys.open(ver.Subject, val, aad(logID, ver.LSN, int(ver.Op), space, key)); ok {
		return p, nil
	}
	return nil, ErrNotFound
}

// ---- KV ----

func (tx *Tx) Put(key string, val []byte, opts ...WriteOpt) error {
	return tx.write(format.Op{Kind: format.OpPut, Space: spaceKV, Key: key, Value: val}, opts)
}
func (tx *Tx) Delete(key string) error {
	return tx.write(format.Op{Kind: format.OpDel, Space: spaceKV, Key: key}, nil)
}
func (tx *Tx) Get(key string) ([]byte, error) { return tx.get(spaceKV, key) }

type KV struct {
	Key   string
	Value []byte
}

// Scan returns live keys with prefix (sorted), including this tx's writes.
func (tx *Tx) Scan(prefix string) ([]KV, error) { return tx.scan(spaceKV, prefix) }

func (tx *Tx) scan(space, prefix string) ([]KV, error) {
	if tx.writable {
		tx.prefixes = append(tx.prefixes, rkey{space, prefix})
	}
	tx.v.mu.RLock()
	ps := tx.v.kv.Scan(space, prefix, tx.at)
	tx.v.mu.RUnlock()
	m := map[string][]byte{}
	for _, p := range ps {
		v, err := tx.open(p.Version, space, p.Key)
		if errors.Is(err, ErrNotFound) {
			continue // shredded
		}
		if err != nil {
			return nil, err
		}
		m[p.Key] = v
	}
	for k, l := range tx.local {
		if k.space == space && strings.HasPrefix(k.key, prefix) {
			if l.del {
				delete(m, k.key)
			} else {
				m[k.key] = l.val
			}
		}
	}
	out := make([]KV, 0, len(m))
	for _, k := range sortedMapKeys(m) {
		out = append(out, KV{k, m[k]})
	}
	return out, nil
}

// ---- Documents ----

type DocTx struct {
	tx   *Tx
	coll string
}

func (tx *Tx) Doc(coll string) DocTx { return DocTx{tx, coll} }

func (d DocTx) Put(id string, doc any, opts ...WriteOpt) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if !json.Valid(b) || len(b) == 0 || b[0] != '{' {
		return fmt.Errorf("freshdb: document must be a JSON object")
	}
	return d.tx.write(format.Op{Kind: format.OpPut, Space: docSpace(d.coll), Key: id, Value: b}, opts)
}
func (d DocTx) Delete(id string) error {
	return d.tx.write(format.Op{Kind: format.OpDel, Space: docSpace(d.coll), Key: id}, nil)
}

// Get returns the raw JSON of a document.
func (d DocTx) Get(id string) (json.RawMessage, error) { return d.tx.get(docSpace(d.coll), id) }

// Find returns ids of documents whose field equals value. Uses the
// secondary-index lens if one is registered, else scans the collection.
// Results reflect committed state at the lens head (not this tx's writes).
func (d DocTx) Find(field string, value any) ([]string, error) {
	canon, ok := lens.CanonicalJSON(value)
	if !ok {
		return nil, fmt.Errorf("freshdb: unindexable value")
	}
	v, err := d.tx.lensView()
	if err != nil {
		return nil, err
	}
	v.mu.RLock()
	ix := v.idx[idxKey(d.coll, field)]
	if ix != nil {
		ids := ix.Lookup(canon)
		v.mu.RUnlock()
		return ids, nil
	}
	v.mu.RUnlock()
	all, err := d.tx.scan(docSpace(d.coll), "")
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, kv := range all {
		var m map[string]any
		dec := json.NewDecoder(bytes.NewReader(kv.Value))
		dec.UseNumber()
		if dec.Decode(&m) == nil {
			if fv, ok := m[field]; ok {
				if c, ok := lens.CanonicalJSON(fv); ok && c == canon {
					ids = append(ids, kv.Key)
				}
			}
		}
	}
	return ids, nil
}

// ---- Vectors ----

type VecTx struct {
	tx   *Tx
	coll string
}

func (tx *Tx) Vector(coll string) VecTx { return VecTx{tx, coll} }

func (v VecTx) Put(id string, vec []float32, opts ...WriteOpt) error {
	if _, ok := lens.DecodeVec(lens.EncodeVec(vec)); !ok {
		return fmt.Errorf("freshdb: vector must be non-empty and finite")
	}
	return v.tx.write(format.Op{Kind: format.OpPut, Space: vecSpace(v.coll), Key: id, Value: lens.EncodeVec(vec)}, opts)
}
func (v VecTx) Delete(id string) error {
	return v.tx.write(format.Op{Kind: format.OpDel, Space: vecSpace(v.coll), Key: id}, nil)
}

// Search returns the k most cosine-similar vectors, best first (ties by
// id). Collections without EnableANN, or with fewer than ANNExactBelow
// vectors, are scanned exactly; otherwise the HNSW graph answers
// (approximate), falling back to the exact scan if it finds fewer than k.
func (v VecTx) Search(q []float32, k int) ([]lens.Hit, error) {
	return v.search(q, k, 0, false)
}

// SearchExact always scans every vector (exact top-k).
func (v VecTx) SearchExact(q []float32, k int) ([]lens.Hit, error) {
	return v.search(q, k, 0, true)
}

// SearchANN is Search with beam width ef (0 = the collection's EfSearch).
func (v VecTx) SearchANN(q []float32, k, ef int) ([]lens.Hit, error) {
	return v.search(q, k, ef, false)
}

// ANNExactBelow is the collection size under which Search scans exactly
// even if the collection has an HNSW graph.
const ANNExactBelow = 4096

func (v VecTx) search(q []float32, k, ef int, exact bool) ([]lens.Hit, error) {
	vw, err := v.tx.lensView()
	if err != nil {
		return nil, err
	}
	u, ok := lens.Normalize(q)
	if !ok || k <= 0 {
		return nil, nil
	}
	vw.mu.RLock()
	defer vw.mu.RUnlock()
	if !exact {
		if hs, ok := vw.vec.ANN(v.coll, u, k, ef, ANNExactBelow); ok && len(hs) >= min(k, vw.vec.Slots(v.coll, len(u))) {
			return lens.MergeHits([][]lens.Hit{hs}, k), nil
		}
	}
	return searchParallel(vw.vec, v.coll, u, k), nil
}

// vecChunk is the smallest slot range worth its own goroutine.
const vecChunk = 8192

// searchParallel splits a brute-force scan across cores. The result equals
// the sequential one: hits are totally ordered by (score desc, id asc) and
// each score is computed by the same code whichever goroutine runs it.
func searchParallel(vl *lens.Vector, coll string, u []float32, k int) []lens.Hit {
	n := vl.Slots(coll, len(u))
	p := min(runtime.GOMAXPROCS(0), n/vecChunk)
	if p <= 1 {
		return lens.MergeHits([][]lens.Hit{vl.TopK(coll, u, k, 0, n)}, k)
	}
	parts := make([][]lens.Hit, p)
	var wg sync.WaitGroup
	for i := range parts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			parts[i] = vl.TopK(coll, u, k, n*i/p, n*(i+1)/p)
		}(i)
	}
	wg.Wait()
	return lens.MergeHits(parts, k)
}

// lensView returns the view non-MVCC lenses should be read from: the live
// view when reading at its head, else an exact replay at tx.at.
func (tx *Tx) lensView() (*view, error) {
	tx.v.mu.RLock()
	h := tx.v.head
	tx.v.mu.RUnlock()
	if tx.lazyLog == nil || h == tx.at || tx.writable {
		return tx.v, nil
	}
	if tx.eph == nil {
		tx.db.mu.Lock()
		idx := append([]IndexDecl{}, tx.db.man.Indexes...)
		tx.db.mu.Unlock()
		v, err := tx.db.buildView(tx.lazyCtx, tx.lazyLog, tx.at, idx, nil)
		if err != nil {
			return nil, err
		}
		tx.eph = v
	}
	return tx.eph, nil
}

// ---- Topics and consumer groups ----

// Message is one topic message. Value is nil and Redacted is true if the
// message's subject was shredded.
type Message struct {
	Topic    string `json:"topic"`
	Offset   uint64 `json:"offset"`
	LSN      uint64 `json:"lsn"`
	TS       int64  `json:"ts"`
	Value    []byte `json:"value"`
	Redacted bool   `json:"redacted,omitempty"`
}

type TopicTx struct {
	tx    *Tx
	topic string
}

func (tx *Tx) Topic(name string) TopicTx { return TopicTx{tx, name} }

// Publish appends a message; its offset is assigned at commit.
func (t TopicTx) Publish(val []byte, opts ...WriteOpt) error {
	return t.tx.write(format.Op{Kind: format.OpAppend, Space: topicSpace(t.topic), Value: val}, opts)
}

// Read returns up to max committed messages starting at offset from.
func (t TopicTx) Read(from uint64, max int) ([]Message, error) {
	t.tx.v.mu.RLock()
	ms := t.tx.v.topic.Read(topicSpace(t.topic), from, max, t.tx.at)
	t.tx.v.mu.RUnlock()
	out := make([]Message, 0, len(ms))
	for _, m := range ms {
		msg := Message{Topic: t.topic, Offset: m.Offset, LSN: m.LSN, TS: m.TS, Redacted: m.Redacted}
		if !m.Redacted {
			if m.Subject == "" {
				msg.Value = bytes.Clone(m.Val) // m.Val is shared lens state
			} else if p, err := t.tx.openAppend(topicSpace(t.topic), m); err == nil {
				msg.Value = p
			} else {
				msg.Redacted = true
			}
		}
		out = append(out, msg)
	}
	return out, nil
}

func (tx *Tx) openAppend(space string, m lens.Msg) ([]byte, error) {
	return tx.db.openMsg(tx.v.log, space, m)
}

// openMsg decrypts a subject topic message of lineage l. Messages in a
// compaction base were re-sealed under baseMsgAAD; others are found by
// re-reading their log entry.
func (db *DB) openMsg(l *logState, space string, m lens.Msg) ([]byte, error) {
	if b := l.based(); b != nil && m.LSN <= b.meta.Base.LSN {
		if p, ok := db.keys.open(m.Subject, m.Val, baseMsgAAD(b.meta.ID, space, m)); ok {
			return p, nil
		}
		return nil, ErrShredded
	}
	e, err := l.entryAt(context.Background(), m.LSN)
	if err != nil {
		return nil, err
	}
	for i, op := range e.Ops {
		if op.Kind == format.OpAppend && bytes.Equal(op.Value, m.Val) {
			if p, ok := db.env.Open(e, i); ok {
				return p, nil
			}
		}
	}
	return nil, ErrShredded
}

// Len returns the number of committed messages in the topic.
func (t TopicTx) Len() uint64 {
	t.tx.v.mu.RLock()
	defer t.tx.v.mu.RUnlock()
	return t.tx.v.topic.Len(topicSpace(t.topic), t.tx.at)
}

// Group is a consumer group: a committed cursor stored in the log (KV).
type Group struct {
	tx          *Tx
	name, topic string
}

func (t TopicTx) Group(name string) Group { return Group{t.tx, name, t.topic} }

func cursorKey(group, topic string) string { return group + "\x00" + topic }

// Cursor returns the next offset the group will consume.
func (g Group) Cursor() (uint64, error) {
	b, err := g.tx.get(spaceCursor, cursorKey(g.name, g.topic))
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil || len(b) != 8 {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

// Fetch returns up to max messages after the group's committed cursor.
// The cursor is in the read set: two transactions consuming the same
// message cannot both commit (exactly-once processing with Ack).
func (g Group) Fetch(max int) ([]Message, error) {
	c, err := g.Cursor()
	if err != nil {
		return nil, err
	}
	return g.tx.Topic(g.topic).Read(c, max)
}

// Ack commits the cursor past msg, atomically with the rest of the tx.
func (g Group) Ack(msg Message) error { return g.Seek(msg.Offset + 1) }

// Seek sets the group's cursor.
func (g Group) Seek(offset uint64) error {
	return g.tx.write(format.Op{Kind: format.OpPut, Space: spaceCursor, Key: cursorKey(g.name, g.topic),
		Value: binary.LittleEndian.AppendUint64(nil, offset)}, nil)
}

// ---- running transactions ----

// Branch is a handle on a named branch ("main" is the default).
type Branch struct {
	db   *DB
	name string
}

// Branch returns a handle on a named branch.
func (db *DB) Branch(name string) *Branch { return &Branch{db, name} }

const maxRetries = 16

// escalateAfter is how many conflicts a transaction may lose optimistically.
// After that its body runs inside the branch's single writer, at the
// applied head and validated before anything else in its group commit, so
// it commits on that attempt: a hot key cannot starve a client. The log is
// unchanged (the same ops are committed); only where fn runs differs.
var escalateAfter = 3 // var only so tests can force the escalated path

// Update runs fn in a serializable read-write transaction on main.
// fn may be re-run on ErrConflict, so it must be free of side effects.
// After escalateAfter lost conflicts fn runs on the branch's writer
// goroutine (so it always commits); it must therefore not block or call
// Update on the same DB.
func (db *DB) Update(ctx context.Context, fn func(*Tx) error) error {
	return db.Branch("main").Update(ctx, fn)
}

// View runs fn in a read-only snapshot transaction on main.
func (db *DB) View(ctx context.Context, fn func(*Tx) error) error {
	return db.Branch("main").View(ctx, fn)
}

// UpdateLSN is Update but also returns the commit LSN.
func (b *Branch) UpdateLSN(ctx context.Context, fn func(*Tx) error) (uint64, error) {
	var last error
	for i := 0; i < maxRetries; i++ {
		w, err := b.db.branchWriter(b.name)
		if err != nil {
			return 0, err
		}
		if i >= escalateAfter {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			res := w.submit(&commitReq{attempt: i, exec: fn})
			if errors.Is(res.err, errPaused) { // compaction swapped the writer: not an attempt
				if err := b.db.waitCompaction(ctx); err != nil {
					return 0, err
				}
				i--
				continue
			}
			if res.panicked != nil {
				panic(res.panicked)
			}
			if !errors.Is(res.err, ErrConflict) {
				return res.lsn, res.err
			}
			last = res.err // cannot happen: validated first at the head
			continue
		}
		tx := &Tx{db: b.db, v: w.v, writable: true, reads: map[rkey]bool{}, local: map[rkey]*localVal{}}
		w.v.mu.RLock()
		tx.at = w.v.head
		w.v.mu.RUnlock()
		if err := fn(tx); err != nil {
			return 0, err
		}
		if len(tx.ops) == 0 {
			return tx.at, nil
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		res := w.submit(&commitReq{at: tx.at, ops: tx.ops, reads: tx.reads, prefixes: tx.prefixes, attempt: i})
		if errors.Is(res.err, errPaused) {
			if err := b.db.waitCompaction(ctx); err != nil {
				return 0, err
			}
			i--
			continue
		}
		if errors.Is(res.err, ErrConflict) {
			last = res.err
			if err := conflictBackoff(ctx, i); err != nil {
				return 0, err
			}
			continue
		}
		return res.lsn, res.err
	}
	return 0, last
}

// conflictBackoff waits a jittered, exponentially growing delay after the
// i-th lost conflict (none after the first: the retry already reads the
// winner's commit). It spreads clients contending for one hot key across
// group commits; the cap keeps the worst case of 16 attempts near 40 ms.
func conflictBackoff(ctx context.Context, i int) error {
	if i == 0 {
		return nil
	}
	d := time.Duration(rand.Int64N(int64(50*time.Microsecond) << min(i, 6)))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (b *Branch) Update(ctx context.Context, fn func(*Tx) error) error {
	_, err := b.UpdateLSN(ctx, fn)
	return err
}

// View runs a read-only transaction at the branch head.
func (b *Branch) View(ctx context.Context, fn func(*Tx) error) error {
	return b.ViewAt(ctx, 0, fn)
}

// ViewAt runs a read-only transaction at LSN `at` (0 = head): time travel.
func (b *Branch) ViewAt(ctx context.Context, at uint64, fn func(*Tx) error) error {
	w, err := b.db.branchWriter(b.name)
	if err != nil {
		return err
	}
	w.v.mu.RLock()
	head := w.v.head
	w.v.mu.RUnlock()
	if at == 0 {
		at = head
	}
	if at > head {
		return fmt.Errorf("%w: %d > head %d", ErrBadLSN, at, head)
	}
	if f := w.l.floor(); at < f {
		return fmt.Errorf("%w: LSN %d < base %d", ErrCompacted, at, f)
	}
	// Read-only: no read set and no local writes, so no maps to allocate.
	tx := &Tx{db: b.db, v: w.v, at: at, lazyCtx: ctx, lazyLog: w.l}
	return fn(tx)
}

// Head returns the branch head LSN.
func (b *Branch) Head() (uint64, error) {
	w, err := b.db.branchWriter(b.name)
	if err != nil {
		return 0, err
	}
	w.v.mu.RLock()
	defer w.v.mu.RUnlock()
	return w.v.head, nil
}

// Name returns the branch name.
func (b *Branch) Name() string { return b.name }

// ---- conveniences on main ----

func (db *DB) Put(ctx context.Context, key string, val []byte, opts ...WriteOpt) error {
	return db.Update(ctx, func(tx *Tx) error { return tx.Put(key, val, opts...) })
}

func (db *DB) Get(ctx context.Context, key string) (v []byte, err error) {
	err = db.View(ctx, func(tx *Tx) error { v, err = tx.Get(key); return err })
	return
}
