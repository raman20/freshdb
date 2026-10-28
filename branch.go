package freshdb

import (
	"context"
	"fmt"

	"github.com/raman20/freshdb/lens"
)

func (db *DB) headOf(id string) uint64 {
	if v, ok := db.views[id]; ok {
		v.mu.RLock()
		defer v.mu.RUnlock()
		return v.head
	}
	if l, ok := db.logs[id]; ok {
		return l.head()
	}
	return 0
}

// CreateBranch creates branch `name` from branch `from` at LSN `at`
// (0 = from's head). O(1): one manifest write, no data copied.
func (db *DB) CreateBranch(ctx context.Context, name, from string, at uint64) error {
	db.lockIdle()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if _, ok := db.man.Refs[name]; ok {
		return fmt.Errorf("%w: %q", ErrBranchExists, name)
	}
	fromID, ok := db.man.Refs[from]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNoBranch, from)
	}
	head := db.headOf(fromID)
	if at == 0 {
		at = head
	}
	if at > head {
		return fmt.Errorf("%w: %d > head %d", ErrBadLSN, at, head)
	}
	if f := db.floorOf(fromID); at < f {
		return fmt.Errorf("%w: branch point %d < base %d", ErrCompacted, at, f)
	}
	newID := fmt.Sprintf("L%d", db.man.NextLog)
	if err := db.commitManifest(ctx, func(m *Manifest) error {
		m.NextLog++
		m.Logs[newID] = &LogMeta{ID: newID, Parent: fromID, ParentLSN: at}
		m.Refs[name] = newID
		m.Reflog = append(m.Reflog, RefChange{Name: name, To: newID, At: at, Why: "branch from " + from})
		return nil
	}); err != nil {
		return err
	}
	_, err := db.openWritable(ctx, newID)
	return err
}

// Rewind moves branch `name` back to LSN `at`: a new log forked at `at`
// becomes the branch; the old head stays readable via the reflog
// (ViewLog). Commits racing with Rewind land on the retired log.
func (db *DB) Rewind(ctx context.Context, name string, at uint64) error {
	db.lockIdle()
	oldID, ok := db.man.Refs[name]
	if !ok {
		db.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNoBranch, name)
	}
	if head := db.headOf(oldID); at > head {
		db.mu.Unlock()
		return fmt.Errorf("%w: %d > head %d", ErrBadLSN, at, head)
	}
	if f := db.floorOf(oldID); at < f {
		db.mu.Unlock()
		return fmt.Errorf("%w: rewind target %d < base %d", ErrCompacted, at, f)
	}
	newID := fmt.Sprintf("L%d", db.man.NextLog)
	err := db.commitManifest(ctx, func(m *Manifest) error {
		m.NextLog++
		m.Logs[newID] = &LogMeta{ID: newID, Parent: oldID, ParentLSN: at}
		m.Refs[name] = newID
		m.Reflog = append(m.Reflog, RefChange{Name: name, From: oldID, To: newID, At: at, Why: "rewind"})
		return nil
	})
	if err == nil {
		_, err = db.openWritable(ctx, newID)
	}
	retired := db.retireUnreferenced(oldID)
	db.mu.Unlock()
	if retired != nil {
		retired.stop()
	}
	return err
}

// Promote fast-forwards branch dst to src (dst's head must be in src's
// history). Afterwards both names refer to the same log.
func (db *DB) Promote(ctx context.Context, src, dst string) error {
	db.lockIdle()
	srcID, ok1 := db.man.Refs[src]
	dstID, ok2 := db.man.Refs[dst]
	if !ok1 || !ok2 {
		db.mu.Unlock()
		return ErrNoBranch
	}
	dstHead := db.headOf(dstID)
	ff := false
	reach := db.headOf(srcID)
	// walk the manifest, not loaded parents: a compacted log does not
	// load its parent but is still descended from it
	for m := db.man.Logs[srcID]; m != nil; m = db.man.Logs[m.Parent] {
		if m.ID == dstID {
			ff = reach >= dstHead
			break
		}
		if m.Parent == "" {
			break
		}
		reach = min(reach, m.ParentLSN)
	}
	if !ff {
		db.mu.Unlock()
		return ErrNotFastForward
	}
	err := db.commitManifest(ctx, func(m *Manifest) error {
		m.Refs[dst] = srcID
		m.Reflog = append(m.Reflog, RefChange{Name: dst, From: dstID, To: srcID, At: db.headOf(srcID), Why: "promote " + src})
		return nil
	})
	var retired *writer
	if err == nil {
		retired = db.retireUnreferenced(dstID)
	}
	db.mu.Unlock()
	if retired != nil {
		retired.stop()
	}
	return err
}

// floorOf is the compaction floor of log id's lineage (0 if none),
// from the manifest. Caller holds db.mu.
func (db *DB) floorOf(id string) uint64 {
	for m := db.man.Logs[id]; m != nil; m = db.man.Logs[m.Parent] {
		if m.Base != nil {
			return m.Base.LSN
		}
		if m.Parent == "" {
			break
		}
	}
	return 0
}

// retireUnreferenced removes the writer of a log no branch points to.
// Caller holds db.mu and must stop the returned writer after unlocking.
func (db *DB) retireUnreferenced(id string) *writer {
	for _, ref := range db.man.Refs {
		if ref == id {
			return nil
		}
	}
	w := db.writers[id]
	delete(db.writers, id)
	delete(db.views, id)
	db.bw.Store(nil)
	return w
}

// ViewLog runs a read-only transaction on any log (e.g. a rewound-away
// head from Manifest().Reflog) at LSN at (0 = its head).
func (db *DB) ViewLog(ctx context.Context, logID string, at uint64, fn func(*Tx) error) error {
	db.mu.Lock()
	l, err := db.loadLog(ctx, logID, false)
	idx := append([]IndexDecl{}, db.man.Indexes...)
	db.mu.Unlock()
	if err != nil {
		return err
	}
	if at == 0 || at > l.head() {
		at = l.head()
	}
	v, err := db.buildView(ctx, l, at, idx, nil)
	if err != nil {
		return err
	}
	return fn(&Tx{db: db, v: v, at: at})
}

// RegisterIndex declares a secondary index on a document field. It is
// built by replaying the log (no migration) and maintained from then on.
func (db *DB) RegisterIndex(ctx context.Context, coll, field string) error {
	db.lockIdle()
	defer db.mu.Unlock()
	for _, d := range db.man.Indexes {
		if d.Coll == coll && d.Field == field {
			return nil
		}
	}
	if err := db.commitManifest(ctx, func(m *Manifest) error {
		m.Indexes = append(m.Indexes, IndexDecl{coll, field})
		return nil
	}); err != nil {
		return err
	}
	for _, id := range sortedMapKeys(db.views) {
		v := db.views[id]
		v.mu.Lock() // blocks the writer's apply: the replay is consistent
		ix := lens.NewDocIndex(coll, field)
		err := db.fold(ctx, v.log, v.head, func(e *lens.Entry) error { ix.Apply(e, v.env); return nil })
		if err == nil {
			v.idx[idxKey(coll, field)] = ix
		}
		v.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// ANNOptions configures a collection's HNSW graph; zero fields take the
// defaults (M 16, EfConstruction 100, EfSearch 64).
type ANNOptions struct {
	M              int `json:"m,omitempty"`
	EfConstruction int `json:"ef_construction,omitempty"`
	EfSearch       int `json:"ef_search,omitempty"`
}

func (o ANNOptions) params() lens.ANNParams {
	return lens.ANNParams{M: o.M, EfConstruction: o.EfConstruction, EfSearch: o.EfSearch}
}

// EnableANN gives a vector collection an approximate (HNSW) index, on
// every branch. The declaration is stored in the manifest; graphs are
// built now in every open view and on every later open (by replay, so
// opening gets slower). Search then uses the graph for collections of at
// least ANNExactBelow vectors; SearchExact still scans. Calling it again
// with other options rebuilds the graphs.
func (db *DB) EnableANN(ctx context.Context, coll string, o ANNOptions) error {
	db.lockIdle()
	defer db.mu.Unlock()
	if err := db.commitManifest(ctx, func(m *Manifest) error {
		for i, d := range m.ANN {
			if d.Coll == coll {
				m.ANN[i].ANNOptions = o
				return nil
			}
		}
		m.ANN = append(m.ANN, ANNDecl{coll, o})
		return nil
	}); err != nil {
		return err
	}
	for _, id := range sortedMapKeys(db.views) {
		v := db.views[id]
		v.mu.Lock()
		v.vec.EnableANN(coll, o.params())
		v.mu.Unlock()
	}
	return nil
}

// LensDigests returns the digest of every lens of a branch (for checking
// lens == fold(log)).
func (db *DB) LensDigests(branch string) (map[string][32]byte, uint64, error) {
	w, err := db.branchWriter(branch)
	if err != nil {
		return nil, 0, err
	}
	w.v.mu.RLock()
	defer w.v.mu.RUnlock()
	out := map[string][32]byte{}
	for _, l := range w.v.lenses() {
		out[l.Name()] = l.Digest()
	}
	return out, w.v.head, nil
}

// RebuildDigests folds the branch's log from scratch (no snapshot) and
// returns the lens digests: must equal LensDigests.
func (db *DB) RebuildDigests(branch string) (map[string][32]byte, error) {
	w, err := db.branchWriter(branch)
	if err != nil {
		return nil, err
	}
	db.mu.Lock()
	idx := append([]IndexDecl{}, db.man.Indexes...)
	db.mu.Unlock()
	w.v.mu.RLock()
	h := w.v.head
	w.v.mu.RUnlock()
	v, err := db.buildView(context.Background(), w.l, h, idx, nil)
	if err != nil {
		return nil, err
	}
	out := map[string][32]byte{}
	for _, l := range v.lenses() {
		out[l.Name()] = l.Digest()
	}
	return out, nil
}
