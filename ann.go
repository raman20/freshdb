package freshdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// Persisted HNSW graphs (docs/design/10-hnsw-persist.md). On Close every
// live view's graphs are saved as ann/<log>/<lsn>-<epoch>-<part>of<n>; on
// open the newest complete snapshot at or below the log head is decoded
// and each node is checked against the vector now in the view (lens
// EnableANNFrom), so a stale, partial or foreign snapshot costs at most a
// rebuild. The objects are a cache: not in the manifest, not in digests,
// deleted by Shred and when GC drops their log.

const annPart = 64 << 20

func annDir(id string) string { return "ann/" + id + "/" }

type annKey struct {
	lsn, epoch uint64
	part, n    int
}

func parseANNKey(k, dir string) (annKey, bool) {
	var a annKey
	s := strings.TrimPrefix(k, dir)
	if len(s) == len(k) {
		return a, false
	}
	_, err := fmt.Sscanf(s, "%020d-%020d-%04dof%04d", &a.lsn, &a.epoch, &a.part, &a.n)
	return a, err == nil && a.n > 0 && a.part < a.n
}

// loadANN returns the newest complete snapshot of log id at or below head.
func (db *DB) loadANN(ctx context.Context, id string, head uint64) lens.ANNSnapshot {
	if db.annCarry != nil {
		if s, ok := db.annCarry[id]; ok {
			return s
		}
	}
	keys, err := db.st.List(ctx, annDir(id))
	if err != nil || len(keys) == 0 {
		return nil
	}
	type set struct {
		n    int
		keys map[int]string
	}
	sets := map[[2]uint64]*set{}
	var best [2]uint64
	found := false
	for _, k := range keys {
		a, ok := parseANNKey(k, annDir(id))
		if !ok || a.lsn > head {
			continue
		}
		v := [2]uint64{a.lsn, a.epoch}
		s := sets[v]
		if s == nil {
			s = &set{n: a.n, keys: map[int]string{}}
			sets[v] = s
		}
		if s.n == a.n {
			s.keys[a.part] = k
		}
		if len(s.keys) == s.n && (!found || v[0] > best[0] || v[0] == best[0] && v[1] > best[1]) {
			best, found = v, true
		}
	}
	if !found {
		return nil
	}
	s := sets[best]
	var blob []byte
	for i := 0; i < s.n; i++ {
		b, err := db.st.Get(ctx, s.keys[i])
		if err != nil {
			db.logf("freshdb: ann snapshot %s: %v (rebuilding)", s.keys[i], err)
			return nil
		}
		blob = append(blob, b...)
	}
	snap, err := lens.DecodeANN(blob)
	if err != nil {
		db.logf("freshdb: ann snapshot of log %s at %d: %v (rebuilding)", id, best[0], err)
		return nil
	}
	return snap
}

// enableANN builds the view's graphs, from a snapshot when one fits.
func (db *DB) enableANN(ctx context.Context, id string, v *view, head uint64) {
	if len(db.man.ANN) == 0 {
		return
	}
	t0 := time.Now()
	snap := db.loadANN(ctx, id, head)
	var tot lens.ANNLoadStats
	for _, d := range db.man.ANN {
		st := v.vec.EnableANNFrom(d.Coll, d.params(), snap)
		tot.Graphs += st.Graphs
		tot.Rebuilt += st.Rebuilt
		tot.Reused += st.Reused
		tot.Stale += st.Stale
		tot.Inserted += st.Inserted
	}
	if db.annStats != nil {
		db.annStats[id] = tot
	}
	if tot.Graphs > 0 {
		db.logf("freshdb: log %s: ANN graphs restored in %v (%d reused, %d stale, %d inserted, %d rebuilt)",
			id, time.Since(t0).Round(time.Millisecond), tot.Reused, tot.Stale, tot.Inserted, tot.Rebuilt)
	}
}

// saveANN writes a stopped writer's graphs and deletes the log's older
// snapshots. A zombie (another opener fenced the log) must not leave a
// snapshot behind: it could hold hashes of vectors the new owner shreds,
// after that shred's sweep. So the fence segment is checked after the put
// and the snapshot is deleted if it is there: a fence before our put is
// seen; a fence after it precedes any shred sweep, which then deletes it.
// Errors are logged: the graph is a cache.
func (db *DB) saveANN(ctx context.Context, w *writer) {
	id, v := w.l.meta.ID, w.v
	head := w.l.head()
	w.l.mu.RLock()
	next := w.l.nextSeq
	w.l.mu.RUnlock()
	fenced := func() bool {
		_, err := db.st.Get(ctx, segKey(id, next))
		return !errors.Is(err, store.ErrNotFound) // an error counts as fenced
	}
	if fenced() {
		return
	}
	v.mu.RLock()
	blob := v.vec.MarshalANN()
	v.mu.RUnlock()
	if blob == nil {
		return
	}
	n := (len(blob) + annPart - 1) / annPart
	var mine []string
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%s%020d-%020d-%04dof%04d", annDir(id), head, db.epoch, i, n)
		mine = append(mine, k)
		part := blob[i*annPart : min(len(blob), (i+1)*annPart)]
		if err := db.st.PutIfAbsent(ctx, k, part); err != nil && !errors.Is(err, store.ErrExists) {
			db.logf("freshdb: saving ann snapshot of log %s: %v", id, err)
			break
		}
	}
	if len(mine) < n || fenced() {
		for _, k := range mine {
			_ = db.st.Delete(ctx, k)
		}
		return
	}
	keys, err := db.st.List(ctx, annDir(id))
	if err != nil {
		return
	}
	for _, k := range keys {
		if a, ok := parseANNKey(k, annDir(id)); ok && (a.lsn < head || a.lsn == head && a.epoch < db.epoch) {
			_ = db.st.Delete(ctx, k)
		}
	}
}

// dropANN deletes every saved graph of log id.
func (db *DB) dropANN(ctx context.Context, id string) (int, error) {
	keys, err := db.st.List(ctx, annDir(id))
	if err != nil {
		return 0, err
	}
	for i, k := range keys {
		if err := db.st.Delete(ctx, k); err != nil && !errors.Is(err, store.ErrNotFound) {
			return i, err
		}
	}
	return len(keys), nil
}

// CheckANN verifies the structure of a branch's HNSW graphs against its
// vectors (for tests and simulations).
func (db *DB) CheckANN(branch string) error {
	db.mu.RLock()
	v := db.views[db.man.Refs[branch]]
	db.mu.RUnlock()
	if v == nil {
		return fmt.Errorf("%w: %q not open", ErrNoBranch, branch)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.vec.CheckANN()
}
