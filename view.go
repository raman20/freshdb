package freshdb

import (
	"context"
	"sync"

	"github.com/raman20/freshdb/lens"
)

// view holds the lenses of one log, folded up to `head`.
type view struct {
	mu    sync.RWMutex
	log   *logState
	head  uint64
	env   lens.Env
	kv    *lens.KV
	topic *lens.Topic
	vec   *lens.Vector
	idx   map[string]*lens.DocIndex // "coll\x00field"
}

func idxKey(coll, field string) string { return coll + "\x00" + field }

func (v *view) lenses() []lens.Lens {
	ls := []lens.Lens{v.kv, v.topic, v.vec}
	for _, k := range sortedMapKeys(v.idx) {
		ls = append(ls, v.idx[k])
	}
	return ls
}

// apply folds one entry into every lens. Caller holds v.mu.
func (v *view) apply(e *lens.Entry) {
	for _, l := range v.lenses() {
		l.Apply(e, v.env)
	}
	v.head = e.LSN
}

// buildView folds the lineage of log up to `upto`. KV and topic lenses may
// start from a persisted snapshot checkpoint; other lenses replay from LSN 0.
func buildView(ctx context.Context, l *logState, upto uint64, env lens.Env, idx []IndexDecl, snap *snapshot, inline int) (*view, error) {
	v := &view{log: l, env: env, kv: lens.NewKVInline(inline), topic: lens.NewTopic(), vec: lens.NewVector(), idx: map[string]*lens.DocIndex{}}
	for _, d := range idx {
		v.idx[idxKey(d.Coll, d.Field)] = lens.NewDocIndex(d.Coll, d.Field)
	}
	var from uint64
	if snap != nil && snap.lsn <= upto {
		v.kv, v.topic, from = snap.kv, snap.topic, snap.lsn
		// the snapshot predates some shreds: redact now (DropSubject is
		// defined to equal folding with the subject redacted from the start)
		for _, s := range snap.drop {
			v.kv.DropSubject(s)
			v.topic.DropSubject(s)
		}
	}
	others := []lens.Lens{v.vec}
	for _, k := range sortedMapKeys(v.idx) {
		others = append(others, v.idx[k])
	}
	err := l.each(ctx, upto, func(e *lens.Entry) error {
		if e.LSN > from {
			v.kv.Apply(e, env)
			v.topic.Apply(e, env)
		}
		for _, o := range others {
			o.Apply(e, env)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	v.head = upto
	return v, nil
}
