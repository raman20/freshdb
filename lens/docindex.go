package lens

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/raman20/freshdb/format"
)

// DocIndex is a secondary-index lens on one JSON field of one document
// collection (space "doc:<coll>"). It needs plaintext, so it folds the
// redacted log via Env.Open. Add it at any time: it is built by replay.
type DocIndex struct {
	Coll, Field string
	byVal       map[string]map[string]bool // canonical JSON value -> ids
	docs        map[string]docEnt          // id -> current indexed value
}

type docEnt struct {
	val     string
	has     bool
	subject string
}

func NewDocIndex(coll, field string) *DocIndex {
	return &DocIndex{Coll: coll, Field: field, byVal: map[string]map[string]bool{}, docs: map[string]docEnt{}}
}

func (l *DocIndex) Name() string { return "index:" + l.Coll + "." + l.Field }

// CanonicalJSON returns the canonical index key for a JSON value.
func CanonicalJSON(v any) (string, bool) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", false
	}
	return string(b), true
}

func (l *DocIndex) remove(id string) {
	if d, ok := l.docs[id]; ok {
		if d.has {
			delete(l.byVal[d.val], id)
			if len(l.byVal[d.val]) == 0 {
				delete(l.byVal, d.val)
			}
		}
		delete(l.docs, id)
	}
}

func (l *DocIndex) Apply(e *Entry, env Env) {
	space := "doc:" + l.Coll
	for i, op := range e.Ops {
		if op.Kind == format.OpShred {
			l.DropSubject(op.Key)
			continue
		}
		if op.Space != space {
			continue
		}
		switch op.Kind {
		case format.OpDel:
			l.remove(op.Key)
		case format.OpPut:
			l.remove(op.Key)
			plain, ok := env.Open(e, i)
			if !ok {
				continue // shredded: behaves like a delete
			}
			ent := docEnt{subject: op.Subject}
			var m map[string]any
			d := json.NewDecoder(bytes.NewReader(plain))
			d.UseNumber()
			if d.Decode(&m) == nil {
				if v, ok := m[l.Field]; ok {
					ent.val, ent.has = CanonicalJSON(v)
				}
			}
			l.docs[op.Key] = ent
			if ent.has {
				if l.byVal[ent.val] == nil {
					l.byVal[ent.val] = map[string]bool{}
				}
				l.byVal[ent.val][op.Key] = true
			}
		}
	}
}

func (l *DocIndex) DropSubject(s string) {
	for _, id := range sortedKeys(l.docs) {
		if l.docs[id].subject == s {
			l.remove(id)
		}
	}
}

// Lookup returns ids whose field equals the canonical JSON value, sorted.
func (l *DocIndex) Lookup(canon string) []string {
	ids := make([]string, 0, len(l.byVal[canon]))
	for id := range l.byVal[canon] {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (l *DocIndex) Digest() [32]byte {
	e := &enc{}
	e.str(l.Name())
	for _, id := range sortedKeys(l.docs) {
		d := l.docs[id]
		e.str(id)
		e.str(d.val)
		e.bool(d.has)
		e.str(d.subject)
	}
	return digest(e.b)
}
