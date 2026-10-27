package lens

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/raman20/freshdb/format"
)

// Vector is a brute-force cosine-similarity lens over "vec:<coll>" spaces.
// Values are little-endian float32 arrays. It folds the redacted log, so
// deleted and shredded vectors can never be returned, and it is rebuilt by
// replay on open, so vectors are never lost on restart.
type Vector struct {
	m map[string]map[string]vecEnt // coll -> id -> entry
}

type vecEnt struct {
	unit    []float32
	subject string
}

func NewVector() *Vector { return &Vector{m: map[string]map[string]vecEnt{}} }

func (*Vector) Name() string { return "vector" }

const vecPrefix = "vec:"

func EncodeVec(v []float32) []byte {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(f))
	}
	return b
}

func DecodeVec(b []byte) ([]float32, bool) {
	if len(b)%4 != 0 || len(b) == 0 {
		return nil, false
	}
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
		if math.IsNaN(float64(v[i])) || math.IsInf(float64(v[i]), 0) {
			return nil, false
		}
	}
	return v, true
}

func normalize(v []float32) ([]float32, bool) {
	var s float64
	for _, f := range v {
		s += float64(f) * float64(f)
	}
	if s == 0 {
		return nil, false
	}
	n := math.Sqrt(s)
	out := make([]float32, len(v))
	for i, f := range v {
		out[i] = float32(float64(f) / n)
	}
	return out, true
}

func (l *Vector) Apply(e *Entry, env Env) {
	for i, op := range e.Ops {
		if op.Kind == format.OpShred {
			l.DropSubject(op.Key)
			continue
		}
		if len(op.Space) <= len(vecPrefix) || op.Space[:len(vecPrefix)] != vecPrefix {
			continue
		}
		coll := op.Space[len(vecPrefix):]
		switch op.Kind {
		case format.OpDel:
			l.del(coll, op.Key)
		case format.OpPut:
			l.del(coll, op.Key)
			plain, ok := env.Open(e, i)
			if !ok {
				continue
			}
			v, ok := DecodeVec(plain)
			if !ok {
				continue
			}
			u, ok := normalize(v)
			if !ok {
				continue
			}
			if l.m[coll] == nil {
				l.m[coll] = map[string]vecEnt{}
			}
			l.m[coll][op.Key] = vecEnt{unit: u, subject: op.Subject}
		}
	}
}

func (l *Vector) DropSubject(s string) {
	for coll, c := range l.m {
		for id, e := range c {
			if e.subject == s {
				delete(c, id)
			}
		}
		if len(c) == 0 {
			delete(l.m, coll) // canonical form: no empty collections
		}
	}
}

func (l *Vector) del(coll, id string) {
	if c := l.m[coll]; c != nil {
		delete(c, id)
		if len(c) == 0 {
			delete(l.m, coll)
		}
	}
}

type Hit struct {
	ID    string
	Score float32
}

// Search returns the top-k ids by cosine similarity (ties broken by id).
func (l *Vector) Search(coll string, q []float32, k int) []Hit {
	u, ok := normalize(q)
	if !ok || k <= 0 {
		return nil
	}
	var hits []Hit
	for id, e := range l.m[coll] {
		if len(e.unit) != len(u) {
			continue
		}
		var s float32
		for i := range u {
			s += u[i] * e.unit[i]
		}
		hits = append(hits, Hit{id, s})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].ID < hits[j].ID
	})
	if len(hits) > k {
		hits = hits[:k]
	}
	return hits
}

func (l *Vector) Count(coll string) int { return len(l.m[coll]) }

func (l *Vector) Digest() [32]byte {
	e := &enc{}
	e.str("VEC1")
	for _, c := range sortedKeys(l.m) {
		e.str(c)
		for _, id := range sortedKeys(l.m[c]) {
			ent := l.m[c][id]
			e.str(id)
			e.str(ent.subject)
			e.bytes(EncodeVec(ent.unit))
		}
	}
	return digest(e.b)
}
