package lens

import (
	"bytes"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/raman20/freshdb/format"
)

// DocIndex is a secondary-index lens on one JSON field of one document
// collection (space "doc:<coll>"). It needs plaintext, so it folds the
// redacted log via Env.Open. Add it at any time: it is built by replay.
type DocIndex struct {
	Coll, Field string
	byVal       map[string]map[string]bool // canonical JSON value -> ids
	docs        map[string]docEnt          // id -> current indexed value
	bySubj      map[string]map[string]bool // subject -> ids written under it
}

type docEnt struct {
	val     string
	has     bool
	subject string
}

func NewDocIndex(coll, field string) *DocIndex {
	return &DocIndex{Coll: coll, Field: field, byVal: map[string]map[string]bool{}, docs: map[string]docEnt{}, bySubj: map[string]map[string]bool{}}
}

func (l *DocIndex) Name() string { return "index:" + l.Coll + "." + l.Field }

// CanonicalJSON returns the canonical index key for a JSON value: equal
// JSON values give equal keys. Object keys are sorted (encoding/json) and
// numbers are compared by exact decimal value, so 1, 1.0, 10e-1 and a Go
// float64 1 are one key, while 2^64 and 2^64+1 stay apart (canonNumber).
func CanonicalJSON(v any) (string, bool) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", false
	}
	var x any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if d.Decode(&x) != nil {
		return "", false
	}
	if b, err = json.Marshal(canonNumbers(x)); err != nil {
		return "", false
	}
	return string(b), true
}

func canonNumbers(x any) any {
	switch t := x.(type) {
	case json.Number:
		return json.Number(canonNumber(string(t)))
	case map[string]any:
		for k, v := range t {
			t[k] = canonNumbers(v)
		}
	case []any:
		for i, v := range t {
			t[i] = canonNumbers(v)
		}
	}
	return x
}

// canonNumber rewrites a JSON number literal as its exact decimal value
// in one form: [-]D[eN], D without leading or trailing zeros ("0" for
// zero), e.g. 1.50 -> 15e-1, 100 -> 1e2, -0.0 -> 0. Linear in the literal;
// no float or big-number conversion (a huge exponent costs nothing). An
// exponent that does not fit an int64 keeps the literal as it is.
func canonNumber(s string) string {
	neg := strings.HasPrefix(s, "-")
	m, e := strings.TrimPrefix(s, "-"), ""
	if i := strings.IndexAny(m, "eE"); i >= 0 {
		m, e = m[:i], m[i+1:]
	}
	ip, fp, _ := strings.Cut(m, ".")
	digits := strings.TrimLeft(ip+fp, "0")
	if digits == "" {
		return "0"
	}
	exp := int64(0)
	if e != "" {
		var err error
		if exp, err = strconv.ParseInt(strings.TrimPrefix(e, "+"), 10, 64); err != nil {
			return s // such literals only equal themselves
		}
	}
	if exp < math.MinInt64/2 || exp > math.MaxInt64/2 {
		return s // the shifts below cannot overflow inside this range
	}
	t := strings.TrimRight(digits, "0")
	exp += int64(len(digits)-len(t)) - int64(len(fp))
	if exp != 0 {
		t += "e" + strconv.FormatInt(exp, 10)
	}
	if neg {
		t = "-" + t
	}
	return t
}

// DocField returns the canonical JSON of field in the JSON object doc,
// and false if doc is not an object or has no such field. The index and
// an unindexed Find both use it, so they agree on every document.
func DocField(doc []byte, field string) (string, bool) {
	var m map[string]any
	d := json.NewDecoder(bytes.NewReader(doc))
	d.UseNumber()
	if d.Decode(&m) != nil {
		return "", false
	}
	v, ok := m[field]
	if !ok {
		return "", false
	}
	return CanonicalJSON(v)
}

func (l *DocIndex) remove(id string) {
	if d, ok := l.docs[id]; ok {
		if d.has {
			delete(l.byVal[d.val], id)
			if len(l.byVal[d.val]) == 0 {
				delete(l.byVal, d.val)
			}
		}
		if d.subject != "" {
			delete(l.bySubj[d.subject], id)
			if len(l.bySubj[d.subject]) == 0 {
				delete(l.bySubj, d.subject)
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
			ent.val, ent.has = DocField(plain, l.Field)
			l.docs[op.Key] = ent
			if ent.subject != "" {
				if l.bySubj[ent.subject] == nil {
					l.bySubj[ent.subject] = map[string]bool{}
				}
				l.bySubj[ent.subject][op.Key] = true
			}
			if ent.has {
				if l.byVal[ent.val] == nil {
					l.byVal[ent.val] = map[string]bool{}
				}
				l.byVal[ent.val][op.Key] = true
			}
		}
	}
}

// DropSubject forgets every document written under subject s: O(its
// documents), not O(collection) (D-D4).
func (l *DocIndex) DropSubject(s string) {
	for _, id := range sortedKeys(l.bySubj[s]) {
		l.remove(id)
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
