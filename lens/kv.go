package lens

import (
	"errors"
	"sort"
	"strings"

	"github.com/raman20/freshdb/format"
)

// Version is one MVCC version of a key.
type Version struct {
	LSN     uint64
	Val     []byte // stored bytes (ciphertext if Subject != "")
	Subject string
	Del     bool // delete, or redacted (shredded) put
}

// KV is the key-value lens: a multi-version map over every PUT/DEL in the
// log, in all spaces (kv, doc:*, vec:*, __cursor). It folds the raw log
// (values stay ciphertext for subject data), so it can be snapshotted
// without leaking plaintext; decryption happens at read time.
type KV struct {
	m map[string][]Version // space \x00 key -> versions ascending by LSN
}

func NewKV() *KV { return &KV{m: map[string][]Version{}} }

func (*KV) Name() string { return "kv" }

func ck(space, key string) string { return space + "\x00" + key }

func (l *KV) Apply(e *Entry, env Env) {
	for _, op := range e.Ops {
		switch op.Kind {
		case format.OpPut:
			v := Version{LSN: e.LSN, Val: op.Value, Subject: op.Subject}
			if op.Subject != "" && env.Shredded(op.Subject) {
				v.Val, v.Del = nil, true
			}
			l.m[ck(op.Space, op.Key)] = append(l.m[ck(op.Space, op.Key)], v)
		case format.OpDel:
			l.m[ck(op.Space, op.Key)] = append(l.m[ck(op.Space, op.Key)], Version{LSN: e.LSN, Del: true})
		case format.OpShred:
			l.DropSubject(op.Key)
		}
	}
}

func (l *KV) DropSubject(s string) {
	for _, vs := range l.m {
		for i := range vs {
			if vs[i].Subject == s {
				vs[i].Val, vs[i].Del = nil, true
			}
		}
	}
}

// Get returns the version of (space,key) visible at LSN `at`.
func (l *KV) Get(space, key string, at uint64) (Version, bool) {
	vs := l.m[ck(space, key)]
	i := sort.Search(len(vs), func(i int) bool { return vs[i].LSN > at }) - 1
	if i < 0 || vs[i].Del {
		return Version{}, false
	}
	return vs[i], true
}

// LastWrite returns the LSN of the latest write to (space,key), 0 if none.
func (l *KV) LastWrite(space, key string) uint64 {
	vs := l.m[ck(space, key)]
	if len(vs) == 0 {
		return 0
	}
	return vs[len(vs)-1].LSN
}

type Pair struct {
	Key string
	Version
}

// Scan returns live keys in space with prefix at LSN `at`, sorted.
func (l *KV) Scan(space, prefix string, at uint64) []Pair {
	p := ck(space, prefix)
	var out []Pair
	for k := range l.m {
		if strings.HasPrefix(k, p) {
			key := k[len(space)+1:]
			if v, ok := l.Get(space, key, at); ok {
				out = append(out, Pair{key, v})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (l *KV) encode() []byte {
	e := &enc{}
	e.str("KV1")
	e.u64(uint64(len(l.m)))
	for _, k := range sortedKeys(l.m) {
		e.str(k)
		vs := l.m[k]
		e.u64(uint64(len(vs)))
		for _, v := range vs {
			e.u64(v.LSN)
			e.bytes(v.Val)
			e.str(v.Subject)
			e.bool(v.Del)
		}
	}
	return e.b
}

func (l *KV) Digest() [32]byte { return digest(l.encode()) }

// Snapshot serialises the lens (checkpoint). Contains ciphertext only for
// subject data.
func (l *KV) Snapshot() []byte { return l.encode() }

var ErrBadSnapshot = errors.New("lens: bad snapshot")

func LoadKV(b []byte) (*KV, error) {
	d := &dec{b: b}
	if d.str() != "KV1" {
		return nil, ErrBadSnapshot
	}
	n := d.u64()
	l := NewKV()
	for i := uint64(0); i < n && !d.bad; i++ {
		k := d.str()
		c := d.u64()
		if c > uint64(len(d.b)) {
			return nil, ErrBadSnapshot
		}
		vs := make([]Version, 0, c)
		for j := uint64(0); j < c && !d.bad; j++ {
			vs = append(vs, Version{LSN: d.u64(), Val: d.bytes(), Subject: d.str(), Del: d.bool()})
		}
		l.m[k] = vs
	}
	if d.bad || len(d.b) != 0 {
		return nil, ErrBadSnapshot
	}
	return l, nil
}
