package lens

import (
	"hash/crc32"
	"sort"
	"strings"

	"github.com/raman20/freshdb/format"
)

// mapKV is the KV lens as it was before runs (a map of version lists),
// kept as the model the run-based lens must match exactly.
type mapKV struct {
	m      map[string][]Version
	inline int
}

func newMapKV(inline int) *mapKV { return &mapKV{m: map[string][]Version{}, inline: inline} }

func (l *mapKV) Apply(e *Entry, env Env) {
	for i, op := range e.Ops {
		switch op.Kind {
		case format.OpPut:
			v := Version{LSN: e.LSN, Op: uint32(i), Subject: op.Subject}
			if op.ValOff != 0 && len(op.Value) > l.inline {
				v.Off, v.Len, v.Sum = op.ValOff, uint32(len(op.Value)), crc32.Checksum(op.Value, Castagnoli)
			} else {
				v.Val = op.Value
			}
			if op.Subject != "" && env.Shredded(op.Subject) {
				redact(&v)
			}
			k := ck(op.Space, op.Key)
			l.m[k] = append(l.m[k], v)
		case format.OpDel:
			k := ck(op.Space, op.Key)
			l.m[k] = append(l.m[k], Version{LSN: e.LSN, Op: uint32(i), Del: true})
		case format.OpShred:
			l.DropSubject(op.Key)
		}
	}
}

func (l *mapKV) DropSubject(s string) {
	for _, vs := range l.m {
		for i := range vs {
			if vs[i].Subject == s {
				redact(&vs[i])
			}
		}
	}
}

func (l *mapKV) Get(space, key string, at uint64) (Version, bool) {
	vs := l.m[ck(space, key)]
	i := sort.Search(len(vs), func(i int) bool { return vs[i].LSN > at }) - 1
	if i < 0 || vs[i].Del {
		return Version{}, false
	}
	return vs[i], true
}

func (l *mapKV) LastWrite(space, key string) uint64 {
	if vs := l.m[ck(space, key)]; len(vs) > 0 {
		return vs[len(vs)-1].LSN
	}
	return 0
}

func (l *mapKV) Scan(space, prefix string, at uint64) []Pair {
	var out []Pair
	for k := range l.m {
		if strings.HasPrefix(k, ck(space, prefix)) {
			if v, ok := l.Get(space, k[len(space)+1:], at); ok {
				out = append(out, Pair{k[len(space)+1:], v})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (l *mapKV) SetLatest(space, key string, v Version) { l.m[ck(space, key)] = []Version{v} }

func (l *mapKV) encode() []byte {
	v3 := false
	for _, vs := range l.m {
		for _, v := range vs {
			v3 = v3 || v.Chunk != 0
		}
	}
	e := &enc{}
	if v3 {
		e.str("KV3")
	} else {
		e.str("KV2")
	}
	e.u64(uint64(l.inline))
	e.u64(uint64(len(l.m)))
	for _, k := range sortedKeys(l.m) {
		e.str(k)
		e.u64(uint64(len(l.m[k])))
		for _, v := range l.m[k] {
			e.u64(v.LSN)
			e.u64(uint64(v.Off)<<32 | uint64(v.Len))
			e.u64(uint64(v.Op)<<32 | uint64(v.Sum))
			if v3 {
				e.u64(uint64(v.Chunk))
			}
			e.bytes(v.Val)
			e.str(v.Subject)
			e.bool(v.Del)
		}
	}
	return e.b
}
