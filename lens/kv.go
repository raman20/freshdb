package lens

import (
	"errors"
	"hash/crc32"
	"math"
	"sort"
	"strings"

	"github.com/raman20/freshdb/format"
)

// Version is one MVCC version of a key. The stored bytes are either inline
// (Val) or, for values larger than the lens' inline limit, a position in
// the log: Len bytes at offset Off of the segment holding LSN, with CRC-32C
// Sum. Positions are part of the log (segments are immutable), so the lens
// is still a pure fold of it; the bytes are read by the caller, not here.
type Version struct {
	LSN     uint64
	Off     uint32 // 0 = value is inline in Val
	Len     uint32
	Op      uint32 // index of the op in its frame (AEAD associated data)
	Sum     uint32 // CRC-32C of the stored bytes when Off != 0
	Del     bool   // delete, or redacted (shredded) put
	Subject string
	Val     []byte // inline stored bytes (ciphertext if Subject != "")
}

// Ref reports whether the value lives in the log rather than in Val.
func (v Version) Ref() bool { return v.Off != 0 }

// Castagnoli is the CRC table used for value checksums.
var Castagnoli = crc32.MakeTable(crc32.Castagnoli)

// KV is the key-value lens: a multi-version map over every PUT/DEL in the
// log, in all spaces (kv, doc:*, vec:*, __cursor). It folds the raw log
// (values stay ciphertext for subject data), so it can be snapshotted
// without leaking plaintext; decryption happens at read time.
type KV struct {
	m      map[string][]Version // space \x00 key -> versions ascending by LSN
	inline int                  // values longer than this are kept as positions
}

// NewKV returns a KV lens that keeps every value inline.
func NewKV() *KV { return NewKVInline(math.MaxInt32) }

// NewKVInline returns a KV lens that keeps values of at most max bytes
// inline and larger ones (whose position in the log is known) as positions.
func NewKVInline(max int) *KV {
	if max < 0 {
		max = 0
	}
	return &KV{m: map[string][]Version{}, inline: max}
}

func redact(v *Version) { v.Val, v.Del, v.Off, v.Len, v.Sum = nil, true, 0, 0, 0 }

func (*KV) Name() string { return "kv" }

func ck(space, key string) string { return space + "\x00" + key }

func (l *KV) Apply(e *Entry, env Env) {
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
			l.put(ck(op.Space, op.Key), v)
		case format.OpDel:
			l.put(ck(op.Space, op.Key), Version{LSN: e.LSN, Op: uint32(i), Del: true})
		case format.OpShred:
			l.DropSubject(op.Key)
		}
	}
}

// put appends a version; a key's first version gets a 1-element slice
// (most keys never get a second one).
func (l *KV) put(k string, v Version) {
	vs, ok := l.m[k]
	if !ok {
		l.m[k] = []Version{v}
		return
	}
	l.m[k] = append(vs, v)
}

func (l *KV) DropSubject(s string) {
	for _, vs := range l.m {
		for i := range vs {
			if vs[i].Subject == s {
				redact(&vs[i])
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
	e.str("KV2")
	e.u64(uint64(l.inline))
	e.u64(uint64(len(l.m)))
	for _, k := range sortedKeys(l.m) {
		e.str(k)
		vs := l.m[k]
		e.u64(uint64(len(vs)))
		for _, v := range vs {
			e.u64(v.LSN)
			e.u64(uint64(v.Off)<<32 | uint64(v.Len))
			e.u64(uint64(v.Op)<<32 | uint64(v.Sum))
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

// LoadKV loads a snapshot. A snapshot written with a different inline
// limit (or by v0.1.0, format KV1) is rejected; the caller then replays the
// log instead, so the result is always fold(log).
func LoadKV(b []byte, inline int) (*KV, error) {
	d := &dec{b: b}
	if d.str() != "KV2" {
		return nil, ErrBadSnapshot
	}
	l := NewKVInline(inline)
	if d.u64() != uint64(l.inline) {
		return nil, ErrBadSnapshot
	}
	n := d.u64()
	for i := uint64(0); i < n && !d.bad; i++ {
		k := d.str()
		c := d.u64()
		if c > uint64(len(d.b)) {
			return nil, ErrBadSnapshot
		}
		vs := make([]Version, 0, c)
		for j := uint64(0); j < c && !d.bad; j++ {
			v := Version{LSN: d.u64()}
			ol, os := d.u64(), d.u64()
			v.Off, v.Len, v.Op, v.Sum = uint32(ol>>32), uint32(ol), uint32(os>>32), uint32(os)
			v.Val, v.Subject, v.Del = d.bytes(), d.str(), d.bool()
			vs = append(vs, v)
		}
		l.m[k] = vs
	}
	if d.bad || len(d.b) != 0 {
		return nil, ErrBadSnapshot
	}
	return l, nil
}

// Inline returns the lens' inline-value limit.
func (l *KV) Inline() int { return l.inline }
