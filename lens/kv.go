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
	Chunk   uint32 // != 0: the value is in value chunk Chunk-1 of the log's compaction base, at Off
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
	m      map[string]vers // space \x00 key -> versions ascending by LSN
	inline int             // values longer than this are kept as positions
}

// ver is the in-memory form of a Version: 40 bytes instead of 72. The
// rarely set Subject and inline Val live behind one pointer, so a 1 KiB
// value kept as a log position costs no extra allocation.
type ver struct {
	lsn                      uint64
	off, len, op, sum, chunk uint32
	del                      bool
	x                        *verX // nil unless Subject or Val is set
}

type verX struct {
	subject string
	val     []byte
}

func pack(v Version) ver {
	w := ver{lsn: v.LSN, off: v.Off, len: v.Len, op: v.Op, sum: v.Sum, chunk: v.Chunk, del: v.Del}
	if v.Subject != "" || v.Val != nil {
		w.x = &verX{v.Subject, v.Val}
	}
	return w
}

func (w ver) unpack() Version {
	v := Version{LSN: w.lsn, Off: w.off, Len: w.len, Op: w.op, Sum: w.sum, Chunk: w.chunk, Del: w.del}
	if w.x != nil {
		v.Subject, v.Val = w.x.subject, w.x.val
	}
	return v
}

// vers is a key's version list: the first version inline (most keys have
// only one, so they need no slice allocation) and later ones in more.
type vers struct {
	one  ver
	more []ver
}

func (vs *vers) n() int { return 1 + len(vs.more) }

func (vs *vers) at(i int) *ver {
	if i == 0 {
		return &vs.one
	}
	return &vs.more[i-1]
}

// NewKV returns a KV lens that keeps every value inline.
func NewKV() *KV { return NewKVInline(math.MaxInt32) }

// NewKVInline returns a KV lens that keeps values of at most max bytes
// inline and larger ones (whose position in the log is known) as positions.
func NewKVInline(max int) *KV {
	if max < 0 {
		max = 0
	}
	return &KV{m: map[string]vers{}, inline: max}
}

func redact(v *Version) { v.Val, v.Del, v.Off, v.Len, v.Sum, v.Chunk = nil, true, 0, 0, 0, 0 }

// redactVer is redact on the packed form (the subject is kept).
func redactVer(w *ver) {
	v := w.unpack()
	redact(&v)
	*w = pack(v)
}

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
		l.m[k] = vers{one: pack(v)}
		return
	}
	vs.more = append(vs.more, pack(v))
	l.m[k] = vs
}

func (l *KV) DropSubject(s string) {
	for k, vs := range l.m {
		changed := false
		for i := 0; i < vs.n(); i++ {
			if w := vs.at(i); w.x != nil && w.x.subject == s {
				redactVer(w)
				changed = true
			}
		}
		if changed {
			l.m[k] = vs // vs.one is a copy; more shares its array
		}
	}
}

// find returns the index of the version visible at `at`, or -1.
func (vs *vers) find(at uint64) int {
	return sort.Search(vs.n(), func(i int) bool { return vs.at(i).lsn > at }) - 1
}

// Get returns the version of (space,key) visible at LSN `at`.
func (l *KV) Get(space, key string, at uint64) (Version, bool) {
	vs, ok := l.m[ck(space, key)]
	if !ok {
		return Version{}, false
	}
	i := vs.find(at)
	if i < 0 || vs.at(i).del {
		return Version{}, false
	}
	return vs.at(i).unpack(), true
}

// LastWrite returns the LSN of the latest write to (space,key), 0 if none.
func (l *KV) LastWrite(space, key string) uint64 {
	vs, ok := l.m[ck(space, key)]
	if !ok {
		return 0
	}
	return vs.at(vs.n() - 1).lsn
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
	// KV3 adds a chunk number per version; it is written only when some
	// version lives in a compaction base, so KV2 bytes (and digests) of
	// uncompacted state are unchanged.
	v3 := false
	for _, vs := range l.m {
		for i := 0; i < vs.n(); i++ {
			v3 = v3 || vs.at(i).chunk != 0
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
		vs := l.m[k]
		e.u64(uint64(vs.n()))
		for i := 0; i < vs.n(); i++ {
			v := vs.at(i).unpack()
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

func (l *KV) Digest() [32]byte { return digest(l.encode()) }

// Snapshot serialises the lens (checkpoint). Contains ciphertext only for
// subject data.
func (l *KV) Snapshot() []byte { return l.encode() }

var ErrBadSnapshot = errors.New("lens: bad snapshot")

// LoadKV loads a snapshot. A snapshot written with a different inline
// limit (or by v0.1.0, format KV1) is rejected; the caller then replays the
// log instead, so the result is always fold(log).
func LoadKV(b []byte, inline int) (*KV, error) { return loadKV(b, inline, true) }

// LoadKVBase loads a compaction base image. Unlike a snapshot (a cache
// that is rebuilt when it does not match), a base is the only copy of its
// state, so it loads under any inline limit: the limit only decides how
// values committed later are held.
func LoadKVBase(b []byte, inline int) (*KV, error) { return loadKV(b, inline, false) }

func loadKV(b []byte, inline int, strict bool) (*KV, error) {
	d := &dec{b: b}
	f := d.str()
	if f != "KV2" && f != "KV3" {
		return nil, ErrBadSnapshot
	}
	l := NewKVInline(inline)
	if d.u64() != uint64(l.inline) && strict {
		return nil, ErrBadSnapshot
	}
	n := d.u64()
	for i := uint64(0); i < n && !d.bad; i++ {
		k := d.str()
		c := d.u64()
		if c > uint64(len(d.b)) {
			return nil, ErrBadSnapshot
		}
		if c == 0 {
			return nil, ErrBadSnapshot // every key has at least one version
		}
		var vs vers
		for j := uint64(0); j < c && !d.bad; j++ {
			v := Version{LSN: d.u64()}
			ol, os := d.u64(), d.u64()
			v.Off, v.Len, v.Op, v.Sum = uint32(ol>>32), uint32(ol), uint32(os>>32), uint32(os)
			if f == "KV3" {
				v.Chunk = uint32(d.u64())
			}
			v.Val, v.Subject, v.Del = d.bytes(), d.str(), d.bool()
			if j == 0 {
				vs.one = pack(v)
			} else {
				if vs.more == nil {
					vs.more = make([]ver, 0, c-1)
				}
				vs.more = append(vs.more, pack(v))
			}
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

// Latest calls fn for every key, in key order, with the version visible
// at the newest LSN (deleted and redacted keys included; fn checks Del).
func (l *KV) Latest(fn func(space, key string, v Version) error) error {
	for _, k := range sortedKeys(l.m) {
		vs := l.m[k]
		i := strings.IndexByte(k, 0)
		if err := fn(k[:i], k[i+1:], vs.at(vs.n()-1).unpack()); err != nil {
			return err
		}
	}
	return nil
}

// SetLatest makes v the only version of (space,key). For building
// compaction bases.
func (l *KV) SetLatest(space, key string, v Version) { l.m[ck(space, key)] = vers{one: pack(v)} }
