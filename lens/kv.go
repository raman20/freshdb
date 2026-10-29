package lens

import (
	"bytes"
	"encoding/binary"
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
//
// Recent versions live in a map (the hot set). When it holds hotMax
// versions it is frozen into an immutable compressed run (kvrun.go), and
// adjacent runs of similar size are merged a few keys per write. Every
// version of a key in a run is older than its versions in newer runs and
// in the hot set, so reads look newest first. The serialised form (and so
// Digest) is the same as when everything was one map.
type KV struct {
	m         map[string]vers // hot set: space \x00 key -> versions ascending by LSN
	hot       int             // versions in m
	keys      []string        // keys of m, sorted, except those in recent
	recent    []string        // keys added to m since keys was last merged
	hotMax    int
	recentMax int
	runs      []*run    // newest first
	mgs       []*merger // merges in progress, of disjoint adjacent runs
	inline    int       // values longer than this are kept as positions
}

const (
	hotMax     = 1 << 16 // versions in the hot set before it is frozen
	mergeRatio = 3       // merge runs[i+1] into runs[i] when at most this many times its size
	mergeStep  = 16      // keys merged per op applied
	recentMax  = 1024    // new hot keys before they are sorted into keys
	maxRuns    = 24      // beyond this a merge in progress completes at once
)

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
	return &KV{m: map[string]vers{}, inline: max, hotMax: hotMax, recentMax: recentMax}
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
	l.work(len(e.Ops) * mergeStep)
}

// put appends a version; a key's first version gets a 1-element slice
// (most keys never get a second one).
func (l *KV) put(k string, v Version) {
	vs, ok := l.m[k]
	if !ok {
		l.m[k] = vers{one: pack(v)}
		if l.recent = append(l.recent, k); len(l.recent) >= l.recentMax {
			l.keys = mergeSorted(l.keys, l.recent)
			l.recent = l.recent[:0]
		}
	} else {
		vs.more = append(vs.more, pack(v))
		l.m[k] = vs
	}
	if l.hot++; l.hot >= l.hotMax {
		l.freeze()
	}
}

// freeze turns the hot set into the newest run.
func (l *KV) freeze() {
	w := newBuilder()
	w.grow(len(l.m), 48*l.hot)
	l.hotRun(w)
	l.m, l.hot, l.keys, l.recent = map[string]vers{}, 0, nil, nil
	if r := w.finish(); r != nil {
		l.runs = append([]*run{r}, l.runs...)
	}
	l.plan()
	for len(l.runs) > maxRuns && len(l.mgs) > 0 {
		l.work(-1)
	}
}

// merging reports whether r is an input of a merge in progress.
func (l *KV) merging(r *run) bool {
	for _, m := range l.mgs {
		for _, x := range m.in {
			if x == r {
				return true
			}
		}
	}
	return false
}

// mergeSorted merges the unsorted keys add into sorted keys. add is
// sorted in place.
func mergeSorted(keys, add []string) []string {
	sort.Strings(add)
	out := make([]string, 0, len(keys)+len(add))
	i, j := 0, 0
	for i < len(keys) && j < len(add) {
		if keys[i] < add[j] {
			out, i = append(out, keys[i]), i+1
		} else {
			out, j = append(out, add[j]), j+1
		}
	}
	out = append(out, keys[i:]...)
	return append(out, add[j:]...)
}

// hotKeys returns the hot set's keys in order without changing l (it runs
// under a read lock).
func (l *KV) hotKeys() []string {
	if len(l.recent) == 0 {
		return l.keys
	}
	return mergeSorted(l.keys, append([]string(nil), l.recent...))
}

// hotRun writes the hot set to w in key order.
func (l *KV) hotRun(w *builder) {
	var kb []byte
	for _, k := range l.hotKeys() {
		vs := l.m[k]
		for i := 0; i < vs.n(); i++ {
			v := vs.at(i).unpack()
			w.ver(&v)
		}
		kb = append(kb[:0], k...)
		w.end(kb)
	}
}

// plan starts merging every adjacent pair of similar size not already
// being merged (sizes then grow at least mergeRatio-fold from newest to
// oldest, so there are O(log n) runs).
func (l *KV) plan() {
	for i := 0; i+1 < len(l.runs); i++ {
		a, b := l.runs[i], l.runs[i+1]
		if b.n <= mergeRatio*a.n && len(a.b)+len(b.b) <= maxRunBytes && !l.merging(a) && !l.merging(b) {
			l.mgs = append(l.mgs, newMerger(l.runs[i:i+2:i+2]))
			i++
		}
	}
}

// work advances each merge in progress by n keys (n < 0: to the end) and
// installs those that are done.
func (l *KV) work(n int) {
	if len(l.mgs) == 0 {
		return
	}
	if n < 0 {
		n = int(^uint(0) >> 1)
	}
	done := false
	for k := 0; k < len(l.mgs); k++ {
		m := l.mgs[k]
		if !m.step(n) {
			continue
		}
		l.mgs = append(l.mgs[:k:k], l.mgs[k+1:]...)
		k--
		done = true
		r := m.result()
		for i := range l.runs {
			if l.runs[i] == m.in[0] { // inputs stay adjacent: runs are only added in front or replaced
				nr := append([]*run{}, l.runs[:i]...)
				if r != nil {
					nr = append(nr, r)
				}
				l.runs = append(nr, l.runs[i+len(m.in):]...)
				break
			}
		}
	}
	if done {
		l.plan()
	}
}

// replace swaps run i for r (nil: removes it); a merge that reads it is
// abandoned (plan starts it again).
func (l *KV) replace(i int, r *run) {
	for k, m := range l.mgs {
		for _, x := range m.in {
			if x == l.runs[i] {
				l.mgs = append(l.mgs[:k:k], l.mgs[k+1:]...)
				break
			}
		}
	}
	if r != nil {
		l.runs[i] = r
	} else {
		l.runs = append(l.runs[:i:i], l.runs[i+1:]...)
	}
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
	// Only runs holding unredacted versions of s are rewritten.
	for i := len(l.runs) - 1; i >= 0; i-- {
		if l.runs[i].subj[s] == 0 {
			continue
		}
		l.replace(i, l.runs[i].rebuild(func(_ []byte, vs []Version) []Version {
			for j := range vs {
				if vs[j].Subject == s {
					redact(&vs[j])
				}
			}
			return vs
		}))
	}
	l.plan()
}

// find returns the index of the version visible at `at`, or -1.
func (vs *vers) find(at uint64) int {
	return sort.Search(vs.n(), func(i int) bool { return vs.at(i).lsn > at }) - 1
}

// Get returns the version of (space,key) visible at LSN `at`.
func (l *KV) Get(space, key string, at uint64) (Version, bool) {
	k := ck(space, key)
	if vs, ok := l.m[k]; ok {
		if i := vs.find(at); i >= 0 {
			if vs.at(i).del {
				return Version{}, false
			}
			return vs.at(i).unpack(), true
		}
	}
	if len(l.runs) == 0 {
		return Version{}, false
	}
	h := khash(k)
	var first [maxRuns + 1]uint64
	pre := len(l.runs) <= len(first)
	if pre {
		for i, r := range l.runs {
			first[i] = r.slots[r.slot(h)]
		}
	}
	for i, r := range l.runs {
		j := r.slot(h)
		s := r.slots[j]
		if pre {
			s = first[i]
		}
		if body, ok := r.lookupAt(k, h, j, s); ok {
			if v, ok := bodyAt(body, at); ok {
				if v.Del {
					return Version{}, false
				}
				return v, true
			}
		}
	}
	return Version{}, false
}

// LastWrite returns the LSN of the latest write to (space,key), 0 if none.
func (l *KV) LastWrite(space, key string) uint64 {
	k := ck(space, key)
	if vs, ok := l.m[k]; ok {
		return vs.at(vs.n() - 1).lsn
	}
	h := khash(k)
	for _, r := range l.runs {
		if body, ok := r.lookup(k, h); ok {
			return bodyLast(body).LSN
		}
	}
	return 0
}

type Pair struct {
	Key string
	Version
}

// Scan returns live keys in space with prefix at LSN `at`, sorted.
func (l *KV) Scan(space, prefix string, at uint64) []Pair {
	p := ck(space, prefix)
	var out []Pair
	done := map[string]bool{} // keys whose visible version was found
	hot := func(k string) {
		vs := l.m[k]
		if i := vs.find(at); i >= 0 {
			done[k] = true
			if w := vs.at(i); !w.del {
				out = append(out, Pair{k[len(space)+1:], w.unpack()})
			}
		}
	}
	for i := sort.SearchStrings(l.keys, p); i < len(l.keys) && strings.HasPrefix(l.keys[i], p); i++ {
		hot(l.keys[i])
	}
	for _, k := range l.recent {
		if strings.HasPrefix(k, p) {
			hot(k)
		}
	}
	pb := []byte(p)
	for _, r := range l.runs {
		c := r.cursor()
		c.seek(pb)
		for c.next() && bytes.HasPrefix(c.key, pb) {
			if done[string(c.key)] {
				continue
			}
			if v, ok := bodyAt(c.body, at); ok {
				k := string(c.key)
				done[k] = true
				if !v.Del {
					out = append(out, Pair{k[len(space)+1:], v})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// each calls fn for every key in key order with all its versions, oldest
// first (vs is reused between calls).
func (l *KV) each(fn func(k []byte, vs []Version) error) error {
	srcs := l.runs
	if len(l.m) > 0 {
		w := newBuilder()
		l.hotRun(w)
		srcs = append([]*run{w.finish()}, srcs...)
	}
	m := newMerger(srcs)
	var key []byte
	var vs []Version
	for {
		have := false
		for _, c := range m.cur {
			if c != nil && (!have || bytes.Compare(c.key, key) < 0) {
				key, have = append(key[:0], c.key...), true
			}
		}
		if !have {
			return nil
		}
		vs = vs[:0]
		for i := len(m.cur) - 1; i >= 0; i-- {
			if c := m.cur[i]; c != nil && bytes.Equal(c.key, key) {
				vs = bodyVers(vs, c.body)
				if !c.next() {
					m.cur[i] = nil
				}
			}
		}
		if err := fn(key, vs); err != nil {
			return err
		}
	}
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
	for _, r := range l.runs {
		v3 = v3 || r.chunk
	}
	e := &enc{}
	if v3 {
		e.str("KV3")
	} else {
		e.str("KV2")
	}
	e.u64(uint64(l.inline))
	at := len(e.b)
	e.u64(0) // key count, patched below
	n := uint64(0)
	_ = l.each(func(k []byte, vs []Version) error {
		n++
		e.u64(uint64(len(k)))
		e.b = append(e.b, k...)
		e.u64(uint64(len(vs)))
		for _, v := range vs {
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
		return nil
	})
	binary.LittleEndian.PutUint64(e.b[at:], n)
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
	w := newBuilder()
	chunked, prev := false, ""
	for i := uint64(0); i < n && !d.bad; i++ {
		k := d.str()
		// Keys are written sorted and unique as space+"\x00"+key. A key
		// out of order would silently replace another, and one without
		// the separator panics Latest.
		if (i > 0 && k <= prev) || strings.IndexByte(k, 0) < 0 {
			return nil, ErrBadSnapshot
		}
		prev = k
		c := d.u64()
		if c > uint64(len(d.b)) {
			return nil, ErrBadSnapshot
		}
		if c == 0 {
			return nil, ErrBadSnapshot // every key has at least one version
		}
		last, lastOp := uint64(0), uint32(0)
		for j := uint64(0); j < c && !d.bad; j++ {
			v := Version{LSN: d.u64()}
			ol, os := d.u64(), d.u64()
			v.Off, v.Len, v.Op, v.Sum = uint32(ol>>32), uint32(ol), uint32(os>>32), uint32(os)
			if f == "KV3" {
				ch := d.u64()
				if ch > math.MaxUint32 {
					return nil, ErrBadSnapshot
				}
				v.Chunk, chunked = uint32(ch), chunked || ch != 0
			}
			v.Val, v.Subject, v.Del = d.bytes(), d.str(), d.bool()
			// Versions are written in (LSN, op) order (one entry may write
			// a key twice); reads rely on it.
			if j > 0 && (v.LSN < last || v.LSN == last && v.Op <= lastOp) {
				return nil, ErrBadSnapshot
			}
			last, lastOp = v.LSN, v.Op
			w.ver(&v)
		}
		w.end([]byte(k))
		if len(w.r.b) >= maxRunBytes { // keys are sorted: runs of disjoint ranges
			l.runs = append(l.runs, w.finish())
			w = newBuilder()
		}
	}
	// KV3 is written only when some version carries a base chunk.
	if d.bad || len(d.b) != 0 || (f == "KV3") != chunked {
		return nil, ErrBadSnapshot
	}
	if r := w.finish(); r != nil {
		l.runs = append(l.runs, r)
	}
	return l, nil
}

// Inline returns the lens' inline-value limit.
func (l *KV) Inline() int { return l.inline }

// Latest calls fn for every key, in key order, with the version visible
// at the newest LSN (deleted and redacted keys included; fn checks Del).
func (l *KV) Latest(fn func(space, key string, v Version) error) error {
	return l.each(func(k []byte, vs []Version) error {
		i := bytes.IndexByte(k, 0)
		return fn(string(k[:i]), string(k[i+1:]), vs[len(vs)-1])
	})
}

// SetLatest makes v the only version of (space,key). For building
// compaction bases, which call it once per key on a fresh lens.
func (l *KV) SetLatest(space, key string, v Version) {
	k := ck(space, key)
	h := khash(k)
	for i := len(l.runs) - 1; i >= 0; i-- {
		if _, ok := l.runs[i].lookup(k, h); ok {
			l.replace(i, l.runs[i].rebuild(func(key []byte, vs []Version) []Version {
				if string(key) == k {
					return nil
				}
				return vs
			}))
		}
	}
	if vs, ok := l.m[k]; ok { // replace in place: k stays in keys/recent
		l.hot -= vs.n() - 1
		l.m[k] = vers{one: pack(v)}
		if l.hot >= l.hotMax {
			l.freeze()
		}
	} else {
		l.put(k, v)
	}
	l.plan()
	l.work(mergeStep)
}
