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
//
// Unit vectors of one collection and dimension live in one contiguous
// float32 slab (deletes swap the last slot in), so a search is a linear
// scan with no per-vector allocation. The slot order depends on history,
// but no result does: hits are totally ordered by (score desc, id asc).
type Vector struct {
	m   map[string]*vecColl  // coll -> vectors
	ann map[string]ANNParams // collections with an HNSW graph (opt-in)
}

type vecColl struct {
	dim   map[string]int // id -> dimension (which slab holds it)
	slabs map[int]*slab  // dimension -> slab
}

type slab struct {
	dim  int
	ids  []string
	subj []string
	data []float32 // len(ids)*dim unit-vector components
	pos  map[string]int
	g    *hnsw   // HNSW graph, if the collection opted in
	node []int32 // slot -> graph node (with g)
}

func NewVector() *Vector { return &Vector{m: map[string]*vecColl{}} }

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

// Normalize returns q scaled to unit length (ok=false for a zero vector).
func Normalize(q []float32) ([]float32, bool) { return normalize(q) }

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
			l.add(coll, op.Key, op.Subject, u)
		}
	}
}

func (l *Vector) add(coll, id, subject string, u []float32) {
	c := l.m[coll]
	if c == nil {
		c = &vecColl{dim: map[string]int{}, slabs: map[int]*slab{}}
		l.m[coll] = c
	}
	sl := c.slabs[len(u)]
	if sl == nil {
		sl = &slab{dim: len(u), pos: map[string]int{}}
		c.slabs[len(u)] = sl
	}
	c.dim[id] = len(u)
	sl.pos[id] = len(sl.ids)
	sl.ids = append(sl.ids, id)
	sl.subj = append(sl.subj, subject)
	sl.data = append(sl.data, u...)
	if p, ok := l.ann[coll]; ok {
		if sl.g == nil {
			sl.g = newHNSW(sl, p)
		} else {
			sl.node = append(sl.node, -1)
			sl.g.insert(len(sl.ids) - 1)
		}
	}
}

func (l *Vector) DropSubject(s string) {
	for _, coll := range sortedKeys(l.m) {
		c := l.m[coll]
		var gone []string
		for _, sl := range c.slabs {
			for i, subj := range sl.subj {
				if subj == s {
					gone = append(gone, sl.ids[i])
				}
			}
		}
		for _, id := range gone {
			l.del(coll, id) // drops empty slabs and collections: canonical form
		}
	}
}

func (l *Vector) del(coll, id string) {
	c := l.m[coll]
	if c == nil {
		return
	}
	d, ok := c.dim[id]
	if !ok {
		return
	}
	delete(c.dim, id)
	sl := c.slabs[d]
	i, last := sl.pos[id], len(sl.ids)-1
	var gone int32 = -1
	if sl.g != nil {
		gone, _ = sl.g.dropSlot(i)
	}
	if i != last { // move the last slot into the hole
		sl.ids[i], sl.subj[i] = sl.ids[last], sl.subj[last]
		copy(sl.data[i*d:(i+1)*d], sl.data[last*d:])
		sl.pos[sl.ids[i]] = i
		if sl.g != nil {
			m := sl.node[last]
			sl.node[i] = m
			if sl.g.slot[m] == int32(last) { // it represented its node
				sl.g.slot[m] = int32(i)
			}
		}
	}
	delete(sl.pos, id)
	sl.ids, sl.subj, sl.data = sl.ids[:last], sl.subj[:last], sl.data[:last*d]
	clear(sl.data[last*d : (last+1)*d]) // no copy of a deleted vector stays behind
	sl.subj[:last+1][last] = ""
	if sl.g != nil {
		sl.node = sl.node[:last]
		if gone >= 0 {
			sl.g.remove(gone)
		}
	}
	if last == 0 {
		delete(c.slabs, d)
	}
	if len(c.dim) == 0 {
		delete(l.m, coll)
	}
}

type Hit struct {
	ID    string
	Score float32
}

// worse reports whether a ranks below b: lower score, then larger id.
func worse(a, b Hit) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}
	return a.ID > b.ID
}

// SortHits orders hits best first.
func SortHits(h []Hit) { sort.Slice(h, func(i, j int) bool { return worse(h[j], h[i]) }) }

// Slots returns how many vectors of dimension dim a collection holds; slot
// ranges [lo,hi) of that many may be searched independently (TopK) and the
// partial results merged (MergeHits).
func (l *Vector) Slots(coll string, dim int) int {
	if c := l.m[coll]; c != nil {
		if sl := c.slabs[dim]; sl != nil {
			return len(sl.ids)
		}
	}
	return 0
}

// TopK returns the best k hits (unordered) among slots [lo,hi) for the unit
// query u. It allocates only the k-element result.
func (l *Vector) TopK(coll string, u []float32, k, lo, hi int) []Hit {
	c := l.m[coll]
	if c == nil || k <= 0 {
		return nil
	}
	sl := c.slabs[len(u)]
	if sl == nil {
		return nil
	}
	hi = min(hi, len(sl.ids))
	d := sl.dim
	h := make([]Hit, 0, min(k, max(hi-lo, 0)))
	for i := lo; i < hi; i++ {
		s := dot(u, sl.data[i*d:(i+1)*d:(i+1)*d])
		if len(h) == k {
			if s < h[0].Score || (s == h[0].Score && sl.ids[i] > h[0].ID) {
				continue
			}
			h[0] = Hit{sl.ids[i], s}
			siftDown(h, 0)
			continue
		}
		h = append(h, Hit{sl.ids[i], s})
		siftUp(h, len(h)-1)
	}
	return h
}

// Dot is the similarity Search ranks by: the dot product of two unit
// vectors of one dimension, computed exactly as the lens computes it.
func Dot(a, b []float32) float32 { return dot(a, b) }

// dot is an 8-wide, 4-accumulator unrolled float32 dot product (independent accumulators
// let the CPU overlap the multiply-adds). len(b) must equal len(a).
func dot(a, b []float32) float32 {
	b = b[:len(a)]
	var s0, s1, s2, s3 float32
	for len(a) >= 8 && len(b) >= 8 { // reslicing lets the compiler drop bounds checks
		s0 += a[0]*b[0] + a[4]*b[4]
		s1 += a[1]*b[1] + a[5]*b[5]
		s2 += a[2]*b[2] + a[6]*b[6]
		s3 += a[3]*b[3] + a[7]*b[7]
		a, b = a[8:], b[8:]
	}
	for i := range a {
		s0 += a[i] * b[i]
	}
	return (s0 + s1) + (s2 + s3)
}

// min-heap by rank: h[0] is the worst hit kept.
func siftUp(h []Hit, i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !worse(h[i], h[p]) {
			return
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}

func siftDown(h []Hit, i int) {
	for {
		l, m := 2*i+1, i
		if l < len(h) && worse(h[l], h[m]) {
			m = l
		}
		if r := l + 1; r < len(h) && worse(h[r], h[m]) {
			m = r
		}
		if m == i {
			return
		}
		h[i], h[m] = h[m], h[i]
		i = m
	}
}

// MergeHits merges partial TopK results into the final best-first top k.
func MergeHits(parts [][]Hit, k int) []Hit {
	var all []Hit
	for _, p := range parts {
		all = append(all, p...)
	}
	SortHits(all)
	if len(all) > k {
		all = all[:k]
	}
	return all
}

// EnableANN gives a collection an HNSW graph (built now from its current
// vectors, then maintained on every apply). Exact search stays available.
func (l *Vector) EnableANN(coll string, p ANNParams) {
	if l.ann == nil {
		l.ann = map[string]ANNParams{}
	}
	if old, ok := l.ann[coll]; ok && old == p {
		return
	}
	l.ann[coll] = p
	if c := l.m[coll]; c != nil {
		for _, sl := range c.slabs { // independent graphs: order is irrelevant
			sl.g = newHNSW(sl, p)
		}
	}
}

// ANN returns up to k approximate hits (unordered) for the unit query u
// from the collection's HNSW graph with beam width ef (0 = the
// collection's EfSearch), and false if the collection has no graph for
// this dimension or holds fewer than minN vectors.
func (l *Vector) ANN(coll string, u []float32, k, ef, minN int) ([]Hit, bool) {
	c := l.m[coll]
	if c == nil || k <= 0 {
		return nil, false
	}
	sl := c.slabs[len(u)]
	if sl == nil || sl.g == nil || len(sl.ids) < minN {
		return nil, false
	}
	if ef <= 0 {
		ef = sl.g.p.EfSearch
	}
	k = min(k, len(sl.ids))
	return sl.g.knn(u, k, min(ef, len(sl.ids))), true
}

// Search returns the top-k ids by cosine similarity (ties broken by id).
func (l *Vector) Search(coll string, q []float32, k int) []Hit {
	u, ok := normalize(q)
	if !ok || k <= 0 {
		return nil
	}
	return MergeHits([][]Hit{l.TopK(coll, u, k, 0, l.Slots(coll, len(u)))}, k)
}

func (l *Vector) Count(coll string) int {
	if c := l.m[coll]; c != nil {
		return len(c.dim)
	}
	return 0
}

func (l *Vector) Digest() [32]byte {
	e := &enc{}
	e.str("VEC1")
	for _, coll := range sortedKeys(l.m) {
		e.str(coll)
		c := l.m[coll]
		for _, id := range sortedKeys(c.dim) {
			sl := c.slabs[c.dim[id]]
			i := sl.pos[id]
			e.str(id)
			e.str(sl.subj[i])
			e.bytes(EncodeVec(sl.data[i*sl.dim : (i+1)*sl.dim]))
		}
	}
	return digest(e.b)
}
