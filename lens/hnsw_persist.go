package lens

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"sort"
)

// Persisted HNSW graphs (docs/design/10-hnsw-persist.md). A graph is a
// cache over the vectors, so a saved one is only a starting point: on load
// every node is checked against the vector the lens now holds for its id
// (by a hash of the unit vector), stale nodes are removed, and vectors the
// graph does not cover are inserted. No vector components are stored.

const annMagic = "FHNSW1\x00\x00"

// ANNVersion is the HNSW snapshot format (a cache: unknown ones are rebuilt).
const ANNVersion = "1"

var errANNFormat = errors.New("lens: bad ANN snapshot")

// ANNSaveStats / ANNLoadStats describe a save or a restore.
type ANNLoadStats struct {
	Graphs   int // graphs restored from the snapshot
	Rebuilt  int // graphs built from scratch (no usable snapshot)
	Reused   int // nodes kept as saved
	Stale    int // saved nodes removed (vector deleted or changed)
	Inserted int // vectors inserted after the restore
}

func vecHash(u []float32) uint64 {
	h := uint64(14695981039346656037)
	for _, x := range u {
		b := math.Float32bits(x)
		for i := 0; i < 4; i++ {
			h = (h ^ uint64(byte(b>>(8*i)))) * 1099511628211
		}
	}
	return h
}

// MarshalANN encodes every HNSW graph of the lens (sorted by collection
// and dimension), or nil if there is none.
func (l *Vector) MarshalANN() []byte {
	var b bytes.Buffer
	n := 0
	for _, coll := range sortedKeys(l.m) {
		c := l.m[coll]
		dims := make([]int, 0, len(c.slabs))
		for d := range c.slabs {
			dims = append(dims, d)
		}
		sort.Ints(dims)
		for _, d := range dims {
			if g := c.slabs[d].g; g != nil {
				g.marshal(&b, coll)
				n++
			}
		}
	}
	if n == 0 {
		return nil
	}
	out := make([]byte, 0, b.Len()+16)
	out = append(out, annMagic...)
	out = binary.LittleEndian.AppendUint32(out, uint32(n))
	out = append(out, b.Bytes()...)
	return binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(out))
}

func putStr(b *bytes.Buffer, s string) {
	var t [binary.MaxVarintLen64]byte
	b.Write(t[:binary.PutUvarint(t[:], uint64(len(s)))])
	b.WriteString(s)
}

func putU(b *bytes.Buffer, x uint64) {
	var t [binary.MaxVarintLen64]byte
	b.Write(t[:binary.PutUvarint(t[:], x)])
}

func (g *hnsw) marshal(b *bytes.Buffer, coll string) {
	putStr(b, coll)
	for _, x := range []int{g.sl.dim, g.p.M, g.p.EfConstruction, g.p.EfSearch, len(g.slot), g.top} {
		putU(b, uint64(x))
	}
	putU(b, uint64(g.entry+1))
	var w [8]byte
	for n, s := range g.slot {
		if s < 0 {
			putStr(b, "") // deleted: no id, no hash
			continue
		}
		putStr(b, g.sl.ids[s])
		binary.LittleEndian.PutUint64(w[:], vecHash(g.vec(int32(n))))
		b.Write(w[:])
		putU(b, uint64(len(g.links[n])))
		for _, ls := range g.links[n] {
			k := 0
			for _, x := range ls {
				if g.slot[x] >= 0 {
					k++
				}
			}
			putU(b, uint64(k)) // links to deleted nodes are not saved
			for _, x := range ls {
				if g.slot[x] >= 0 {
					binary.LittleEndian.PutUint32(w[:4], uint32(x))
					b.Write(w[:4])
				}
			}
		}
	}
}

type annSnap struct {
	p     ANNParams
	top   int
	entry int32
	ids   []string // node -> id ("" = deleted)
	hash  []uint64
	links [][][]int32
}

// ANNSnapshot is a decoded MarshalANN blob: graphs by collection and
// dimension.
type ANNSnapshot map[string]map[int]*annSnap

type rd struct {
	b   []byte
	err error
}

func (r *rd) u() uint64 {
	x, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errANNFormat
		r.b = nil
		return 0
	}
	r.b = r.b[n:]
	return x
}

func (r *rd) str() string {
	n := r.u()
	if uint64(len(r.b)) < n {
		r.err, r.b = errANNFormat, nil
		return ""
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s
}

func (r *rd) fixed(n int) []byte {
	if len(r.b) < n {
		r.err, r.b = errANNFormat, nil
		return make([]byte, n)
	}
	x := r.b[:n]
	r.b = r.b[n:]
	return x
}

// DecodeANN parses a MarshalANN blob.
func DecodeANN(b []byte) (ANNSnapshot, error) {
	if len(b) < len(annMagic)+8 || string(b[:len(annMagic)]) != annMagic {
		return nil, errANNFormat
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return nil, errANNFormat
	}
	r := &rd{b: body[len(annMagic)+4:]}
	out := ANNSnapshot{}
	for k := binary.LittleEndian.Uint32(body[len(annMagic):]); k > 0 && r.err == nil; k-- {
		coll := r.str()
		var v [6]int
		for i := range v {
			v[i] = int(r.u())
		}
		s := &annSnap{p: ANNParams{M: v[1], EfConstruction: v[2], EfSearch: v[3]}, top: v[5], entry: int32(r.u()) - 1}
		dim, nodes := v[0], v[4]
		if nodes > len(r.b) || s.top > maxLevel {
			return nil, errANNFormat
		}
		s.ids, s.hash, s.links = make([]string, nodes), make([]uint64, nodes), make([][][]int32, nodes)
		for n := 0; n < nodes && r.err == nil; n++ {
			if s.ids[n] = r.str(); s.ids[n] == "" {
				continue
			}
			s.hash[n] = binary.LittleEndian.Uint64(r.fixed(8))
			lv := int(r.u())
			if lv > maxLevel+1 {
				return nil, errANNFormat
			}
			s.links[n] = make([][]int32, lv)
			for l := 0; l < lv && r.err == nil; l++ {
				c := int(r.u())
				raw := r.fixed(4 * c)
				ls := make([]int32, c)
				for i := range ls {
					x := int32(binary.LittleEndian.Uint32(raw[4*i:]))
					if x < 0 || int(x) >= nodes {
						return nil, errANNFormat
					}
					ls[i] = x
				}
				s.links[n][l] = ls
			}
		}
		if s.entry < -1 || int(s.entry) >= nodes || (s.entry >= 0 && s.ids[s.entry] == "") {
			return nil, errANNFormat
		}
		for _, lv := range s.links { // every link must land on a node with that level
			for l, ls := range lv {
				for _, x := range ls {
					if s.ids[x] != "" && len(s.links[x]) <= l { // links to deleted nodes are swept on restore
						return nil, errANNFormat
					}
				}
			}
		}
		if out[coll] == nil {
			out[coll] = map[int]*annSnap{}
		}
		out[coll][dim] = s
	}
	if r.err != nil || len(r.b) != 0 {
		return nil, errANNFormat
	}
	return out, nil
}

// EnableANNFrom is EnableANN that starts from saved graphs where they fit
// (same parameters), then catches up with the current vectors.
func (l *Vector) EnableANNFrom(coll string, p ANNParams, snap ANNSnapshot) ANNLoadStats {
	var st ANNLoadStats
	if l.ann == nil {
		l.ann = map[string]ANNParams{}
	}
	if old, ok := l.ann[coll]; ok && old == p {
		return st
	}
	l.ann[coll] = p
	c := l.m[coll]
	if c == nil {
		return st
	}
	for d, sl := range c.slabs {
		// M and EfConstruction shape the graph; EfSearch only affects
		// queries, so a changed EfSearch (or its default) keeps the graph.
		if s := snap[coll][d]; s != nil && s.p.M == p.norm().M && s.p.EfConstruction == p.norm().EfConstruction {
			s.p.EfSearch = p.norm().EfSearch
			sl.g = restoreHNSW(sl, s, &st)
			st.Graphs++
		} else {
			sl.g = newHNSW(sl, p)
			st.Rebuilt++
		}
	}
	return st
}

func restoreHNSW(sl *slab, s *annSnap, st *ANNLoadStats) *hnsw {
	g := &hnsw{p: s.p, sl: sl, entry: s.entry, top: s.top, links: s.links}
	n := len(s.ids)
	g.slot = make([]int32, n)
	sl.node = make([]int32, len(sl.ids))
	for i := range sl.node {
		sl.node[i] = -1
	}
	var stale []int32
	for i, id := range s.ids {
		g.slot[i] = -1
		if id == "" {
			continue
		}
		p, ok := sl.pos[id]
		if ok && sl.node[p] < 0 && len(s.links[i]) == g.levelOf(id)+1 &&
			vecHash(sl.data[p*sl.dim:(p+1)*sl.dim]) == s.hash[i] {
			g.slot[i], sl.node[p] = int32(p), int32(i)
			st.Reused++
		} else {
			stale = append(stale, int32(i))
		}
	}
	for _, x := range stale {
		if g.links[x] != nil {
			g.remove(x) // repairs its live neighbors
		}
		st.Stale++
	}
	g.sweep()
	if g.entry < 0 || g.slot[g.entry] < 0 || len(g.links[g.entry])-1 != g.top {
		g.pickEntry()
	}
	for p := range sl.ids {
		if sl.node[p] < 0 {
			g.insert(p)
			st.Inserted++
		}
	}
	return g
}

// pickEntry chooses the live node with the most levels as the entry.
func (g *hnsw) pickEntry() {
	g.entry, g.top = -1, 0
	for i, s := range g.slot {
		if s >= 0 && (g.entry < 0 || len(g.links[i])-1 > g.top) {
			g.entry, g.top = int32(i), len(g.links[i])-1
		}
	}
}

// CheckANN verifies every graph's structure against its slab: each vector
// has exactly one live node and back, levels follow the id, links stay
// within bounds and levels, and the entry is a live top-level node.
func (l *Vector) CheckANN() error {
	for _, coll := range sortedKeys(l.m) {
		for d, sl := range l.m[coll].slabs {
			g := sl.g
			if g == nil {
				continue
			}
			bad := func(msg string, xs ...int) error {
				for _, x := range xs {
					msg += " " + itoa(x)
				}
				return errors.New("lens: ann " + coll + "/" + itoa(d) + ": " + msg)
			}
			if len(sl.node) != len(sl.ids) {
				return bad("node map, slots:", len(sl.node), len(sl.ids))
			}
			live := 0
			for n, s := range g.slot {
				if s < 0 {
					if g.links[n] != nil {
						return bad("deleted node keeps links:", n)
					}
					continue
				}
				live++
				if int(s) >= len(sl.ids) || sl.node[s] != int32(n) {
					return bad("node -> slot does not map back:", n, int(s))
				}
				if len(g.links[n]) != g.levelOf(sl.ids[s])+1 {
					return bad("node, levels:", n, len(g.links[n]))
				}
				for lv, ls := range g.links[n] {
					if len(ls) > g.maxM(lv) {
						return bad("node, level, links:", n, lv, len(ls))
					}
					for _, x := range ls {
						if x < 0 || int(x) >= len(g.slot) || x == int32(n) {
							return bad("node, bad link:", n, int(x))
						}
						if g.slot[x] >= 0 && len(g.links[x]) <= lv {
							return bad("link above its level:", n, int(x))
						}
					}
				}
			}
			if live != len(sl.ids) {
				return bad("live nodes, vectors:", live, len(sl.ids))
			}
			if live > 0 && (g.entry < 0 || g.slot[g.entry] < 0 || len(g.links[g.entry])-1 != g.top) {
				return bad("entry, top not a live top node:", int(g.entry), g.top)
			}
		}
	}
	return nil
}

func itoa(x int) string {
	if x < 0 {
		return "-" + itoa(-x)
	}
	if x < 10 {
		return string(rune('0' + x))
	}
	return itoa(x/10) + string(rune('0'+x%10))
}
