package lens

import (
	"math"
	"sort"
)

// ANNParams configures an HNSW graph for one vector collection.
type ANNParams struct {
	M              int // links per node above layer 0 (2M at layer 0); default 16
	EfConstruction int // beam width while inserting; default 100
	EfSearch       int // default beam width while searching; default 64
}

func (p ANNParams) norm() ANNParams {
	if p.M < 2 {
		p.M = 16
	}
	if p.EfConstruction < p.M {
		p.EfConstruction = max(100, p.M)
	}
	if p.EfSearch <= 0 {
		p.EfSearch = 64
	}
	return p
}

// hnsw is a hierarchical navigable small-world graph over the slots of one
// slab (Malkov & Yashunin). It holds no vectors: node n's vector is the
// slab slot slot[n], kept in sync by the slab's swap-delete. Levels come
// from a hash of the id, and nodes are inserted in log order, so the same
// history builds the same graph. It is a cache: not part of the digest.
type hnsw struct {
	p     ANNParams
	sl    *slab
	slot  []int32     // node -> slab slot; -1 = deleted
	links [][][]int32 // node -> level -> neighbor nodes
	free  []int32     // deleted nodes with no remaining references
	dead  int         // deleted nodes still possibly referenced
	entry int32
	top   int
}

// visited is a small open-addressing set of node ids, one per search (a
// search touches about ef·M nodes, far fewer than the graph holds).
type visited struct {
	t []int32 // -1 = empty
	n int
}

func newVisited(hint int) *visited {
	c := 64
	for c < 2*hint {
		c *= 2
	}
	t := make([]int32, c)
	for i := range t {
		t[i] = -1
	}
	return &visited{t: t}
}

// add inserts x and reports whether it was absent.
func (v *visited) add(x int32) bool {
	if 2*(v.n+1) > len(v.t) {
		old := v.t
		v.t, v.n = make([]int32, 2*len(old)), 0
		for i := range v.t {
			v.t[i] = -1
		}
		for _, y := range old {
			if y >= 0 {
				v.add(y)
			}
		}
	}
	m := uint32(len(v.t) - 1)
	for i := uint32(x) * 2654435761 & m; ; i = (i + 1) & m {
		switch v.t[i] {
		case x:
			return false
		case -1:
			v.t[i] = x
			v.n++
			return true
		}
	}
}

func newHNSW(sl *slab, p ANNParams) *hnsw {
	g := &hnsw{p: p.norm(), sl: sl, entry: -1}
	sl.node = make([]int32, 0, len(sl.ids))
	for i := range sl.ids {
		sl.node = append(sl.node, -1)
		g.insert(i)
	}
	return g
}

const maxLevel = 16

func (g *hnsw) levelOf(id string) int {
	h := uint64(14695981039346656037) // FNV-1a
	for i := 0; i < len(id); i++ {
		h = (h ^ uint64(id[i])) * 1099511628211
	}
	h ^= h >> 29 // mix the high bits we use
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 32
	r := (float64(h>>11) + 1) / (1 << 53) // (0,1]
	l := int(-math.Log(r) / math.Log(float64(g.p.M)))
	return min(l, maxLevel)
}

func (g *hnsw) vec(n int32) []float32 {
	d := g.sl.dim
	s := int(g.slot[n])
	return g.sl.data[s*d : (s+1)*d : (s+1)*d]
}

func (g *hnsw) maxM(l int) int {
	if l == 0 {
		return 2 * g.p.M
	}
	return g.p.M
}

// insert adds slab slot s (already appended) to the graph.
func (g *hnsw) insert(s int) {
	var n int32
	if k := len(g.free); k > 0 {
		n, g.free = g.free[k-1], g.free[:k-1]
	} else {
		n = int32(len(g.slot))
		g.slot = append(g.slot, 0)
		g.links = append(g.links, nil)
	}
	g.slot[n] = int32(s)
	g.sl.node[s] = n
	lv := g.levelOf(g.sl.ids[s])
	g.links[n] = make([][]int32, lv+1)
	if g.entry < 0 {
		g.entry, g.top = n, lv
		return
	}
	q := g.vec(n)
	ep := g.entry
	for l := g.top; l > lv; l-- {
		ep = g.greedy(q, ep, l)
	}
	eps := []int32{ep}
	for l := min(lv, g.top); l >= 0; l-- {
		w := g.search(q, eps, g.p.EfConstruction, l)
		g.links[n][l] = g.selectNb(q, w, g.maxM(l))
		for _, nb := range g.links[n][l] {
			g.link(nb, n, l)
		}
		eps = eps[:0]
		for _, c := range w {
			eps = append(eps, c.n)
		}
	}
	if lv > g.top {
		g.entry, g.top = n, lv
	}
}

// link adds n to nb's level-l links, re-selecting if it overflows.
func (g *hnsw) link(nb, n int32, l int) {
	ls := append(g.links[nb][l], n)
	if len(ls) > g.maxM(l) {
		v := g.vec(nb)
		c := make([]cand, 0, len(ls))
		for _, x := range ls {
			if g.slot[x] >= 0 { // drop dangling links to deleted nodes
				c = append(c, cand{x, dot(v, g.vec(x))})
			}
		}
		// same diversity heuristic as for new nodes: keeping only the
		// closest drops the long links between clusters and strands them
		sortCands(c)
		ls = g.selectNb(v, c, g.maxM(l))
	}
	g.links[nb][l] = ls
}

type cand struct {
	n int32
	s float32
}

func sortCands(c []cand) {
	sort.Slice(c, func(i, j int) bool {
		if c[i].s != c[j].s {
			return c[i].s > c[j].s
		}
		return c[i].n < c[j].n
	})
}

// selectNb is the paper's heuristic: keep a candidate only if it is closer
// to the base than to every kept one (no top-up with pruned candidates, as
// in hnswlib). c is sorted best first.
func (g *hnsw) selectNb(base []float32, c []cand, m int) []int32 {
	out := make([]int32, 0, m)
	for _, x := range c {
		if len(out) == m {
			break
		}
		xv, ok := g.vec(x.n), true
		for _, r := range out {
			if dot(xv, g.vec(r)) > x.s {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, x.n)
		}
	}
	return out
}

func (g *hnsw) greedy(q []float32, ep int32, l int) int32 {
	best := dot(q, g.vec(ep))
	for changed := true; changed; {
		changed = false
		for _, nb := range g.links[ep][l] {
			if g.slot[nb] < 0 {
				continue
			}
			if s := dot(q, g.vec(nb)); s > best {
				ep, best, changed = nb, s, true
			}
		}
	}
	return ep
}

// search is the beam search at layer l; it returns up to ef nodes, best
// first. It only reads the graph, so concurrent searches are safe.
func (g *hnsw) search(q []float32, eps []int32, ef, l int) []cand {
	vis := newVisited(ef * g.p.M)
	var front candHeap // max-heap: best candidate first
	var res candHeap   // min-heap of size ef: worst result first
	for _, e := range eps {
		vis.add(e)
		c := cand{e, dot(q, g.vec(e))}
		front.push(c, true)
		res.push(c, false)
	}
	for len(front) > 0 {
		c := front.pop(true)
		if len(res) >= ef && c.s < res[0].s {
			break
		}
		for _, nb := range g.links[c.n][l] {
			if g.slot[nb] < 0 || !vis.add(nb) {
				continue
			}
			s := dot(q, g.vec(nb))
			if len(res) < ef || s > res[0].s {
				front.push(cand{nb, s}, true)
				res.push(cand{nb, s}, false)
				if len(res) > ef {
					res.pop(false)
				}
			}
		}
	}
	out := []cand(res)
	sortCands(out)
	return out
}

// remove unlinks node n (its slot is already gone from the slab) and
// repairs each of its neighbors from the union of both link lists.
func (g *hnsw) remove(n int32) {
	g.slot[n] = -1
	for l, ls := range g.links[n] {
		for _, nb := range ls {
			if g.slot[nb] < 0 {
				continue
			}
			v := g.vec(nb)
			seen := map[int32]bool{nb: true, n: true}
			var c []cand
			for _, x := range append(append([]int32{}, g.links[nb][l]...), ls...) {
				if !seen[x] && g.slot[x] >= 0 {
					seen[x] = true
					c = append(c, cand{x, dot(v, g.vec(x))})
				}
			}
			sortCands(c)
			g.links[nb][l] = g.selectNb(v, c, g.maxM(l))
		}
	}
	g.links[n] = nil
	g.dead++
	if n == g.entry {
		g.entry, g.top = -1, 0
		for i, s := range g.slot {
			if s >= 0 && (g.entry < 0 || len(g.links[i])-1 > g.top) {
				g.entry, g.top = int32(i), len(g.links[i])-1
			}
		}
	}
	if g.dead > 64 && g.dead*4 > len(g.slot) {
		g.sweep()
	}
}

// sweep drops references to deleted nodes from every link list (other
// nodes may still point at them) and makes them reusable.
func (g *hnsw) sweep() {
	for n := range g.links {
		for l, ls := range g.links[n] {
			k := 0
			for _, x := range ls {
				if g.slot[x] >= 0 {
					ls[k] = x
					k++
				}
			}
			g.links[n][l] = ls[:k]
		}
	}
	g.free = g.free[:0] // rebuilt: earlier free nodes are dead too
	for n, s := range g.slot {
		if s < 0 {
			g.free = append(g.free, int32(n))
		}
	}
	g.dead = 0
}

// knn returns up to k hits for the unit query u with beam width ef.
func (g *hnsw) knn(u []float32, k, ef int) []Hit {
	if g.entry < 0 {
		return nil
	}
	ep := g.entry
	for l := g.top; l > 0; l-- {
		ep = g.greedy(u, ep, l)
	}
	w := g.search(u, []int32{ep}, max(ef, k), 0)
	h := make([]Hit, 0, min(k, len(w)))
	for _, c := range w {
		h = append(h, Hit{g.sl.ids[g.slot[c.n]], c.s})
	}
	return h
}

type candHeap []cand

func (h *candHeap) less(i, j int, maxh bool) bool {
	a, b := (*h)[i], (*h)[j]
	if maxh {
		return a.s > b.s || (a.s == b.s && a.n < b.n)
	}
	return a.s < b.s || (a.s == b.s && a.n > b.n)
}

func (h *candHeap) push(c cand, maxh bool) {
	*h = append(*h, c)
	for i := len(*h) - 1; i > 0; {
		p := (i - 1) / 2
		if !h.less(i, p, maxh) {
			break
		}
		(*h)[i], (*h)[p] = (*h)[p], (*h)[i]
		i = p
	}
}

func (h *candHeap) pop(maxh bool) cand {
	old := *h
	top := old[0]
	last := len(old) - 1
	old[0] = old[last]
	*h = old[:last]
	for i := 0; ; {
		l, r, b := 2*i+1, 2*i+2, i
		if l < last && h.less(l, b, maxh) {
			b = l
		}
		if r < last && h.less(r, b, maxh) {
			b = r
		}
		if b == i {
			break
		}
		(*h)[i], (*h)[b] = (*h)[b], (*h)[i]
		i = b
	}
	return top
}
