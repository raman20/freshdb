package lens

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// TestHNSWPersist: a saved graph restored onto the same vectors is
// byte-identical; restored onto a later state (overwrites, deletes, new
// ids, a new dimension) it drops stale nodes, inserts the rest, keeps the
// invariants and the recall; bad blobs and other parameters are refused.
func TestHNSWPersist(t *testing.T) {
	p := ANNParams{M: 8, EfConstruction: 40}
	for seed := int64(0); seed < 6; seed++ {
		r := rand.New(rand.NewSource(seed))
		var ops []func(*Vector)
		for i := 0; i < 2000; i++ {
			id, v := fmt.Sprintf("v%04d", r.Intn(1500)), randVec(r, 16)
			subj := fmt.Sprint("s", r.Intn(30))
			if r.Intn(8) == 0 {
				ops = append(ops, func(l *Vector) { l.del("c", id) })
			} else {
				ops = append(ops, func(l *Vector) { put(l, "c", id, subj, v) })
			}
		}
		a := NewVector()
		a.EnableANN("c", p)
		for _, op := range ops {
			op(a)
		}
		blob := a.MarshalANN()
		snap, err := DecodeANN(blob)
		if err != nil {
			t.Fatal(err)
		}

		same := NewVector()
		for _, op := range ops {
			op(same)
		}
		st := same.EnableANNFrom("c", p, snap)
		if st.Graphs != 1 || st.Stale != 0 || st.Inserted != 0 || st.Reused != len(a.m["c"].slabs[16].ids) {
			t.Fatalf("seed %d: unchanged restore %+v", seed, st)
		}
		if !bytes.Equal(same.MarshalANN(), blob) {
			t.Fatalf("seed %d: restored graph differs", seed)
		}

		later := NewVector()
		for _, op := range ops {
			op(later)
		}
		for i := 0; i < 600; i++ {
			id := fmt.Sprintf("v%04d", r.Intn(2000))
			switch r.Intn(4) {
			case 0:
				later.del("c", id)
			case 1:
				later.DropSubject(fmt.Sprint("s", r.Intn(30)))
			default:
				put(later, "c", id, "x", randVec(r, 16))
			}
		}
		put(later, "c", "other-dim", "x", randVec(r, 4))
		st = later.EnableANNFrom("c", p, snap)
		sl := later.m["c"].slabs[16]
		checkGraph(t, sl)
		checkGraph(t, later.m["c"].slabs[4])
		if err := later.CheckANN(); err != nil {
			t.Fatal(err)
		}
		if st.Graphs != 1 || st.Rebuilt != 1 || st.Stale == 0 || st.Inserted == 0 || st.Reused+st.Inserted != len(sl.ids) {
			t.Fatalf("seed %d: catch-up restore %+v (%d vectors)", seed, st, len(sl.ids))
		}
		var sum float64
		for q := 0; q < 50; q++ {
			u, _ := normalize(randVec(r, 16))
			got, _ := later.ANN("c", u, 10, 200, 0)
			for _, h := range got {
				if _, ok := sl.pos[h.ID]; !ok {
					t.Fatalf("dead id %s", h.ID)
				}
			}
			sum += recall(got, MergeHits([][]Hit{later.TopK("c", u, 10, 0, len(sl.ids))}, 10))
		}
		if sum/50 < 0.95 {
			t.Fatalf("seed %d: recall after catch-up %.3f", seed, sum/50)
		}
		// Then it keeps working as a live graph.
		for i := 0; i < 300; i++ {
			later.del("c", fmt.Sprintf("v%04d", r.Intn(2000)))
			put(later, "c", fmt.Sprintf("n%04d", i), "y", randVec(r, 16))
		}
		checkGraph(t, later.m["c"].slabs[16])

		other := NewVector()
		for _, op := range ops {
			op(other)
		}
		if st := other.EnableANNFrom("c", ANNParams{M: 12}, snap); st.Graphs != 0 || st.Rebuilt != 1 {
			t.Fatalf("other params restored: %+v", st)
		}
	}
	a := NewVector()
	a.EnableANN("c", p)
	r := rand.New(rand.NewSource(9))
	for i := 0; i < 300; i++ {
		put(a, "c", fmt.Sprint(i), "", randVec(r, 8))
	}
	blob := a.MarshalANN()
	for i := 0; i < len(blob); i += 7 {
		b := append([]byte{}, blob...)
		b[i] ^= 0x40
		if _, err := DecodeANN(b); err == nil {
			t.Fatalf("flipped byte %d accepted", i)
		}
	}
	if _, err := DecodeANN(blob[:len(blob)-1]); err == nil {
		t.Fatal("truncated blob accepted")
	}
	if NewVector().MarshalANN() != nil {
		t.Fatal("blob without graphs")
	}
}

// Identical vectors share a node; the copies are not saved but rejoin
// their node by hash on restore, so a restore of the same state rebuilds
// the same graph, and deleting representatives keeps every copy findable.
func TestHNSWDuplicates(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	dirs := make([][]float32, 12)
	for i := range dirs {
		dirs[i] = randVec(r, 8)
	}
	var ops []func(*Vector)
	for i := 0; i < 3000; i++ {
		id, v := fmt.Sprintf("v%04d", r.Intn(1200)), dirs[r.Intn(len(dirs))]
		if r.Intn(4) == 0 {
			ops = append(ops, func(l *Vector) { l.del("c", id) })
		} else {
			ops = append(ops, func(l *Vector) { put(l, "c", id, "s", v) })
		}
	}
	p := ANNParams{M: 4, EfConstruction: 16}
	a := NewVector()
	a.EnableANN("c", p)
	for _, op := range ops {
		op(a)
	}
	sl := a.m["c"].slabs[8]
	checkGraph(t, sl)
	if len(sl.g.dups) == 0 {
		t.Fatal("no shared nodes")
	}
	for _, d := range dirs {
		u, _ := normalize(d)
		got, _ := a.ANN("c", u, 50, 0, 0)
		want := MergeHits([][]Hit{a.TopK("c", u, 50, 0, len(sl.ids))}, 50)
		if rc := recall(MergeHits([][]Hit{got}, 50), want); rc != 1 {
			t.Fatalf("recall %.2f on a duplicated direction", rc)
		}
	}
	snap, err := DecodeANN(a.MarshalANN())
	if err != nil {
		t.Fatal(err)
	}
	b := NewVector()
	for _, op := range ops {
		op(b)
	}
	st := b.EnableANNFrom("c", p, snap)
	checkGraph(t, b.m["c"].slabs[8])
	if st.Inserted != 0 || st.Stale != 0 || st.Reused+st.Copies != len(sl.ids) || st.Copies == 0 {
		t.Fatalf("restore %+v of %d vectors", st, len(sl.ids))
	}
	if !reflect.DeepEqual(b.m["c"].slabs[8].g.dups, sl.g.dups) || !bytes.Equal(b.MarshalANN(), a.MarshalANN()) {
		t.Fatal("restored graph differs")
	}
	// Delete every representative: the copies take over.
	for n, s := range sl.g.slot {
		if s >= 0 && sl.g.dups[int32(n)] != nil {
			a.del("c", sl.ids[s])
		}
	}
	checkGraph(t, sl)
	for _, d := range dirs {
		u, _ := normalize(d)
		got, _ := a.ANN("c", u, 50, 0, 0)
		want := MergeHits([][]Hit{a.TopK("c", u, 50, 0, len(sl.ids))}, 50)
		if rc := recall(MergeHits([][]Hit{got}, 50), want); rc != 1 {
			t.Fatalf("recall %.2f after deleting representatives", rc)
		}
	}
}

// CheckANN must notice each kind of broken copy bookkeeping.
func TestCheckANNCatches(t *testing.T) {
	build := func() (*Vector, *hnsw) {
		l := NewVector()
		l.EnableANN("c", ANNParams{M: 4})
		for i := 0; i < 60; i++ {
			put(l, "c", itoa(i), "", []float32{float32(i%3) + 1, 1})
		}
		return l, l.m["c"].slabs[2].g
	}
	if l, _ := build(); l.CheckANN() != nil {
		t.Fatal(l.CheckANN())
	}
	breaks := map[string]func(g *hnsw){
		"copy forgotten": func(g *hnsw) {
			for n, ds := range g.dups {
				g.dups[n] = ds[1:]
				return
			}
		},
		"copies unsorted": func(g *hnsw) {
			for _, ds := range g.dups {
				ds[0], ds[1] = ds[1], ds[0]
				return
			}
		},
		"copy listed twice": func(g *hnsw) {
			for n, ds := range g.dups {
				g.dups[n] = append(ds, ds[len(ds)-1])
				return
			}
		},
		"copy differs": func(g *hnsw) {
			for _, ds := range g.dups {
				p := g.sl.pos[ds[0]]
				g.sl.data[p*2] += 1
				return
			}
		},
		"empty copy list": func(g *hnsw) { g.dups[g.entry] = []string{} },
		"self link":       func(g *hnsw) { g.links[g.entry][0] = append(g.links[g.entry][0], g.entry) },
		"dead node with copies": func(g *hnsw) {
			g.slot = append(g.slot, -1)
			g.links = append(g.links, nil)
			g.dups[int32(len(g.slot)-1)] = []string{"x"}
		},
	}
	for name, f := range breaks {
		l, g := build()
		f(g)
		if l.CheckANN() == nil {
			t.Errorf("%s: not caught", name)
		}
	}
}
