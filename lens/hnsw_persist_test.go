package lens

import (
	"bytes"
	"fmt"
	"math/rand"
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
