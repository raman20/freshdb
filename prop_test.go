package freshdb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/raman20/freshdb/store"
	"pgregory.net/rapid"
)

// For random programs over the public API: every branch's lens state equals
// a from-scratch fold of its log, and reopening (replay from snapshot+log)
// reproduces bit-identical lens state.
func TestPropEngineLensEqualsFoldAcrossRestart(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		st := store.NewMem()
		seed := rapid.Int64().Draw(t, "seed")
		clk := int64(0)
		opts := []Option{quiet(), WithRand(rand.New(rand.NewSource(seed))), WithClock(func() int64 { clk++; return clk }),
			WithSnapshotEvery(rapid.IntRange(1, 10).Draw(t, "snapEvery"))}
		db, err := OpenStore(ctx, st, opts...)
		if err != nil {
			t.Fatal(err)
		}
		db.RegisterIndex(ctx, "d", "f")
		branches := []string{"main"}
		n := rapid.IntRange(1, 40).Draw(t, "n")
		for i := 0; i < n; i++ {
			br := rapid.SampledFrom(branches).Draw(t, "branch")
			subj := rapid.SampledFrom([]string{"", "s1", "s2"}).Draw(t, "subj")
			var o []WriteOpt
			if subj != "" {
				o = append(o, Subject(subj))
			}
			switch rapid.IntRange(0, 9).Draw(t, "op") {
			case 0, 1, 2:
				db.Branch(br).Update(ctx, func(tx *Tx) error {
					return tx.Put(fmt.Sprint("k", rapid.IntRange(0, 3).Draw(t, "k")), []byte(fmt.Sprint(i)), o...)
				})
			case 3:
				db.Branch(br).Update(ctx, func(tx *Tx) error { return tx.Topic("t").Publish([]byte(fmt.Sprint(i)), o...) })
			case 4:
				db.Branch(br).Update(ctx, func(tx *Tx) error {
					return tx.Vector("v").Put(fmt.Sprint("x", rapid.IntRange(0, 3).Draw(t, "vk")), []float32{float32(i % 3), 1}, o...)
				})
			case 5:
				db.Branch(br).Update(ctx, func(tx *Tx) error {
					return tx.Doc("d").Put(fmt.Sprint("d", rapid.IntRange(0, 3).Draw(t, "dk")), map[string]any{"f": i % 2}, o...)
				})
			case 6:
				db.Branch(br).Update(ctx, func(tx *Tx) error {
					tx.Vector("v").Delete(fmt.Sprint("x", i%4))
					return tx.Doc("d").Delete(fmt.Sprint("d", i%4))
				})
			case 7:
				name := fmt.Sprint("b", i)
				if db.CreateBranch(ctx, name, br, 0) == nil {
					branches = append(branches, name)
				}
			case 8:
				h, _ := db.Branch(br).Head()
				if h > 0 {
					db.Rewind(ctx, br, uint64(rapid.IntRange(0, int(h)).Draw(t, "at")))
				}
			case 9:
				if subj != "" {
					db.Shred(ctx, subj)
				}
			}
		}
		before := map[string]string{}
		for _, br := range branches {
			d1, _, err := db.LensDigests(br)
			if err != nil {
				t.Fatal(err)
			}
			d2, _ := db.RebuildDigests(br)
			if fmt.Sprint(d1) != fmt.Sprint(d2) {
				t.Fatalf("branch %s: lens != fold(log)", br)
			}
			before[br] = fmt.Sprint(d1)
		}
		db.Close()
		db, err = OpenStore(ctx, st, opts...)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, br := range branches {
			d, _, _ := db.LensDigests(br)
			if fmt.Sprint(d) != before[br] {
				t.Fatalf("branch %s: state differs after restart", br)
			}
		}
		if r, _ := db.Verify(ctx); !r.OK {
			t.Fatalf("%+v", r)
		}
	})
}
