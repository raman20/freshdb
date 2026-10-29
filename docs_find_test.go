package freshdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/raman20/freshdb/store"
	"pgregory.net/rapid"
)

// Indexed and unindexed Find must agree, inside read-write transactions
// too (own writes), and an indexed Find must be part of the read set
// (D-D1, D-D2 in docs/components/documents.md).
func TestDocFindIndexedMatchesScan(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprint("indexed=", indexed), func(t *testing.T) {
			db := mustOpen(t, store.NewMem())
			defer db.Close()
			if indexed {
				if err := db.RegisterIndex(ctx, "c", "s"); err != nil {
					t.Fatal(err)
				}
			}
			put := func(tx *Tx, id, s string) error { return tx.Doc("c").Put(id, map[string]any{"s": s}) }
			if err := db.Update(ctx, func(tx *Tx) error { return put(tx, "a", "x") }); err != nil {
				t.Fatal(err)
			}
			// own writes: a new match, a changed one, a deleted one
			if err := db.Update(ctx, func(tx *Tx) error {
				put(tx, "b", "x")
				tx.Doc("c").Delete("a")
				ids, err := tx.Doc("c").Find("s", "x")
				if err != nil || !slices.Equal(ids, []string{"b"}) {
					t.Fatalf("own writes: %v %v, want [b]", ids, err)
				}
				put(tx, "b", "y")
				if ids, _ := tx.Doc("c").Find("s", "x"); len(ids) != 0 {
					t.Fatalf("after changing b: %v, want []", ids)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			// phantom: a concurrent insert matching the predicate conflicts
			n := 0
			if err := db.Update(ctx, func(tx *Tx) error {
				n++
				ids, _ := tx.Doc("c").Find("s", "z")
				if n == 1 {
					db.Update(context.Background(), func(tx *Tx) error { return put(tx, "z1", "z") })
				}
				if len(ids) == 0 {
					return put(tx, "guard", "none-seen")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if n != 2 {
				t.Fatalf("Find then a concurrent matching insert: %d attempts, want 2 (a conflict)", n)
			}
			// a snapshot does not see a later commit
			if err := db.View(ctx, func(tx *Tx) error {
				done := make(chan error)
				go func() { done <- db.Update(context.Background(), func(tx *Tx) error { return put(tx, "late", "z") }) }()
				if err := <-done; err != nil {
					return err
				}
				ids, _ := tx.Doc("c").Find("s", "z")
				if !slices.Equal(ids, []string{"z1"}) {
					t.Fatalf("snapshot Find: %v, want [z1]", ids)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Property: over random documents, deletes, shreds and in-transaction
// writes, an indexed and an unindexed database answer every Find alike.
func TestDocFindProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		plain := mustOpen(t, store.NewMem())
		defer plain.Close()
		idx := mustOpen(t, store.NewMem())
		defer idx.Close()
		if err := idx.RegisterIndex(ctx, "c", "f"); err != nil {
			rt.Fatal(err)
		}
		vals := []any{"a", "b", 1, 2.5, true, nil, map[string]any{"k": 1}, []any{1, "x"}}
		ids := []string{"d0", "d1", "d2", "d3", "d4"}
		subjects := []string{"", "s1", "s2"}
		for step := rapid.IntRange(1, 25).Draw(rt, "steps"); step > 0; step-- {
			kind := rapid.IntRange(0, 9).Draw(rt, "kind")
			if kind == 9 { // shred a subject on both
				i := rapid.IntRange(1, len(subjects)-1).Draw(rt, "shred")
				s := subjects[i]
				subjects[i] = fmt.Sprintf("%s'", s) // a shredded subject takes no writes
				for _, db := range []*DB{plain, idx} {
					if _, err := db.Shred(ctx, s); err != nil {
						rt.Fatal(err)
					}
				}
				continue
			}
			nops := rapid.IntRange(1, 4).Draw(rt, "nops")
			type op struct {
				id, subj string
				doc      map[string]any
			}
			var ops []op
			for i := 0; i < nops; i++ {
				o := op{id: rapid.SampledFrom(ids).Draw(rt, "id"), subj: rapid.SampledFrom(subjects).Draw(rt, "subj")}
				if rapid.IntRange(0, 4).Draw(rt, "del") > 0 {
					o.doc = map[string]any{}
					if rapid.Bool().Draw(rt, "has") {
						o.doc["f"] = rapid.SampledFrom(vals).Draw(rt, "v")
					}
				}
				ops = append(ops, o)
			}
			q := rapid.SampledFrom(vals).Draw(rt, "q")
			var got [2][]string
			for i, db := range []*DB{plain, idx} {
				err := db.Update(ctx, func(tx *Tx) error {
					for _, o := range ops {
						var err error
						if o.doc == nil {
							err = tx.Doc("c").Delete(o.id)
						} else if o.subj != "" {
							err = tx.Doc("c").Put(o.id, o.doc, Subject(o.subj))
						} else {
							err = tx.Doc("c").Put(o.id, o.doc)
						}
						if err != nil {
							return err
						}
					}
					r, err := tx.Doc("c").Find("f", q)
					got[i] = r
					return err
				})
				if err != nil {
					rt.Fatal(err)
				}
			}
			if !slices.Equal(got[0], got[1]) {
				rt.Fatalf("in tx Find(f,%v): scan %v, index %v", q, got[0], got[1])
			}
			for i, db := range []*DB{plain, idx} {
				db.View(ctx, func(tx *Tx) error { got[i], _ = tx.Doc("c").Find("f", q); return nil })
			}
			if !slices.Equal(got[0], got[1]) {
				rt.Fatalf("committed Find(f,%v): scan %v, index %v", q, got[0], got[1])
			}
		}
	})
}

// Numbers are matched by exact value: a document's 1.0 is found by 1, and
// integers past 2^53 are not rounded together (D-D3).
func TestDocFindNumbers(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		db := mustOpen(t, store.NewMem())
		if indexed {
			if err := db.RegisterIndex(ctx, "c", "n"); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Update(ctx, func(tx *Tx) error {
			tx.Doc("c").Put("one", json.RawMessage(`{"n":1.0}`))
			tx.Doc("c").Put("big", json.RawMessage(`{"n":12345678901234567891}`))
			return tx.Doc("c").Put("big2", json.RawMessage(`{"n":12345678901234567890}`))
		}); err != nil {
			t.Fatal(err)
		}
		db.View(ctx, func(tx *Tx) error {
			for _, c := range []struct {
				q    any
				want []string
			}{{1, []string{"one"}}, {json.Number("1e0"), []string{"one"}}, {json.Number("12345678901234567891"), []string{"big"}}} {
				if ids, err := tx.Doc("c").Find("n", c.q); err != nil || !slices.Equal(ids, c.want) {
					t.Errorf("indexed=%v Find(n,%v) = %v %v, want %v", indexed, c.q, ids, err, c.want)
				}
			}
			return nil
		})
		db.Close()
	}
}

// An index declared after a compaction (state in the base, not in
// segments) and one rebuilt on reopen both see every document.
func TestDocIndexAfterCompactAndReopen(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	for i := 0; i < 20; i++ {
		db.Update(ctx, func(tx *Tx) error {
			return tx.Doc("c").Put(fmt.Sprintf("d%02d", i), map[string]any{"g": i % 3})
		})
	}
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	db.Update(ctx, func(tx *Tx) error { return tx.Doc("c").Delete("d00") })
	if err := db.RegisterIndex(ctx, "c", "g"); err != nil {
		t.Fatal(err)
	}
	want := []string{"d03", "d06", "d09", "d12", "d15", "d18"}
	check := func(db *DB, what string) {
		db.View(ctx, func(tx *Tx) error {
			if ids, err := tx.Doc("c").Find("g", 0); err != nil || !slices.Equal(ids, want) {
				t.Fatalf("%s: %v %v, want %v", what, ids, err, want)
			}
			return nil
		})
	}
	check(db, "after compact + register")
	db.Close()
	db = mustOpen(t, st)
	defer db.Close()
	check(db, "after reopen")
}

// Bad input is refused with ErrBadValue (HTTP 400), and a second
// RegisterIndex of the same index is a no-op.
func TestDocBadValues(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	for i := 0; i < 2; i++ {
		if err := db.RegisterIndex(ctx, "c", "f"); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(db.Manifest().Indexes); n != 1 {
		t.Fatalf("%d index declarations, want 1", n)
	}
	err := db.Update(ctx, func(tx *Tx) error {
		for _, doc := range []any{[]int{1}, "s", 1, nil, json.RawMessage(`[1]`), make(chan int)} {
			if err := tx.Doc("c").Put("x", doc); !errors.Is(err, ErrBadValue) {
				t.Errorf("Put(%#v) = %v, want ErrBadValue", doc, err)
			}
		}
		if _, err := tx.Doc("c").Find("f", make(chan int)); !errors.Is(err, ErrBadValue) {
			t.Errorf("Find(chan) = %v, want ErrBadValue", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
