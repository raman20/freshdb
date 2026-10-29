package freshdb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/raman20/freshdb/store"
)

// TestRotAtlas is the latent-rot half of the storage fault model: after a
// workload, every live object in turn is damaged for good (bit flips in the
// header and the body, truncation, another object's bytes, deletion), and
// the store is opened without RepairTail. The oracle: ErrCorrupt (at open
// or on a read) or exactly the right data, never a wrong answer. The atlas
// (go test -run TestRotAtlas -v) shows which outcome each object class and
// fault gives; docs/components/log.md records it.
func TestRotAtlas(t *testing.T) {
	src, want := rotWorkload(t)
	keys := slices.Sorted(maps.Keys(rawObjects(src)))
	newest := map[string]bool{} // the last key of each directory
	for i, k := range keys {
		if i+1 == len(keys) || path.Dir(keys[i+1]) != path.Dir(k) {
			newest[k] = true
		}
	}
	atlas := map[string]map[string]int{} // "class fault" -> outcome -> count
	for _, key := range keys {
		for _, fault := range []string{"flip-head", "flip-body", "truncate", "swap", "transplant", "delete"} {
			st := cloneMem(src)
			if !rotObject(st, key, fault) {
				continue
			}
			out := rotOutcome(t, st, want)
			if strings.HasPrefix(out, "WRONG") {
				if fault == "delete" && newest[key] {
					// Losing the newest object of a log, or the newest
					// manifest, looks exactly like that write never
					// happening: undetectable by construction. The store
					// must not lose acknowledged objects (log.md).
					out = "lost-newest (undetectable)"
				} else {
					t.Errorf("%s %s: %s", key, fault, out)
				}
			}
			cell := rotClass(key) + " " + fault
			if atlas[cell] == nil {
				atlas[cell] = map[string]int{}
			}
			atlas[cell][out]++
		}
	}
	for _, cell := range slices.Sorted(maps.Keys(atlas)) {
		t.Logf("%-28s %v", cell, atlas[cell])
	}
}

// rotWorkload builds a store with several segments, snapshots, a branch
// and a deletion, and returns main's expected key/value state.
func rotWorkload(t *testing.T) (*store.Mem, map[string]map[string]string) {
	st := store.NewMem()
	db := mustOpen(t, st, WithSnapshotEvery(4))
	want := map[string]string{}
	for i := 0; i < 12; i++ {
		k, v := fmt.Sprintf("k%d", i%7), fmt.Sprintf("v%d", i)
		if err := db.Put(ctx, k, []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	if err := db.Update(ctx, func(tx *Tx) error { return tx.Delete("k3") }); err != nil {
		t.Fatal(err)
	}
	delete(want, "k3")
	if err := db.CreateBranch(ctx, "b", "main", 0); err != nil {
		t.Fatal(err)
	}
	onB := maps.Clone(want)
	for i := 0; i < 9; i++ {
		k, v := fmt.Sprintf("k%d", i%7), fmt.Sprintf("b%d", i)
		if err := db.Branch("b").Update(ctx, func(tx *Tx) error { return tx.Put(k, []byte(v)) }); err != nil {
			t.Fatal(err)
		}
		onB[k] = v
	}
	db.Close()
	return st, map[string]map[string]string{"main": want, "b": onB}
}

func rawObjects(st *store.Mem) map[string][]byte {
	var out map[string][]byte
	st.Raw(func(m map[string][]byte) { out = maps.Clone(m) })
	return out
}

func cloneMem(src *store.Mem) *store.Mem {
	st := store.NewMem()
	objs := rawObjects(src)
	st.Raw(func(m map[string][]byte) {
		for k, v := range objs {
			m[k] = slices.Clone(v)
		}
	})
	return st
}

var rotDigits = regexp.MustCompile(`[0-9]+`)

// rotClass names an object's kind: its key with numbers elided.
func rotClass(key string) string { return rotDigits.ReplaceAllString(key, "N") }

// rotObject damages key in place; false if the fault does not apply.
func rotObject(st *store.Mem, key, fault string) bool {
	applied := true
	st.Raw(func(m map[string][]byte) {
		b := m[key]
		switch fault {
		case "flip-head":
			applied = len(b) > 0
			if applied {
				b[0] ^= 0x10
			}
		case "flip-body":
			applied = len(b) > 40
			if applied {
				b[len(b)-len(b)/3] ^= 0x01
			}
		case "truncate":
			applied = len(b) > 1
			m[key] = b[:len(b)/2]
		case "swap": // another object of the same directory
			applied = false
			applied = swapFrom(m, key, func(k string) bool { return path.Dir(k) == path.Dir(key) })
		case "transplant": // an object of the same kind from another directory (log)
			applied = swapFrom(m, key, func(k string) bool { return path.Dir(k) != path.Dir(key) })
		case "delete":
			delete(m, key)
		}
	})
	return applied
}

// swapFrom overwrites key with the bytes of the first (sorted) other object
// of the same extension that where accepts and that differs from it.
func swapFrom(m map[string][]byte, key string, where func(string) bool) bool {
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if k != key && where(k) && path.Ext(k) == path.Ext(key) && string(m[k]) != string(m[key]) {
			m[key] = slices.Clone(m[k])
			return true
		}
	}
	return false
}

// rotOutcome opens st and reads main back: "corrupt-open", "corrupt-read",
// "error: …" (any other refusal), "ok", "ok+verify" (Verify flagged the
// damage) or "WRONG: …".
func rotOutcome(t *testing.T, st store.Store, want map[string]map[string]string) string {
	db, err := OpenStore(context.Background(), st, quiet())
	if errors.Is(err, ErrCorrupt) {
		return "corrupt-open"
	}
	if err != nil {
		return "error-open: " + firstLine(err)
	}
	defer db.Close()
	for _, br := range slices.Sorted(maps.Keys(want)) {
		for i := 0; i < 7; i++ {
			k := fmt.Sprintf("k%d", i)
			var v []byte
			err := db.Branch(br).View(ctx, func(tx *Tx) (err error) { v, err = tx.Get(k); return err })
			switch w, ok := want[br][k]; {
			case errors.Is(err, ErrCorrupt):
				return "corrupt-read"
			case errors.Is(err, ErrNotFound) && !ok:
			case err != nil:
				return "error-read: " + firstLine(err)
			case !ok || string(v) != w:
				return fmt.Sprintf("WRONG: %s@%s = %q, want %q (present %v)", k, br, v, w, ok)
			}
		}
	}
	r, err := db.Verify(ctx)
	if err != nil || !r.OK || len(r.Warnings) > 0 {
		return "ok+verify"
	}
	return "ok"
}

func firstLine(err error) string {
	s := err.Error()
	if i := strings.IndexAny(s, ":\n"); i > 0 {
		s = s[:i]
	}
	return s
}
