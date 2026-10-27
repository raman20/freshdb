package freshdb

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/raman20/freshdb/store"
)

func bigVal(tag string, i int) []byte {
	return []byte(strings.Repeat(fmt.Sprintf("%s-%d|", tag, i), 100))
}

// Large values are kept as log positions (not in memory) and must read back
// identically at the head, at older LSNs (time travel), after a restart and
// on a branch that reads its parent's segments; small ones stay inline.
func TestValuesByPosition(t *testing.T) {
	for name, mk := range backends(t) {
		for _, cache := range []int{0, -1} {
			t.Run(fmt.Sprintf("%s/cache=%d", name, cache), func(t *testing.T) {
				st := mk()
				db := mustOpen(t, st, WithValueCacheBytes(cache))
				const n = 300
				var mid uint64
				for round := 0; round < 2; round++ {
					lsn, err := db.Branch("main").UpdateLSN(ctx, func(tx *Tx) error {
						for i := 0; i < n; i++ {
							if err := tx.Put(fmt.Sprint("k", i), bigVal(fmt.Sprint("r", round), i)); err != nil {
								return err
							}
						}
						return tx.Put("small", []byte(fmt.Sprint("s", round)))
					})
					if err != nil {
						t.Fatal(err)
					}
					if round == 0 {
						mid = lsn
					}
				}
				check := func(db *DB, branch string, at uint64, round int) {
					t.Helper()
					err := db.Branch(branch).ViewAt(ctx, at, func(tx *Tx) error {
						for i := 0; i < n; i++ {
							v, err := tx.Get(fmt.Sprint("k", i))
							if err != nil || !bytes.Equal(v, bigVal(fmt.Sprint("r", round), i)) {
								return fmt.Errorf("k%d at %d: %.20q %v", i, at, v, err)
							}
						}
						kvs, err := tx.Scan("k1")
						if err != nil || len(kvs) == 0 || !bytes.Equal(kvs[0].Value, bigVal(fmt.Sprint("r", round), 1)) {
							return fmt.Errorf("scan: %d %v", len(kvs), err)
						}
						v, err := tx.Get("small")
						if err != nil || string(v) != fmt.Sprint("s", round) {
							return fmt.Errorf("small: %q %v", v, err)
						}
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				check(db, "main", 0, 1)
				check(db, "main", mid, 0)
				// the lens keeps positions for large values, bytes for small ones
				v := db.views[db.man.Refs["main"]]
				if ver, ok := v.kv.Get(spaceKV, "k1", v.head); !ok || !ver.Ref() || ver.Val != nil {
					t.Fatalf("large value not kept as a position: %+v", ver)
				}
				if ver, ok := v.kv.Get(spaceKV, "small", v.head); !ok || ver.Ref() {
					t.Fatalf("small value not inline: %+v", ver)
				}
				if err := db.CreateBranch(ctx, "b", "main", mid); err != nil {
					t.Fatal(err)
				}
				check(db, "b", 0, 0)
				db.Close()
				db = mustOpen(t, st, WithValueCacheBytes(cache))
				defer db.Close()
				check(db, "main", 0, 1)
				check(db, "main", mid, 0)
				check(db, "b", 0, 0)
			})
		}
	}
}

// Subject-encrypted large values are positions too: they decrypt with the
// op index kept in the lens, and become unreadable after Shred.
func TestEncryptedValuesByPosition(t *testing.T) {
	db := mustOpen(t, store.NewMem(), WithInlineMax(-1))
	defer db.Close()
	err := db.Update(ctx, func(tx *Tx) error {
		tx.Put("plain", []byte("p"))
		tx.Put("a", bigVal("alice", 1), Subject("alice"))
		return tx.Put("b", []byte("bob"), Subject("bob"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(ctx, "a"); err != nil || !bytes.Equal(v, bigVal("alice", 1)) {
		t.Fatalf("a: %.20q %v", v, err)
	}
	if v, err := db.Get(ctx, "b"); err != nil || string(v) != "bob" {
		t.Fatalf("b: %q %v", v, err)
	}
	if _, err := db.Shred(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("shredded value readable: %v", err)
	}
	if v, err := db.Get(ctx, "b"); err != nil || string(v) != "bob" {
		t.Fatalf("b after shred: %q %v", v, err)
	}
}

// A value whose bytes change under the lens is reported, never returned.
func TestValueChecksumDetectsCorruption(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithValueCacheBytes(-1))
	defer db.Close()
	want := bigVal("x", 1)
	if err := db.Put(ctx, "k", want); err != nil {
		t.Fatal(err)
	}
	st.Raw(func(m map[string][]byte) {
		for k, b := range m {
			if strings.HasPrefix(k, "log/") {
				if i := bytes.Index(b, want); i >= 0 {
					b[i+10] ^= 1
				}
			}
		}
	})
	if _, err := db.Get(ctx, "k"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupted value: err=%v", err)
	}
}
