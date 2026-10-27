package freshdb

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/raman20/freshdb/store"
)

// TestHotKeyReadModifyWriteNoStarvation runs 64 clients incrementing one
// counter. Every increment must commit (oldest-retry-first validation plus
// backoff keeps a client from losing maxRetries times in a row) and the
// total must be exact (serializability).
func TestHotKeyReadModifyWriteNoStarvation(t *testing.T) {
	ctx := context.Background()
	db := mustOpen(t, store.NewMem())
	const clients, each = 64, 30
	var wg sync.WaitGroup
	errs := make(chan error, clients*each)
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				errs <- db.Update(ctx, func(tx *Tx) error {
					n := 0
					if v, err := tx.Get("ctr"); err == nil {
						n, _ = strconv.Atoi(string(v))
					}
					return tx.Put("ctr", []byte(strconv.Itoa(n+1)))
				})
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("increment failed: %v", err)
		}
	}
	v, err := db.Get(ctx, "ctr")
	if err != nil || string(v) != strconv.Itoa(clients*each) {
		t.Fatalf("counter %q %v, want %d", v, err, clients*each)
	}
}

// TestEscalatedUpdate forces every Update onto the writer-side path and
// checks commit, read-your-own-writes, user errors, read-only bodies and
// panics behave as on the optimistic path.
func TestEscalatedUpdate(t *testing.T) {
	old := escalateAfter
	escalateAfter = 0
	defer func() { escalateAfter = old }()
	ctx := context.Background()
	db := mustOpen(t, store.NewMem())
	lsn, err := db.Branch("main").UpdateLSN(ctx, func(tx *Tx) error {
		if err := tx.Put("a", []byte("1")); err != nil {
			return err
		}
		if v, err := tx.Get("a"); err != nil || string(v) != "1" {
			t.Errorf("own write: %q %v", v, err)
		}
		return tx.Topic("t").Publish([]byte("m"))
	})
	if err != nil || lsn == 0 {
		t.Fatalf("commit: %d %v", lsn, err)
	}
	boom := errors.New("boom")
	if err := db.Update(ctx, func(tx *Tx) error { tx.Put("b", []byte("x")); return boom }); err != boom {
		t.Fatalf("user error: %v", err)
	}
	ro, err := db.Branch("main").UpdateLSN(ctx, func(tx *Tx) error { _, err := tx.Get("a"); return err })
	if err != nil || ro != lsn {
		t.Fatalf("read-only: %d %v (head %d)", ro, err, lsn)
	}
	func() {
		defer func() {
			if p := recover(); p != "kaboom" {
				t.Fatalf("panic not re-raised in caller: %v", p)
			}
		}()
		db.Update(ctx, func(tx *Tx) error { panic("kaboom") })
	}()
	if _, err := db.Get(ctx, "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("aborted write visible: %v", err)
	}
	if v, err := db.Get(ctx, "a"); err != nil || string(v) != "1" {
		t.Fatalf("a=%q %v", v, err)
	}
	if err := db.Put(ctx, "c", []byte("ok")); err != nil { // writer still alive
		t.Fatal(err)
	}
}
