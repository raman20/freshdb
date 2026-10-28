package freshdb

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitHead(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, "mem://", WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	pub := func(v string) {
		if err := db.Update(ctx, func(tx *Tx) error { return tx.Topic("t").Publish([]byte(v)) }); err != nil {
			t.Fatal(err)
		}
	}
	pub("a")
	h0, err := db.Head("main")
	if err != nil {
		t.Fatal(err)
	}

	// no change: times out with the current head
	tctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	h, err := db.WaitHead(tctx, "main", h0)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || h != h0 {
		t.Fatalf("idle wait: %d, %v", h, err)
	}
	// already stale: returns at once
	if h, err := db.WaitHead(ctx, "main", h0-1); err != nil || h != h0 {
		t.Fatalf("stale: %d, %v", h, err)
	}

	// a commit wakes a blocked waiter
	got := make(chan uint64, 1)
	go func() { h, _ := db.WaitHead(ctx, "main", h0); got <- h }()
	time.Sleep(20 * time.Millisecond)
	pub("b")
	select {
	case h := <-got:
		if h <= h0 {
			t.Fatalf("woke with %d <= %d", h, h0)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not wake the waiter")
	}

	// a rewind (head moves back) wakes it too
	h1, _ := db.Head("main")
	go func() { h, _ := db.WaitHead(ctx, "main", h1); got <- h }()
	time.Sleep(20 * time.Millisecond)
	if err := db.Rewind(ctx, "main", h0); err != nil {
		t.Fatal(err)
	}
	select {
	case h := <-got:
		if h != h0 {
			t.Fatalf("after rewind: %d, want %d", h, h0)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rewind did not wake the waiter")
	}

	if _, err := db.WaitHead(ctx, "nope", 0); !errors.Is(err, ErrNoBranch) {
		t.Fatalf("unknown branch: %v", err)
	}

	// Close wakes waiters with ErrClosed
	h2, _ := db.Head("main")
	errc := make(chan error, 1)
	go func() { _, err := db.WaitHead(ctx, "main", h2); errc <- err }()
	time.Sleep(20 * time.Millisecond)
	db.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("after close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not wake the waiter")
	}
}

// Many waiters and a stream of commits: every waiter sees every change
// eventually (no lost wake-up), checked by waiting for the final head.
func TestWaitHeadNoLostWakeup(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, "mem://", WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const waiters = 200 / 25
	base, _ := db.Head("main")
	n := base + 200
	done := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			seen := base
			for seen < n {
				h, err := db.WaitHead(ctx, "main", seen)
				if err != nil {
					done <- err
					return
				}
				seen = h
			}
			done <- nil
		}()
	}
	for i := 0; i < 200; i++ {
		if err := db.Update(ctx, func(tx *Tx) error { return tx.Put("k", []byte{byte(i)}) }); err != nil {
			t.Fatal(err)
		}
	}
	if h, _ := db.Head("main"); h != n {
		t.Fatalf("head %d, want %d", h, n)
	}
	for i := 0; i < waiters; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a waiter missed the last change")
		}
	}
}
