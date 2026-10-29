package freshdb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/raman20/freshdb/store"
)

func TestCommitPolicyDefaultNeverWaits(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	t0 := time.Now()
	for i := 0; i < 20; i++ {
		db.Put(ctx, "k", []byte("v"))
	}
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("20 serial puts took %v", d)
	}
}

// A batch waits up to MaxDelay, gathering what arrives in the window.
func TestCommitPolicyGathers(t *testing.T) {
	const delay = 300 * time.Millisecond
	db := mustOpen(t, store.NewMem(), WithCommitPolicy(CommitPolicy{MaxDelay: delay}))
	defer db.Close()
	segs := db.Stats().Segments
	t0 := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.Put(ctx, fmt.Sprintf("k%d", i), []byte("v")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if d := time.Since(t0); d < delay/2 || d > 5*delay {
		t.Fatalf("batch committed after %v, want about %v", d, delay)
	}
	if n := db.Stats().Segments - segs; n != 1 {
		t.Fatalf("%d segments for 8 puts in one window, want 1", n)
	}
}

// Reaching TargetBytes (or MaxBatch) ends the wait at once.
func TestCommitPolicyTargetBytes(t *testing.T) {
	db := mustOpen(t, store.NewMem(), WithCommitPolicy(CommitPolicy{MaxDelay: time.Hour, TargetBytes: 1}))
	defer db.Close()
	done := make(chan error)
	go func() { done <- db.Put(ctx, "k", []byte("v")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a batch at its byte target still waited")
	}
	db2 := mustOpen(t, store.NewMem(), WithCommitPolicy(CommitPolicy{MaxDelay: time.Hour}), func(o *Options) { o.MaxBatch = 1 })
	defer db2.Close()
	go func() { done <- db2.Put(ctx, "k", []byte("v")) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a full batch (MaxBatch) still waited")
	}
}

// Close does not wait out the budget: a lingering batch is committed.
func TestCommitPolicyClose(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithCommitPolicy(CommitPolicy{MaxDelay: time.Hour}))
	done := make(chan error)
	go func() { done <- db.Put(ctx, "k", []byte("v")) }()
	time.Sleep(50 * time.Millisecond)
	closed := make(chan struct{})
	go func() { db.Close(); close(closed) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("lingering put: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited out MaxDelay")
	}
	<-closed
	db = mustOpen(t, st)
	defer db.Close()
	if v, err := db.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("after close: %q %v", v, err)
	}
}
