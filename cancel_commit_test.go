package freshdb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/raman20/freshdb/store"
)

// cancelStore cancels the caller's context when a manifest put starts,
// as an HTTP client that times out on POST /v1/compact does.
type cancelStore struct {
	store.Store
	cancel context.CancelFunc
}

func (c *cancelStore) PutIfAbsent(ctx context.Context, k string, b []byte) error {
	if c.cancel != nil && strings.HasPrefix(k, "manifest/") {
		c.cancel()
	}
	return c.Store.PutIfAbsent(ctx, k, b)
}

// A caller that gives up during (or before) a manifest commit must not
// halt the database: found in a soak, where a client timeout on
// /v1/compact made every later write fail until a restart.
func TestCompactCallerCancelDoesNotHalt(t *testing.T) {
	cs := &cancelStore{Store: store.NewMem()}
	db := mustOpen(t, cs)
	defer db.Close()
	fill(t, db, 10)

	// cancelled while the manifest put runs: the commit completes
	c, cancel := context.WithCancel(ctx)
	cs.cancel = cancel
	if _, err := db.Compact(c, CompactOptions{}); err != nil {
		t.Fatalf("compact cancelled mid-commit: %v", err)
	}
	cs.cancel = nil
	if err := db.Put(ctx, "after", []byte("1")); err != nil {
		t.Fatalf("write after a cancelled compaction: %v", err)
	}

	// cancelled before it starts: nothing is committed, no halt
	fill(t, db, 3)
	c, cancel = context.WithCancel(ctx)
	cancel()
	if _, err := db.Compact(c, CompactOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("compact with a cancelled context: %v, want context.Canceled", err)
	}
	if err := db.Put(ctx, "after2", []byte("1")); err != nil {
		t.Fatalf("write after a refused compaction: %v", err)
	}
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// GC takes the same path
	fill(t, db, 3)
	c, cancel = context.WithCancel(ctx)
	cs.cancel = cancel
	if _, err := db.GC(c, GCOptions{}); err != nil {
		t.Fatalf("gc cancelled mid-commit: %v", err)
	}
	cs.cancel = nil
	if err := db.Put(ctx, "after3", []byte("1")); err != nil {
		t.Fatalf("write after a cancelled gc: %v", err)
	}
}
