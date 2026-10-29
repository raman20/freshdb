package store

import (
	"context"
	"errors"
	"testing"
)

// Read-path faults are transient: whatever a faulty read returns, the
// stored object is unchanged and a clean read returns it.
func TestFaultyReadFaultsAreTransient(t *testing.T) {
	ctx := context.Background()
	mem := NewMem()
	keys := []string{"log/L1/1.seg", "log/L1/2.seg", "log/L1/3.seg"}
	for i, k := range keys {
		mem.PutIfAbsent(ctx, k, []byte{byte(i), 1, 2, 3, 4, 5, 6, 7})
	}
	f := NewFaulty(mem, 1, FaultProbs{BitFlip: 0.3, Misdirect: 0.3, ShortRead: 0.3, StaleList: 0.5})
	f.On = true
	seen := map[string]bool{}
	for i := 0; i < 300; i++ {
		k := keys[i%3]
		b, err := f.Get(ctx, k)
		want, _ := mem.Get(ctx, k)
		switch {
		case errors.Is(err, ErrShortRead):
			seen["short"] = true
		case err != nil:
			t.Fatal(err)
		case string(b) != string(want):
			seen["wrong bytes"] = true
		}
		if l, _ := f.List(ctx, "log/L1/"); len(l) < 3 {
			seen["stale"] = true
		}
	}
	if len(seen) != 3 {
		t.Fatalf("faults seen: %v", seen)
	}
	for i, k := range keys {
		if b, _ := mem.Get(ctx, k); b[0] != byte(i) || len(b) != 8 {
			t.Fatalf("%s changed: %v", k, b)
		}
	}
	// other prefixes are never read-faulted
	mem.PutIfAbsent(ctx, "keys/x", []byte("x"))
	for i := 0; i < 50; i++ {
		if b, err := f.Get(ctx, "keys/x"); err != nil || string(b) != "x" {
			t.Fatalf("keys/x: %q %v", b, err)
		}
	}
}
