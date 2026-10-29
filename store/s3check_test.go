package store

import (
	"context"
	"strings"
	"testing"
)

// lax is a store whose "create-only" put silently overwrites: the failure
// mode of an S3-compatible endpoint that ignores If-None-Match.
type lax struct{ *Mem }

func (l lax) PutIfAbsent(ctx context.Context, k string, v []byte) error {
	l.Mem.Delete(ctx, k)
	return l.Mem.PutIfAbsent(ctx, k, v)
}

func TestS3Check(t *testing.T) {
	ctx := context.Background()
	for _, r := range S3Check(ctx, NewMem(), 1<<20) {
		if !r.OK {
			t.Errorf("mem: %s: %s", r.Name, r.Detail)
		}
	}
	var bad []string
	for _, r := range S3Check(ctx, lax{NewMem()}, 0) {
		if !r.OK {
			bad = append(bad, r.Name)
		}
	}
	if !strings.Contains(strings.Join(bad, "|"), "create-only") {
		t.Fatalf("a store without conditional writes passed: failed=%v", bad)
	}
	t.Logf("lax store failed: %v", bad)
	m := NewMem()
	S3Check(ctx, m, 1<<20)
	if ks, _ := m.List(ctx, ""); len(ks) != 0 {
		t.Fatalf("left behind: %v", ks)
	}
}
