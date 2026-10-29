//go:build unix

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"syscall"
	"testing"
)

// Disk full on the journal: writes fail with ENOSPC after a random prefix
// lands, syncs fail, and the disk stays full for a while. Every call that
// fails must say ENOSPC; once space is back, appends work without a
// reopen; and any reopen (in the middle, and at the end) must succeed and
// hold every acknowledged put, none of the acknowledged deletes, and for a
// failed call either outcome, never other bytes.
func TestJournalDiskFull(t *testing.T) {
	for seed := int64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) { diskFull(t, seed) })
	}
}

func diskFull(t *testing.T, seed int64) {
	c, a, b := jrnChunk, jrnGrowAt, jrnGrowBit
	jrnChunk, jrnGrowAt, jrnGrowBit = 64<<10, 32<<10, 16<<10
	defer func() { jrnChunk, jrnGrowAt, jrnGrowBit = c, a, b }()
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(seed))
	var mu sync.Mutex
	full, syncFail := 0, false // ops left with a full disk; fail the next sync
	frnd := rand.New(rand.NewSource(seed * 7))
	jfault = func(op byte, n int) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		if op == 's' && syncFail {
			syncFail = false
			return 0, syscall.ENOSPC
		}
		if op == 'w' && full > 0 {
			return frnd.Intn(n + 1), syscall.ENOSPC
		}
		return n, nil
	}
	defer func() { jfault = nil }()
	d := t.TempDir()
	j := mustJournal(t, d)
	want := map[string][]byte{}  // acknowledged and live
	maybe := map[string][]byte{} // failed put or failed delete: either outcome
	var keys []string
	failures := 0
	check := func() {
		t.Helper()
		j.Close()
		mu.Lock()
		f := full
		full = 0
		mu.Unlock()
		var err error
		if j, err = NewJournal(d); err != nil {
			t.Fatalf("reopen after disk full: %v", err)
		}
		mu.Lock()
		full = f
		mu.Unlock()
		got, _ := j.Keys(ctx)
		seen := map[string]bool{}
		for _, k := range got {
			seen[k] = true
			v, err := j.Get(ctx, k)
			if err != nil {
				t.Fatalf("get %s: %v", k, err)
			}
			if w, ok := want[k]; ok && !bytes.Equal(v, w) {
				t.Fatalf("%s: wrong bytes", k)
			} else if m, ok := maybe[k]; !ok && want[k] == nil {
				t.Fatalf("%s present but never acknowledged nor attempted", k)
			} else if ok && !bytes.Equal(v, m) {
				t.Fatalf("%s: bytes of a failed put differ", k)
			}
		}
		for k := range want {
			if !seen[k] {
				t.Fatalf("acknowledged %s lost after disk full", k)
			}
		}
	}
	for i := 0; i < 400; i++ {
		mu.Lock()
		switch r := rnd.Intn(30); {
		case r == 0 && full == 0:
			full = 1 + rnd.Intn(6)
		case r == 1:
			syncFail = true
		}
		if full > 0 {
			full--
		}
		mu.Unlock()
		if rnd.Intn(40) == 0 {
			check()
			continue
		}
		if rnd.Intn(4) == 0 && len(keys) > 0 {
			x := rnd.Intn(len(keys))
			k := keys[x]
			old, wasWant := want[k]
			err := j.Delete(ctx, k)
			if err != nil {
				if !errors.Is(err, syscall.ENOSPC) {
					t.Fatalf("delete: %v (want ENOSPC)", err)
				}
				failures++
				if wasWant {
					maybe[k] = old
					delete(want, k)
				}
				continue
			}
			keys = append(keys[:x], keys[x+1:]...)
			delete(want, k)
			delete(maybe, k)
			continue
		}
		k := fmt.Sprintf("log/L1/%04d.seg", i)
		v := make([]byte, 1+rnd.Intn(9000))
		rnd.Read(v)
		if err := j.PutIfAbsent(ctx, k, v); err != nil {
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("put: %v (want ENOSPC)", err)
			}
			failures++
			maybe[k] = v
			keys = append(keys, k)
			continue
		}
		want[k] = v
		keys = append(keys, k)
	}
	mu.Lock()
	full, syncFail = 0, false
	mu.Unlock()
	// space is back: appends work in the same process
	if err := j.PutIfAbsent(ctx, "log/L1/after.seg", []byte("x")); err != nil {
		t.Fatalf("append after space freed: %v", err)
	}
	want["log/L1/after.seg"] = []byte("x")
	check()
	j.Close()
	if failures == 0 {
		t.Fatal("no injected failure hit")
	}
	t.Logf("seed %d: %d failed calls, %d live objects", seed, failures, len(want))
}
