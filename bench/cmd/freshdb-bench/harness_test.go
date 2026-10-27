package main

import (
	"testing"
	"time"
)

// An engine slower than the target rate must not make an open-loop phase run
// forever: it stops at 2x the duration and reports what it could not issue.
func TestOpenLoopOverloadStopsAndReports(t *testing.T) {
	t0 := time.Now()
	p := runOpen(1, 1000, 200*time.Millisecond, func(int) opFunc {
		return func() error { time.Sleep(10 * time.Millisecond); return nil }
	})
	if el := time.Since(t0); el > time.Second {
		t.Fatalf("phase took %v, want about 400ms", el)
	}
	if p.NotIssued == 0 || p.Ops+p.NotIssued != 200 {
		t.Fatalf("ops %d + not issued %d, want total 200 with some not issued", p.Ops, p.NotIssued)
	}
	if p.Latency.MaxUs < 100_000 {
		t.Fatalf("max latency %vus: queueing behind the slow op must be charged", p.Latency.MaxUs)
	}
}

// A no-op at a modest rate completes every scheduled op.
func TestOpenLoopCompletes(t *testing.T) {
	p := runOpen(4, 2000, 200*time.Millisecond, func(int) opFunc { return func() error { return nil } })
	if p.Ops != 400 || p.NotIssued != 0 || p.Errors != 0 {
		t.Fatalf("ops %d not issued %d errors %d", p.Ops, p.NotIssued, p.Errors)
	}
}
