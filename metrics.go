package freshdb

import (
	"context"
	"errors"
	"fmt"

	"math"
	"sync/atomic"
	"time"

	"github.com/raman20/freshdb/store"
)

// LatencyBounds are the upper bounds (seconds) of the latency histogram
// buckets in Stats; the last bucket is +Inf.
var LatencyBounds = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// LatencyHistogram is a lock-free latency LatencyHistogram over LatencyBounds.
type LatencyHistogram struct {
	counts [17]atomic.Uint64 // len(LatencyBounds)+1
	sumNs  atomic.Uint64
}

func (h *LatencyHistogram) Observe(d time.Duration) {
	s := d.Seconds()
	i := 0
	for i < len(LatencyBounds) && s > LatencyBounds[i] {
		i++
	}
	h.counts[i].Add(1)
	h.sumNs.Add(uint64(max(d, 0)))
}

// Histogram is a snapshot: Counts[i] observations ≤ LatencyBounds[i] and
// > the previous bound (not cumulative); the last entry is above every bound.
type Histogram struct {
	Counts []uint64 `json:"counts"`
	Count  uint64   `json:"count"`
	Sum    float64  `json:"sum_seconds"`
}

func (h *LatencyHistogram) Snapshot() Histogram {
	out := Histogram{Counts: make([]uint64, len(h.counts))}
	for i := range h.counts {
		out.Counts[i] = h.counts[i].Load()
		out.Count += out.Counts[i]
	}
	out.Sum = float64(h.sumNs.Load()) / 1e9
	return out
}

// Quantile estimates the q-quantile (0..1) from the buckets (upper bound
// of the bucket holding it; +Inf beyond the last bound).
func (h Histogram) Quantile(q float64) float64 {
	if h.Count == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.Count)))
	var seen uint64
	for i, c := range h.Counts {
		seen += c
		if seen >= rank {
			if i < len(LatencyBounds) {
				return LatencyBounds[i]
			}
			return math.Inf(1)
		}
	}
	return math.Inf(1)
}

// metrics are the engine's counters (DB.Stats).
type metrics struct {
	commits, commitErrors, conflicts atomic.Uint64
	segments, segmentBytes           atomic.Uint64
	commitLat, segmentLat            LatencyHistogram
	compactions, compactErrors       atomic.Uint64
	gcs, gcErrors                    atomic.Uint64
	compactLat                       LatencyHistogram
	snapshots, snapshotErrors        atomic.Uint64
	halted                           atomic.Pointer[error] // first writer halt
}

// Stats are cumulative counters since Open, for monitoring
// (`serve` exports them on /metrics).
type Stats struct {
	Commits      uint64 `json:"commits"`       // transactions committed
	CommitErrors uint64 `json:"commit_errors"` // failed for any reason but a conflict
	Conflicts    uint64 `json:"conflicts"`     // attempts that lost a serializability conflict (retried)
	// CommitLatency: submit to acknowledgement of each commit attempt
	// (queueing for the group commit + the segment write).
	CommitLatency Histogram `json:"commit_latency"`
	Segments      uint64    `json:"segments"` // group commits written
	SegmentBytes  uint64    `json:"segment_bytes"`
	// SegmentWrite: the store write (and fsync) of one group commit.
	SegmentWrite     Histogram `json:"segment_write"`
	Compactions      uint64    `json:"compactions"`
	CompactionErrors uint64    `json:"compaction_errors"`
	GCs              uint64    `json:"gcs"`
	GCErrors         uint64    `json:"gc_errors"`
	// CompactionPause: how long commits were paused by each compaction or GC.
	CompactionPause  Histogram `json:"compaction_pause"`
	Snapshots        uint64    `json:"snapshots"`
	SnapshotErrors   uint64    `json:"snapshot_errors"`
	ValueCacheHits   uint64    `json:"value_cache_hits"`
	ValueCacheMisses uint64    `json:"value_cache_misses"`
	ValueCacheBytes  int64     `json:"value_cache_bytes"` // capacity
	// Heads is the head LSN of every branch.
	Heads map[string]uint64 `json:"heads"`
	// Halted is the error that stopped a writer ("" if none); commits on
	// that branch fail until the database is reopened.
	Halted string `json:"halted,omitempty"`
}

// Stats returns the counters since Open.
func (db *DB) Stats() Stats {
	m := &db.met
	s := Stats{
		Commits: m.commits.Load(), CommitErrors: m.commitErrors.Load(), Conflicts: m.conflicts.Load(),
		CommitLatency: m.commitLat.Snapshot(), Segments: m.segments.Load(), SegmentBytes: m.segmentBytes.Load(),
		SegmentWrite: m.segmentLat.Snapshot(), Compactions: m.compactions.Load(), CompactionErrors: m.compactErrors.Load(),
		GCs: m.gcs.Load(), GCErrors: m.gcErrors.Load(), CompactionPause: m.compactLat.Snapshot(),
		Snapshots: m.snapshots.Load(), SnapshotErrors: m.snapshotErrors.Load(),
		Heads: db.Branches(),
	}
	s.ValueCacheHits, s.ValueCacheMisses, s.ValueCacheBytes = db.vcache.counts()
	if h := m.halted.Load(); h != nil {
		s.Halted = (*h).Error()
	}
	return s
}

// Healthy reports whether the database can take writes: nil, or why not
// (closed, fenced by another writer, or a writer halted by a storage
// error until reopen). Reads may still work when it is not nil.
func (db *DB) Healthy() error {
	db.mu.Lock()
	closed, failed := db.closed, db.failed
	db.mu.Unlock()
	switch {
	case closed:
		return ErrClosed
	case failed != nil:
		return failed
	}
	if h := db.met.halted.Load(); h != nil {
		return *h
	}
	return nil
}

// Ping is Healthy plus a round trip to the store: it reads the current
// manifest and checks that no newer one exists (a newer one means another
// process took the write lease, and this one is fenced). Two store reads;
// callers polling it (readiness probes) should rate-limit.
func (db *DB) Ping(ctx context.Context) error {
	if err := db.Healthy(); err != nil {
		return err
	}
	db.mu.RLock()
	id := db.man.ID
	db.mu.RUnlock()
	if _, err := db.st.Get(ctx, manifestKey(id)); err != nil {
		return fmt.Errorf("freshdb: store unreachable: %w", err)
	}
	switch _, err := db.st.Get(ctx, manifestKey(id+1)); {
	case err == nil:
		return ErrFenced
	case !errors.Is(err, store.ErrNotFound):
		return fmt.Errorf("freshdb: store unreachable: %w", err)
	}
	return nil
}
