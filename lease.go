package freshdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Lease records (design 16): with WithHolder, a writable open writes
// lease/<epoch>.json naming the process that took the write lease. They
// are informational only, never read for correctness: they let the opener
// say whom it fenced, and a fenced writer say who fenced it. GC keeps the
// newest 16 (as for manifests).

// WithHolder names this process in its lease record (e.g. "pid 42 on
// host-a"). Empty (the default) writes no record. See HolderName.
func WithHolder(s string) Option { return func(o *Options) { o.Holder = s } }

// HolderName is a default holder description: pid and host.
func HolderName() string {
	h, _ := os.Hostname()
	return fmt.Sprintf("pid %d on %s", os.Getpid(), h)
}

type leaseRecord struct {
	Epoch  uint64    `json:"epoch"`
	Holder string    `json:"holder"`
	Since  time.Time `json:"since"`
}

func leaseKey(ep uint64) string { return fmt.Sprintf("lease/%020d.json", ep) }

// writeLease records our lease (best effort) and logs whom we fenced.
func (db *DB) writeLease(ctx context.Context, prev uint64) {
	if db.opts.Holder == "" {
		return
	}
	rec, _ := json.Marshal(leaseRecord{Epoch: db.epoch, Holder: db.opts.Holder, Since: time.Unix(0, db.opts.Clock()).UTC()})
	if err := db.st.PutIfAbsent(ctx, leaseKey(db.epoch), rec); err != nil {
		db.logf("freshdb: lease record: %v", err)
	}
	if prev > 0 {
		if who := db.leaseOf(ctx, prev); who != "" {
			db.logf("freshdb: took the write lease (epoch %d) from %s; that writer, if still running, is now fenced", db.epoch, who)
		}
	}
}

// leaseOf describes epoch ep's holder, or "" if it left no record.
func (db *DB) leaseOf(ctx context.Context, ep uint64) string {
	b, err := db.st.Get(ctx, leaseKey(ep))
	var r leaseRecord
	if err != nil || json.Unmarshal(b, &r) != nil {
		return ""
	}
	return fmt.Sprintf("epoch %d (%s, since %s)", ep, r.Holder, r.Since.Format(time.RFC3339))
}

// fenced builds the error a fenced writer reports: ErrFenced naming the
// newer epoch and, if it left a lease record, its holder.
func (db *DB) fenced() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	m, err := loadLatestManifest(ctx, db.st)
	if err != nil || m == nil || m.Epoch <= db.epoch {
		return ErrFenced
	}
	who := db.leaseOf(ctx, m.Epoch)
	if who == "" {
		who = fmt.Sprintf("epoch %d", m.Epoch)
	}
	return fmt.Errorf("%w (our epoch %d; now held by %s)", ErrFenced, db.epoch, who)
}

// deleteOldLeases keeps the lease records of the newest keep epochs.
// Caller holds db.mu.
func (db *DB) deleteOldLeases(ctx context.Context, keep int) {
	keys, err := db.st.List(ctx, "lease/")
	if err != nil {
		return
	}
	for _, k := range keys {
		if n, ok := parseNum(k, "lease/", ".json"); ok && n+uint64(keep) <= db.epoch {
			db.st.Delete(ctx, k)
		}
	}
}
