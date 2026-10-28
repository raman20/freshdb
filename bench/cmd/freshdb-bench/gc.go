package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/raman20/freshdb"
	"github.com/raman20/freshdb/store"
)

type gcCycle struct {
	Mode        string  `json:"mode"` // compact, gc, gc+shrink
	Cycle       int     `json:"cycle"`
	DiskBytes   int64   `json:"disk_bytes"`     // allocated blocks after maintenance
	ApparentB   int64   `json:"apparent_bytes"` // file sizes after maintenance
	WrittenMiB  float64 `json:"written_mib"`    // values written so far
	Manifest    uint64  `json:"manifest_id"`
	Logs        int     `json:"logs"` // logs in the manifest
	Reflog      int     `json:"reflog"`
	MaintMs     float64 `json:"maint_ms"`
	Deleted     int     `json:"deleted_objects"`
	LogsDropped int     `json:"logs_dropped,omitempty"`
	ManDeleted  int     `json:"manifests_deleted,omitempty"`
}

// cmdGC measures whether storage stays bounded under overwrite + branch +
// rewind churn with Compact, with GC, and with GC plus a journal shrink.
func cmdGC(args []string) {
	fl := flag.NewFlagSet("gc", flag.ExitOnError)
	dir := fl.String("dir", "/tmp/freshdb-bench/gc", "database directory (wiped per mode)")
	keys := fl.Int("keys", 10000, "live keys")
	vsize := fl.Int("value", 1024, "value bytes")
	cycles := fl.Int("cycles", 20, "churn cycles (each overwrites every key once)")
	batch := fl.Int("batch", 100, "puts per transaction")
	modes := fl.String("modes", "compact,gc,gc+shrink", "maintenance modes")
	keepCommits := fl.Uint64("keep-commits", 10, "GC history window (commits)")
	out := fl.String("out", "results", "output directory")
	fl.Parse(args)
	ctx := context.Background()
	res := &Result{Suite: "gc", Env: envInfo(*dir), Params: map[string]any{
		"keys": *keys, "value": *vsize, "cycles": *cycles, "batch": *batch, "keep_commits": *keepCommits,
		"gc": "KeepReflog 4, KeepManifests 4"}}
	quiet := freshdb.WithLogf(func(string, ...any) {})
	for _, mode := range strings.Split(*modes, ",") {
		wipe(*dir)
		db, err := freshdb.Open(ctx, *dir, quiet)
		if err != nil {
			fatal(err)
		}
		written := 0.0
		for c := 0; c < *cycles; c++ {
			val := []byte(fmt.Sprintf("c%d-", c) + strings.Repeat("v", *vsize))[:*vsize]
			for k := 0; k < *keys; k += *batch {
				if err := db.Update(ctx, func(tx *freshdb.Tx) error {
					for i := k; i < k+*batch && i < *keys; i++ {
						if err := tx.Put(fmt.Sprintf("key%08d", i), val); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					fatal(err)
				}
			}
			written += float64(*keys**vsize) / (1 << 20)
			// branch churn: a short-lived branch per cycle, and a rewind
			b := fmt.Sprint("b", c)
			if err := db.CreateBranch(ctx, b, "main", 0); err != nil {
				fatal(err)
			}
			if err := db.Branch(b).Update(ctx, func(tx *freshdb.Tx) error { return tx.Put("on-"+b, val) }); err != nil {
				fatal(err)
			}
			if c > 0 {
				if err := db.DeleteBranch(ctx, fmt.Sprint("b", c-1)); err != nil {
					fatal(err)
				}
			}
			h, _ := db.Branch("main").Head()
			if err := db.Rewind(ctx, "main", h); err != nil { // retires a log, no data lost
				fatal(err)
			}
			t := time.Now()
			var st freshdb.CompactStats
			if mode == "compact" {
				st, err = db.Compact(ctx, freshdb.CompactOptions{})
			} else {
				st, err = db.GC(ctx, freshdb.GCOptions{KeepCommits: *keepCommits, KeepReflog: 4, KeepManifests: 4})
			}
			if err != nil {
				fatal(err)
			}
			if mode == "gc+shrink" {
				db.Close()
				if _, err := store.ShrinkJournal(*dir); err != nil {
					fatal(err)
				}
				if db, err = freshdb.Open(ctx, *dir, quiet); err != nil {
					fatal(err)
				}
			}
			ms := float64(time.Since(t).Microseconds()) / 1000
			m := db.Manifest()
			r := gcCycle{Mode: mode, Cycle: c, WrittenMiB: written, Manifest: m.ID, Logs: len(m.Logs), Reflog: len(m.Reflog),
				MaintMs: ms, Deleted: st.Deleted, LogsDropped: st.LogsDropped, ManDeleted: st.ManifestsDeleted}
			r.DiskBytes, r.ApparentB, _ = du(*dir)
			logf("gc %-9s cycle %2d: disk %7.1f MiB apparent %7.1f MiB (written %6.0f MiB) logs %3d reflog %3d manifest %4d  %6.0f ms",
				mode, c, float64(r.DiskBytes)/(1<<20), float64(r.ApparentB)/(1<<20), written, r.Logs, r.Reflog, r.Manifest, ms)
			res.Runs = append(res.Runs, r)
		}
		v, err := db.Get(ctx, fmt.Sprintf("key%08d", *keys-1))
		if err != nil || !strings.HasPrefix(string(v), fmt.Sprintf("c%d-", *cycles-1)) {
			fatal(fmt.Errorf("%s: last value wrong: %.10q %v", mode, v, err))
		}
		if r, err := db.Verify(ctx); err != nil || !r.OK {
			fatal(fmt.Errorf("%s: verify %+v %v", mode, r, err))
		}
		db.Close()
	}
	writeResult(*out, "gc", res)
}
