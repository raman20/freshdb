package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/raman20/freshdb"
)

type compactRun struct {
	Phase       string  `json:"phase"`          // "before" or "after" compaction
	DiskBytes   int64   `json:"disk_bytes"`     // allocated blocks (du)
	ApparentB   int64   `json:"apparent_bytes"` // file sizes (the journal file never shrinks)
	Files       int     `json:"files"`
	OpenMs      float64 `json:"open_ms"`  // Open (replay) until ready, page cache warm
	HeapMiB     float64 `json:"heap_mib"` // live heap after Open + GC
	CompactMs   float64 `json:"compact_ms,omitempty"`
	WriteSecs   float64 `json:"write_secs,omitempty"`
	LiveKeys    int     `json:"live_keys"`
	LogicalMiB  float64 `json:"logical_mib"` // keys x value size
	WrittenMiB  float64 `json:"written_mib"` // all values ever written
	Deleted     int     `json:"deleted_objects,omitempty"`
	BaseMiB     float64 `json:"base_mib,omitempty"`
	ReadCheckOK bool    `json:"read_check_ok"`
}

// cmdCompact measures what compaction does to storage, open time and
// memory on an overwrite-heavy workload: every key is rewritten -rounds
// times, so the live data is 1/rounds of what was written.
func cmdCompact(args []string) {
	fl := flag.NewFlagSet("compact", flag.ExitOnError)
	dir := fl.String("dir", "/tmp/freshdb-bench/compact", "database directory (wiped first)")
	url := fl.String("url", "", "database URL (default: -dir, the default local layout)")
	keys := fl.Int("keys", 10000, "live keys")
	rounds := fl.Int("rounds", 50, "times every key is overwritten")
	vsize := fl.Int("value", 1024, "value bytes")
	batch := fl.Int("batch", 100, "puts per transaction")
	out := fl.String("out", "results", "output directory")
	fl.Parse(args)
	if *url == "" {
		*url = *dir
	}
	wipe(*dir)
	ctx := context.Background()
	res := &Result{Suite: "compact", Env: envInfo(*dir), Params: map[string]any{
		"url": *url, "keys": *keys, "rounds": *rounds, "value": *vsize, "batch": *batch}}
	quiet := freshdb.WithLogf(func(string, ...any) {})
	db, err := freshdb.Open(ctx, *url, quiet)
	if err != nil {
		fatal(err)
	}
	val := func(r, k int) []byte {
		b := make([]byte, *vsize)
		copy(b, fmt.Sprintf("r%d-k%d-", r, k))
		return b
	}
	t0 := time.Now()
	for r := 0; r < *rounds; r++ {
		for k := 0; k < *keys; k += *batch {
			err := db.Update(ctx, func(tx *freshdb.Tx) error {
				for i := k; i < k+*batch && i < *keys; i++ {
					if err := tx.Put(fmt.Sprintf("key%08d", i), val(r, i)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				fatal(err)
			}
		}
	}
	write := time.Since(t0).Seconds()
	db.Close()
	logical := float64(*keys**vsize) / (1 << 20)
	measure := func(phase string) (compactRun, *freshdb.DB) {
		r := compactRun{Phase: phase, LiveKeys: *keys, LogicalMiB: logical, WrittenMiB: logical * float64(*rounds)}
		r.DiskBytes, r.ApparentB, r.Files = du(*dir)
		runtime.GC()
		t := time.Now()
		db, err := freshdb.Open(ctx, *url, quiet)
		if err != nil {
			fatal(err)
		}
		r.OpenMs = float64(time.Since(t).Microseconds()) / 1000
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		r.HeapMiB = float64(ms.HeapAlloc) / (1 << 20)
		r.ReadCheckOK = check(db, *keys, *rounds, val)
		return r, db
	}
	before, db := measure("before")
	before.WriteSecs = write
	t := time.Now()
	st, err := db.Compact(ctx, freshdb.CompactOptions{})
	if err != nil {
		fatal(err)
	}
	cms := float64(time.Since(t).Microseconds()) / 1000
	db.Close()
	after, db := measure("after")
	db.Close()
	after.CompactMs, after.Deleted, after.BaseMiB = cms, st.Deleted, float64(st.BaseBytes)/(1<<20)
	for _, r := range []compactRun{before, after} {
		logf("compact %-6s disk %8.1f MiB (apparent %8.1f MiB, %d files)  open %8.1f ms  heap %7.1f MiB  reads ok=%v",
			r.Phase, float64(r.DiskBytes)/(1<<20), float64(r.ApparentB)/(1<<20), r.Files, r.OpenMs, r.HeapMiB, r.ReadCheckOK)
		res.Runs = append(res.Runs, r)
	}
	logf("compact: %.0f ms, %d objects deleted, base %.1f MiB; live data %.1f MiB of %.1f MiB written",
		cms, st.Deleted, after.BaseMiB, logical, logical*float64(*rounds))
	writeResult(*out, "compact", res)
}

// check reads every key and compares it with the last round's value.
func check(db *freshdb.DB, keys, rounds int, val func(r, k int) []byte) bool {
	ok := true
	db.View(context.Background(), func(tx *freshdb.Tx) error {
		for k := 0; k < keys; k++ {
			v, err := tx.Get(fmt.Sprintf("key%08d", k))
			if err != nil || string(v) != string(val(rounds-1, k)) {
				ok = false
				return nil
			}
		}
		return nil
	})
	return ok
}

// du returns allocated bytes, apparent bytes and the file count under dir.
func du(dir string) (alloc, apparent int64, files int) {
	filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		files++
		apparent += fi.Size()
		if s, ok := fi.Sys().(*syscall.Stat_t); ok {
			alloc += s.Blocks * 512
		}
		return nil
	})
	return
}
