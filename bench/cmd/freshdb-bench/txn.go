package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/raman20/freshdb"
)

type occRun struct {
	System       string  `json:"system"`
	Fsync        bool    `json:"fsync"`
	Clients      int     `json:"clients"`
	HotKeys      int     `json:"hot_keys"`
	Commits      int64   `json:"commits"`
	CommitsPerS  float64 `json:"commits_per_sec"`
	Attempts     int64   `json:"attempts"`
	AbortRate    float64 `json:"abort_rate"` // (attempts - commits) / attempts
	GaveUp       int64   `json:"gave_up_after_16_retries"`
	Latency      Lat     `json:"latency_incl_retries"`
	InvariantOK  bool    `json:"invariant_ok"`
	InvariantMsg string  `json:"invariant"`
}

type lensRun struct {
	Fsync       bool    `json:"fsync"`
	Partitions  int     `json:"partitions"`
	Batch       int     `json:"batch"`
	Txns        int64   `json:"txns"`
	TxnsPerSec  float64 `json:"txns_per_sec"`
	MsgsPerSec  float64 `json:"msgs_per_sec"`
	Latency     Lat     `json:"txn_latency"`
	ExactlyOnce bool    `json:"exactly_once_check"`
	// InputDrained: a worker emptied its input partition before the
	// duration ended and stopped; rates are over the time actually worked.
	InputDrained bool   `json:"input_drained"`
	Check        string `json:"check"`
	Errors       int64  `json:"errors"`
}

func enc(v int64) []byte { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, uint64(v)); return b }
func dec(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func cmdTxn(args []string) {
	fs := flag.NewFlagSet("txn", flag.ExitOnError)
	fsync := fs.Bool("fsync", true, "durable commits")
	clientsF := fs.String("clients", "8,64", "concurrent clients for contended OCC")
	hotF := fs.String("hot", "1,16,256,65536", "hot-set sizes; each txn moves 1 unit between 2 random hot keys (1 = increment one counter)")
	partsF := fs.String("partitions", "1,8", "multi-lens: independent topic partitions, one worker each")
	batchF := fs.String("batch", "1,10", "multi-lens: messages per transaction")
	fs.IntVar(&lensPrefill, "prefill", 1_000_000, "multi-lens: input messages published before the run (256 B each), split across partitions")
	dur := fs.Duration("duration", 5*time.Second, "per cell")
	seed := fs.Uint64("seed", 42, "RNG seed")
	dataDir := fs.String("dir", "/tmp/freshdb-bench", "data directory (wiped)")
	out := fs.String("out", "results", "output directory")
	fs.Parse(args)
	res := &Result{Suite: "txn", Env: envInfo(*dataDir), Params: map[string]any{"fsync": *fsync, "duration": dur.String(), "seed": *seed, "prefill": lensPrefill,
		"occ_txn":        "read 2 hot keys, write both (a-1, b+1); retried on conflict up to 16 times like freshdb.Update",
		"multi_lens_txn": "Fetch N from in-topic group, Put result/<offset> per msg, Publish each to out-topic, Ack last: one transaction"}}
	for _, sys := range []string{"freshdb", "badger"} {
		for _, c := range parseInts(*clientsF) {
			for _, h := range parseInts(*hotF) {
				dir := filepath.Join(*dataDir, fmt.Sprintf("txn-%s-%v", sys, *fsync))
				r := occOnce(sys, dir, *fsync, c, h, *dur, *seed)
				res.Runs = append(res.Runs, r)
				logf("occ %s fsync=%v c=%d hot=%d: %.0f commits/s abort %.1f%% gaveup %d p50 %.0fus p99 %.0fus inv=%v",
					sys, *fsync, c, h, r.CommitsPerS, 100*r.AbortRate, r.GaveUp, r.Latency.P50Us, r.Latency.P99Us, r.InvariantOK)
				wipe(dir)
			}
		}
	}
	for _, p := range parseInts(*partsF) {
		for _, b := range parseInts(*batchF) {
			dir := filepath.Join(*dataDir, fmt.Sprintf("lens-%v", *fsync))
			r := lensOnce(dir, *fsync, p, b, *dur)
			res.Runs = append(res.Runs, r)
			logf("multi-lens fsync=%v parts=%d batch=%d: %.0f txn/s %.0f msg/s p50 %.0fus p99 %.0fus exactly-once=%v (%s)",
				*fsync, p, b, r.TxnsPerSec, r.MsgsPerSec, r.Latency.P50Us, r.Latency.P99Us, r.ExactlyOnce, r.Check)
			wipe(dir)
		}
	}
	res.Params.(map[string]any)["peak_rss"] = peakRSS()
	writeResult(*out, fmt.Sprintf("txn-fsync-%v", *fsync), res)
}

func occOnce(sys, dir string, fsync bool, clients, hot int, dur time.Duration, seed uint64) occRun {
	var attempts, gaveUp atomic.Int64
	var commit func(r *rand.Rand) error
	var sum func() (int64, error)
	var closeFn func()
	key := func(i int) string { return fmt.Sprintf("hot/%06d", i) }
	pick := func(r *rand.Rand) (string, string) {
		if hot == 1 {
			return key(0), ""
		}
		a := r.IntN(hot)
		b := r.IntN(hot - 1)
		if b >= a {
			b++
		}
		return key(a), key(b)
	}
	switch sys {
	case "freshdb":
		db, err := openFreshDB(dir, fsync, false)
		if err != nil {
			fatal(err)
		}
		commit = func(r *rand.Rand) error {
			a, b := pick(r)
			err := db.Update(bg, func(tx *freshdb.Tx) error {
				attempts.Add(1)
				va, err := tx.Get(a)
				if err != nil && !errors.Is(err, freshdb.ErrNotFound) {
					return err
				}
				if b == "" {
					return tx.Put(a, enc(dec(va)+1))
				}
				vb, err := tx.Get(b)
				if err != nil && !errors.Is(err, freshdb.ErrNotFound) {
					return err
				}
				if err := tx.Put(a, enc(dec(va)-1)); err != nil {
					return err
				}
				return tx.Put(b, enc(dec(vb)+1))
			})
			if errors.Is(err, freshdb.ErrConflict) {
				gaveUp.Add(1)
			}
			return err
		}
		sum = func() (s int64, err error) {
			err = db.View(bg, func(tx *freshdb.Tx) error {
				kvs, err := tx.Scan("hot/")
				for _, kv := range kvs {
					s += dec(kv.Value)
				}
				return err
			})
			return
		}
		closeFn = func() { db.Close() }
	case "badger":
		kv, err := openBadger(dir, fsync)
		if err != nil {
			fatal(err)
		}
		db := kv.(*badgerKV).db
		get := func(txn *badger.Txn, k string) (int64, error) {
			it, err := txn.Get([]byte(k))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return 0, nil
			}
			if err != nil {
				return 0, err
			}
			v, err := it.ValueCopy(nil)
			return dec(v), err
		}
		commit = func(r *rand.Rand) error {
			a, b := pick(r)
			var err error
			for i := 0; i < 16; i++ {
				err = db.Update(func(txn *badger.Txn) error {
					attempts.Add(1)
					va, err := get(txn, a)
					if err != nil {
						return err
					}
					if b == "" {
						return txn.Set([]byte(a), enc(va+1))
					}
					vb, err := get(txn, b)
					if err != nil {
						return err
					}
					if err := txn.Set([]byte(a), enc(va-1)); err != nil {
						return err
					}
					return txn.Set([]byte(b), enc(vb+1))
				})
				if !errors.Is(err, badger.ErrConflict) {
					return err
				}
			}
			gaveUp.Add(1)
			return err
		}
		sum = func() (s int64, err error) {
			err = db.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte("hot/")})
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					v, err := it.Item().ValueCopy(nil)
					if err != nil {
						return err
					}
					s += dec(v)
				}
				return nil
			})
			return
		}
		closeFn = func() { kv.Close() }
	}
	ph := runClosed(clients, dur, func(id int) opFunc {
		r := rand.New(rand.NewPCG(seed, uint64(id)))
		return func() error { return commit(r) }
	})
	commits := ph.Ops - ph.Errors
	s, err := sum()
	r := occRun{System: sys, Fsync: fsync, Clients: clients, HotKeys: hot, Commits: commits, CommitsPerS: round(float64(commits)/ph.Seconds, 1),
		Attempts: attempts.Load(), GaveUp: gaveUp.Load(), Latency: ph.Latency}
	if r.Attempts > 0 {
		r.AbortRate = round(float64(r.Attempts-commits)/float64(r.Attempts), 4)
	}
	want := int64(0)
	if hot == 1 {
		want = commits
	}
	r.InvariantOK = err == nil && s == want
	r.InvariantMsg = fmt.Sprintf("sum of hot keys = %d, expected %d (err=%v)", s, want, err)
	closeFn()
	return r
}

var lensPrefill = 1_000_000

func lensOnce(dir string, fsync bool, parts, batch int, dur time.Duration) lensRun {
	db, err := openFreshDB(dir, fsync, false)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	payload := make([]byte, 256)
	// prefill input; a worker that empties its partition stops (errStop), so
	// rates cover only the time spent doing real work (reported as drained)
	prefill := lensPrefill
	for p := 0; p < parts; p++ {
		for i := 0; i < prefill/parts; i += 1000 {
			if err := db.Update(bg, func(tx *freshdb.Tx) error {
				for j := 0; j < 1000; j++ {
					if err := tx.Topic(fmt.Sprintf("in-%d", p)).Publish(payload); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				fatal(err)
			}
		}
	}
	var msgs atomic.Int64
	stopAware = func(err error) bool { return errors.Is(err, errStop) }
	defer func() { stopAware = func(error) bool { return false } }()
	ph := runClosed(parts, dur, func(id int) opFunc {
		in, outT := fmt.Sprintf("in-%d", id), fmt.Sprintf("out-%d", id)
		return func() error {
			var n int
			err := db.Update(bg, func(tx *freshdb.Tx) error {
				g := tx.Topic(in).Group("worker")
				ms, err := g.Fetch(batch)
				if err != nil {
					return err
				}
				if len(ms) == 0 {
					return errStop // input drained: this worker is done
				}
				for _, m := range ms {
					if err := tx.Put(fmt.Sprintf("result/%s/%d", in, m.Offset), m.Value); err != nil {
						return err
					}
					if err := tx.Topic(outT).Publish(m.Value); err != nil {
						return err
					}
				}
				n = len(ms)
				return g.Ack(ms[len(ms)-1])
			})
			if err == nil {
				msgs.Add(int64(n))
			}
			return err
		}
	})
	// exactly-once: for each partition, cursor == out length == result count
	ok, drained, check := true, false, ""
	db.View(bg, func(tx *freshdb.Tx) error {
		var outs, results, cursors uint64
		for p := 0; p < parts; p++ {
			in := fmt.Sprintf("in-%d", p)
			cur, _ := tx.Topic(in).Group("worker").Cursor()
			ol := tx.Topic(fmt.Sprintf("out-%d", p)).Len()
			rs, _ := tx.Scan("result/" + in + "/")
			if cur != ol || ol != uint64(len(rs)) {
				ok = false
			}
			if cur >= uint64(prefill/parts) {
				drained = true
			}
			outs += ol
			results += uint64(len(rs))
			cursors += cur
		}
		check = fmt.Sprintf("consumed %d, out messages %d, result keys %d, acked by workers %d", cursors, outs, results, msgs.Load())
		if cursors != uint64(msgs.Load()) {
			ok = false
		}
		return nil
	})
	return lensRun{Fsync: fsync, Partitions: parts, Batch: batch, Txns: ph.Ops - ph.Errors, TxnsPerSec: round(float64(ph.Ops-ph.Errors)/ph.Seconds, 1),
		MsgsPerSec: round(float64(msgs.Load())/ph.Seconds, 1), Latency: ph.Latency, ExactlyOnce: ok, InputDrained: drained, Check: check, Errors: ph.Errors}
}
