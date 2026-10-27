package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/raman20/freshdb"
	"github.com/raman20/freshdb/store"
)

// AWS S3 Standard request prices, us-east-1, from the AWS Price List API
// (pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json,
// publicationDate 2026-10-15T17:35:40Z). Estimates only: no storage, transfer, tiers or discounts.
const (
	pricePutPer1k = 0.005  // PUT, COPY, POST, LIST
	priceGetPer1k = 0.0004 // GET and all other requests (DELETE is free)
	priceSource   = "AWS Price List API, AmazonS3 us-east-1, publicationDate 2026-10-15: $0.005 per 1,000 PUT/COPY/POST/LIST, $0.0004 per 1,000 GET"
)

// counting wraps a Store and counts requests by type.
type counting struct {
	inner                   store.Store
	puts, gets, lists, dels atomic.Int64
}

func (c *counting) PutIfAbsent(ctx context.Context, k string, b []byte) error {
	c.puts.Add(1)
	return c.inner.PutIfAbsent(ctx, k, b)
}
func (c *counting) Get(ctx context.Context, k string) ([]byte, error) {
	c.gets.Add(1)
	return c.inner.Get(ctx, k)
}
func (c *counting) List(ctx context.Context, p string) ([]string, error) {
	c.lists.Add(1)
	return c.inner.List(ctx, p)
}
func (c *counting) Delete(ctx context.Context, k string) error {
	c.dels.Add(1)
	return c.inner.Delete(ctx, k)
}

type reqs struct{ Put, Get, List, Delete int64 }

func (c *counting) snap() reqs {
	return reqs{c.puts.Load(), c.gets.Load(), c.lists.Load(), c.dels.Load()}
}
func (a reqs) sub(b reqs) reqs {
	return reqs{a.Put - b.Put, a.Get - b.Get, a.List - b.List, a.Delete - b.Delete}
}

type s3Run struct {
	Kind            string    `json:"kind"`
	Clients         int       `json:"clients,omitempty"`
	KeysPerTxn      int       `json:"keys_per_txn,omitempty"`
	Commits         int64     `json:"commits,omitempty"`
	CommitsPerSec   float64   `json:"commits_per_sec,omitempty"`
	KeysPerSec      float64   `json:"keys_per_sec,omitempty"`
	Latency         *Lat      `json:"commit_latency,omitempty"`
	PutsPerCommit   float64   `json:"put_requests_per_commit,omitempty"`
	OtherPerCommit  float64   `json:"get_list_requests_per_commit,omitempty"`
	CostPerMCommits float64   `json:"est_usd_per_million_commits,omitempty"`
	CostPerMKeys    float64   `json:"est_usd_per_million_keys,omitempty"`
	Segments        int       `json:"segments,omitempty"`
	OpenMsMedian    float64   `json:"open_ms_median,omitempty"`
	OpenMsAll       []float64 `json:"open_ms_all,omitempty"`
	OpenRequests    *reqs     `json:"open_requests,omitempty"`
	Errors          int64     `json:"errors,omitempty"`
}

func cmdS3(args []string) {
	fs := flag.NewFlagSet("s3", flag.ExitOnError)
	url := fs.String("url", os.Getenv("FRESHDB_BENCH_S3"), "s3://bucket/prefix?endpoint=...&insecure=1&create=1 (credentials from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)")
	batchF := fs.String("batch", "1,10,100,1000", "keys per transaction (1 client)")
	clientsF := fs.String("clients", "1,8,64", "concurrent clients, 1 key per transaction (group commit)")
	segsF := fs.String("segments", "100,1000,5000", "cold-open: number of segments in the log")
	dur := fs.Duration("duration", 5*time.Second, "per cell")
	vsize := fs.Int("value", 1024, "value bytes")
	out := fs.String("out", "results", "output directory")
	fs.Parse(args)
	if *url == "" {
		fatal(fmt.Errorf("s3: set -url or FRESHDB_BENCH_S3"))
	}
	run := fmt.Sprintf("%d", time.Now().UnixNano())
	open := func(name string) (*counting, *freshdb.DB) {
		st, err := store.Open(withPrefix(*url, "bench-"+run+"/"+name))
		if err != nil {
			fatal(err)
		}
		c := &counting{inner: st}
		db, err := freshdb.OpenStore(bg, c, freshdb.WithLogf(func(string, ...any) {}))
		if err != nil {
			fatal(err)
		}
		return c, db
	}
	res := &Result{Suite: "s3", Env: envInfo("/tmp"), Params: map[string]any{"url_redacted": redact(*url), "duration": dur.String(), "value_bytes": *vsize, "price_source": priceSource}}
	res.Notes = append(res.Notes,
		"Latency is to a local MinIO (single node, single drive, loopback) unless the URL says otherwise; real S3 adds network round trips of milliseconds per request.",
		"Cost = requests x published request price only (no storage, data transfer, free tier or volume discounts). It is an estimate.")
	val := make([]byte, *vsize)
	cost := func(r reqs) float64 {
		return float64(r.Put+r.List)/1000*pricePutPer1k + float64(r.Get)/1000*priceGetPer1k
	}
	measure := func(kind string, clients, batch int) s3Run {
		c, db := open(fmt.Sprintf("%s-c%d-b%d", kind, clients, batch))
		defer db.Close()
		var keys atomic.Int64
		before := c.snap()
		ph := runClosed(clients, *dur, func(id int) opFunc {
			r := rand.New(rand.NewPCG(uint64(id), 9))
			return func() error {
				return db.Update(bg, func(tx *freshdb.Tx) error {
					for i := 0; i < batch; i++ {
						if err := tx.Put(fmt.Sprintf("k/%d/%d", id, r.Uint64()), val); err != nil {
							return err
						}
					}
					keys.Add(int64(batch))
					return nil
				})
			}
		})
		d := c.snap().sub(before)
		commits := ph.Ops - ph.Errors
		lat := ph.Latency
		r := s3Run{Kind: kind, Clients: clients, KeysPerTxn: batch, Commits: commits, CommitsPerSec: round(float64(commits)/ph.Seconds, 1),
			KeysPerSec: round(float64(commits*int64(batch))/ph.Seconds, 1), Latency: &lat, Errors: ph.Errors}
		if commits > 0 {
			r.PutsPerCommit = round(float64(d.Put)/float64(commits), 3)
			r.OtherPerCommit = round(float64(d.Get+d.List)/float64(commits), 3)
			r.CostPerMCommits = round(cost(d)/float64(commits)*1e6, 2)
			r.CostPerMKeys = round(cost(d)/float64(commits*int64(batch))*1e6, 2)
		}
		logf("s3 %s c=%d batch=%d: %.0f commits/s (%.0f keys/s) p50 %.1fms p99 %.1fms p99.9 %.1fms | %.3f PUT/commit | ~$%.2f per 1M commits",
			kind, clients, batch, r.CommitsPerSec, r.KeysPerSec, lat.P50Us/1000, lat.P99Us/1000, lat.P999Us/1000, r.PutsPerCommit, r.CostPerMCommits)
		return r
	}
	for _, b := range parseInts(*batchF) {
		res.Runs = append(res.Runs, measure("batch-size", 1, b))
	}
	for _, c := range parseInts(*clientsF) {
		res.Runs = append(res.Runs, measure("group-commit", c, 1))
	}
	for _, n := range parseInts(*segsF) {
		name := fmt.Sprintf("open-%d", n)
		c, db := open(name)
		// one client => one commit per segment, so the log has exactly n segments
		var made atomic.Int64
		runClosed(1, time.Hour, func(id int) opFunc {
			return func() error {
				if made.Add(1) > int64(n) {
					return errStop
				}
				return db.Update(bg, func(tx *freshdb.Tx) error { return tx.Put(fmt.Sprintf("k%d", id), val) })
			}
		})
		segs := countSegments(c, db)
		db.Close()
		var times []float64
		var openReq reqs
		for i := 0; i < 3; i++ {
			st, err := store.Open(withPrefix(*url, "bench-"+run+"/"+name))
			if err != nil {
				fatal(err)
			}
			cc := &counting{inner: st}
			t0 := time.Now()
			d, err := freshdb.OpenStore(bg, cc, freshdb.WithLogf(func(string, ...any) {}))
			if err != nil {
				fatal(err)
			}
			times = append(times, round(float64(time.Since(t0).Microseconds())/1000, 1))
			openReq = cc.snap()
			d.Close()
		}
		sorted := append([]float64(nil), times...)
		sort.Float64s(sorted)
		res.Runs = append(res.Runs, s3Run{Kind: "cold-open", Segments: segs, OpenMsMedian: sorted[1], OpenMsAll: times, OpenRequests: &openReq})
		logf("s3 cold open with %d segments: median %.0f ms %v, requests %+v", segs, sorted[1], times, openReq)
	}
	writeResult(*out, "s3", res)
}

var errStop = fmt.Errorf("stop")

// runClosed with an op that returns errStop: the bench loop ends when every
// client has seen errStop. Implemented by the caller's counter; the hour-long
// duration is never reached because each client returns quickly once done.
func init() {
	stopAware = func(err error) bool { return err == errStop }
}

func countSegments(c *counting, db *freshdb.DB) int {
	n := 0
	for id := range db.Manifest().Logs {
		segs, _ := c.inner.List(bg, "log/"+id+"/")
		n += len(segs)
	}
	return n
}

func withPrefix(u, p string) string {
	for i := 0; i < len(u); i++ {
		if u[i] == '?' {
			return trimSlash(u[:i]) + "/" + p + u[i:]
		}
	}
	return trimSlash(u) + "/" + p
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func redact(u string) string { return u } // credentials never appear in the URL; they come from env
