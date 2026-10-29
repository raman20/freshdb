package freshdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raman20/freshdb/store"
)

// s3Proxy sits between freshDB and an S3 endpoint. It counts requests by
// kind (the cost model) and, with probability Fault, injects one of:
// 503 SlowDown without forwarding, a connection reset without forwarding,
// or a lost response (forwarded and applied, then the connection is
// dropped before the reply: the client cannot know it succeeded).
type s3Proxy struct {
	target string // host:port
	srv    *httptest.Server
	mu     sync.Mutex
	rng    *rand.Rand
	Fault  float64
	// TruncateGets cuts every object GET body in half, keeping the
	// original Content-Length, then drops the connection.
	TruncateGets bool
	counts       map[string]int
}

func newS3Proxy(t *testing.T, target string, seed int64) *s3Proxy {
	p := &s3Proxy{target: target, rng: rand.New(rand.NewSource(seed)), counts: map[string]int{}}
	p.srv = httptest.NewServer(p)
	t.Cleanup(p.srv.Close)
	return p
}

func (p *s3Proxy) count(k string) { p.mu.Lock(); p.counts[k]++; p.mu.Unlock() }

func (p *s3Proxy) Counts() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]int{}
	for k, v := range p.counts {
		out[k] = v
	}
	return out
}

func (p *s3Proxy) Reset() { p.mu.Lock(); p.counts = map[string]int{}; p.mu.Unlock() }

// kind classifies an S3 call for the cost table.
func kind(r *http.Request) string {
	q := r.URL.Query()
	switch {
	case r.Method == "GET" && (q.Has("list-type") || q.Has("prefix") || q.Has("delimiter")):
		return "LIST"
	case r.Method == "GET" && q.Has("location"), r.Method == "HEAD":
		return "HEAD/meta"
	case r.Method == "GET":
		return "GET"
	case r.Method == "PUT":
		return "PUT"
	case r.Method == "DELETE":
		return "DELETE"
	}
	return r.Method
}

func (p *s3Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	k := kind(r)
	p.count(k)
	p.mu.Lock()
	x := p.rng.Float64()
	f := p.Fault
	p.mu.Unlock()
	drop := func() {
		if hj, ok := w.(http.Hijacker); ok {
			if c, _, err := hj.Hijack(); err == nil {
				c.Close()
			}
		}
	}
	switch {
	case x < f/3:
		p.count("fault: 503 SlowDown")
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code><Message>injected</Message></Error>`)
		return
	case x < 2*f/3:
		p.count("fault: reset before forwarding")
		drop()
		return
	}
	out, _ := http.NewRequestWithContext(r.Context(), r.Method, "http://"+p.target+r.URL.RequestURI(), bytes.NewReader(body))
	out.Header = r.Header.Clone()
	out.Host = r.Host // SigV4 signed the proxy's host
	out.ContentLength = int64(len(body))
	resp, err := http.DefaultTransport.RoundTrip(out)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusPreconditionFailed {
		p.count("412 (create refused)")
	}
	if x < f {
		p.count("fault: lost response (" + k + " applied)")
		io.Copy(io.Discard, resp.Body)
		drop()
		return
	}
	for h, v := range resp.Header {
		w.Header()[h] = v
	}
	w.WriteHeader(resp.StatusCode)
	if p.TruncateGets && k == "GET" && resp.StatusCode == http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		w.Write(b[:len(b)/2])
		w.(http.Flusher).Flush()
		drop()
		return
	}
	io.Copy(w, resp.Body)
}

// A GET whose body ends early is an error, never a shorter object.
func TestS3TruncatedGetIsAnError(t *testing.T) {
	raw, endpoint := s3Target(t)
	p := newS3Proxy(t, endpoint, 1)
	via, direct := p.proxied(raw, "trunc")
	open := func(raw string) store.Store {
		st, err := store.Open(raw)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	if err := open(direct).PutIfAbsent(ctx, "obj", bytes.Repeat([]byte("x"), 4096)); err != nil {
		t.Fatal(err)
	}
	p.TruncateGets = true
	b, err := open(via).Get(ctx, "obj")
	if !errors.Is(err, store.ErrShortRead) {
		t.Fatalf("truncated GET: %d bytes, %v; want ErrShortRead", len(b), err)
	}
	t.Logf("truncated GET: %v", err)
}

// proxied rewrites an s3:// URL to go through p, under a fresh prefix.
func (p *s3Proxy) proxied(raw, name string) (via, direct string) {
	u, _ := url.Parse(raw)
	u.Path += fmt.Sprintf("/%s-%d", name, time.Now().UnixNano())
	direct = u.String()
	q := u.Query()
	q.Set("endpoint", strings.TrimPrefix(p.srv.URL, "http://"))
	u.RawQuery = q.Encode()
	return u.String(), direct
}

func s3Target(t *testing.T) (raw, endpoint string) {
	raw = os.Getenv("FRESHDB_TEST_S3")
	if raw == "" {
		t.Skip("FRESHDB_TEST_S3 not set")
	}
	u, _ := url.Parse(raw)
	if u.Query().Get("insecure") == "" {
		t.Skip("the fault proxy speaks plain HTTP: needs an insecure=1 endpoint (local MinIO)")
	}
	return raw, u.Query().Get("endpoint")
}

// TestS3Faults: a workload through injected S3 faults (15% and 60% of requests:
// 503s, resets, lost responses of applied writes). There is one writer, so
// any ErrFenced is a false fence; after the run every acknowledged write
// must be readable directly from the bucket and Verify must pass.
func TestS3Faults(t *testing.T) {
	raw, endpoint := s3Target(t)
	// minio-go backs off between retries, so the heavy run is kept short.
	for _, c := range []struct {
		fault float64
		ops   int
	}{{0.15, 300}, {0.6, 60}} {
		t.Run(fmt.Sprintf("fault=%.2f", c.fault), func(t *testing.T) { s3Faults(t, raw, endpoint, c.fault, c.ops) })
	}
}

func s3Faults(t *testing.T, raw, endpoint string, fault float64, ops int) {
	p := newS3Proxy(t, endpoint, 1)
	via, direct := p.proxied(raw, "faults")
	type keyModel struct {
		vals  []string
		acked int
	}
	model := map[string]*keyModel{}
	open := func() *DB {
		for i := 0; ; i++ {
			db, err := Open(ctx, via, quiet())
			if err == nil {
				return db
			}
			if i == 50 {
				t.Fatalf("open through faults: %v", err)
			}
		}
	}
	p.Fault = fault
	db := open()
	var acked, unknown, reopens int
	for i := 0; i < ops; i++ {
		k, v := fmt.Sprint("k", i%40), fmt.Sprint("v", i)
		m := model[k]
		if m == nil {
			m = &keyModel{acked: -1}
			model[k] = m
		}
		m.vals = append(m.vals, v)
		err := db.Update(ctx, func(tx *Tx) error { return tx.Put(k, []byte(v)) })
		switch {
		case err == nil:
			m.acked = len(m.vals) - 1
			acked++
		case errors.Is(err, ErrFenced):
			t.Fatalf("op %d: false fence with a single writer: %v", i, err)
		default: // outcome unknown: the writer halts; reopen learns the outcome
			unknown++
			db.Close()
			db = open()
			reopens++
		}
		if i == ops/2 {
			if _, err := db.Compact(ctx, CompactOptions{}); errors.Is(err, ErrFenced) {
				t.Fatalf("compaction: false fence: %v", err)
			} else if err != nil {
				t.Logf("compaction under faults: %v (allowed)", err)
			}
		}
	}
	db.Close()
	p.Fault = 0
	d, err := Open(ctx, direct, quiet())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for k, m := range model {
		got, err := d.Get(ctx, k)
		ok := false
		for i := max(m.acked, 0); i < len(m.vals); i++ {
			ok = ok || err == nil && string(got) == m.vals[i]
		}
		if m.acked < 0 && errors.Is(err, ErrNotFound) {
			ok = true
		}
		if !ok {
			t.Errorf("%s = %q, %v; last acked %d of %v", k, got, err, m.acked, m.vals)
		}
	}
	if r, err := d.Verify(ctx); err != nil || !r.OK {
		t.Fatalf("verify: %v %+v", err, r)
	}
	c := p.Counts()
	var ks []string
	for k := range c {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for _, k := range ks {
		t.Logf("%-40s %d", k, c[k])
	}
	t.Logf("%d acked, %d unknown outcome (reopened %d times), 0 false fences", acked, unknown, reopens)
	if c["fault: lost response (PUT applied)"] == 0 || c["412 (create refused)"] == 0 {
		t.Fatal("no lost PUT response was retried into a 412: the retry path was not exercised")
	}
}

// TestS3Cost counts S3 requests per operation (no faults) and prices them
// at AWS S3 Standard us-east-1 list prices (PUT/LIST/DELETE-free: $0.005
// per 1,000 PUT/COPY/POST/LIST; $0.0004 per 1,000 GET/HEAD; DELETE free).
// Results: bench/results/2026-10-29-s3/.
func TestS3Cost(t *testing.T) {
	raw, endpoint := s3Target(t)
	p := newS3Proxy(t, endpoint, 2)
	via, _ := p.proxied(raw, "cost")
	price := func(c map[string]int) float64 {
		return float64(c["PUT"]+c["LIST"])*0.005/1000 + float64(c["GET"]+c["HEAD/meta"])*0.0004/1000
	}
	row := func(name string, n int, c map[string]int) {
		t.Logf("%-34s n=%-4d PUT %6.2f  GET %6.2f  LIST %5.2f  DELETE %5.2f  per op; $%.2f per million ops",
			name, n, float64(c["PUT"])/float64(n), float64(c["GET"]+c["HEAD/meta"])/float64(n),
			float64(c["LIST"])/float64(n), float64(c["DELETE"])/float64(n), price(c)/float64(n)*1e6)
	}
	measure := func(name string, n int, fn func()) {
		p.Reset()
		fn()
		row(name, n, p.Counts())
	}
	var db *DB
	measure("open (empty prefix)", 1, func() {
		var err error
		if db, err = Open(ctx, via, quiet()); err != nil {
			t.Fatal(err)
		}
	})
	put := func(i int) {
		if err := db.Update(ctx, func(tx *Tx) error { return tx.Put(fmt.Sprint("k", i%500), []byte(fmt.Sprint("v", i))) }); err != nil {
			t.Fatal(err)
		}
	}
	measure("commit, serial (1 writer)", 200, func() {
		for i := 0; i < 200; i++ {
			put(i)
		}
	})
	measure("commit, 32 concurrent writers", 3200, func() {
		var wg sync.WaitGroup
		for w := 0; w < 32; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					put(w*100 + i)
				}
			}(w)
		}
		wg.Wait()
	})
	measure("get (cached, after commits)", 500, func() {
		for i := 0; i < 500; i++ {
			db.Get(ctx, fmt.Sprint("k", i))
		}
	})
	db.Close()
	measure("open (after 3400 commits)", 1, func() {
		var err error
		if db, err = Open(ctx, via, quiet()); err != nil {
			t.Fatal(err)
		}
	})
	measure("get (cold, after reopen)", 500, func() {
		for i := 0; i < 500; i++ {
			db.Get(ctx, fmt.Sprint("k", i))
		}
	})
	measure("compaction", 1, func() {
		if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
			t.Fatal(err)
		}
	})
	measure("open (after compaction)", 1, func() {
		db.Close()
		var err error
		if db, err = Open(ctx, via, quiet()); err != nil {
			t.Fatal(err)
		}
	})
	measure("gc (keep 0)", 1, func() {
		if _, err := db.GC(ctx, GCOptions{}); err != nil {
			t.Fatal(err)
		}
	})
	db.Close()
}
