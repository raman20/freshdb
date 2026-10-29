// Command freshdb-soak is a real-time soak and chaos test of a freshdb
// server binary: mixed workload (puts, deletes, large values, atomic
// multi-key transactions, topic publishes) from several clients while the
// harness periodically kills the server with SIGKILL and restarts it, and
// compaction, GC and online backups run concurrently. After every restart,
// periodically while running, and on every backup it checks:
//
//   - every key's value is one written by an operation between the last
//     acknowledged one and the last attempted one (no lost acked write,
//     no value from nowhere, no resurrected older value);
//   - the keys of one transaction are always seen together (atomicity);
//   - each topic holds every acknowledged message, in order, no
//     duplicates, nothing never published;
//   - a restored backup satisfies the same bounds as of the backup.
//
// It stops at the first violation (exit 1, data directory kept).
//
//	freshdb-soak -bin ./freshdb -dir /tmp/soak -duration 6h
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/raman20/freshdb"
	"github.com/raman20/freshdb/store"
)

var (
	bin      = flag.String("bin", "freshdb", "freshdb binary")
	dir      = flag.String("dir", "", "work directory (required; kept on failure)")
	duration = flag.Duration("duration", time.Hour, "how long to run")
	workers  = flag.Int("workers", 8, "client goroutines")
	killMin  = flag.Duration("kill-min", 40*time.Second, "min time between kill -9")
	killMax  = flag.Duration("kill-max", 120*time.Second, "max time between kill -9")
	compactE = flag.Duration("compact-every", 20*time.Second, "compaction period")
	gcE      = flag.Duration("gc-every", 61*time.Second, "GC period")
	backupE  = flag.Duration("backup-every", 90*time.Second, "backup period")
	keys     = flag.Int("keys", 64, "keys per worker")
	seed     = flag.Int64("seed", time.Now().UnixNano(), "random seed")
)

const token = "soak-admin-token"

func main() {
	flag.Parse()
	if *dir == "" {
		log.Fatal("-dir is required")
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	h := &harness{db: filepath.Join(*dir, "db"), rng: rand.New(rand.NewSource(*seed))}
	os.MkdirAll(*dir, 0o755)
	log.Printf("soak: seed %d, %d workers, %v, dir %s", *seed, *workers, *duration, *dir)
	if err := h.run(); err != nil {
		log.Printf("SOAK FAIL: %v", err)
		h.stop()
		os.Exit(1)
	}
	h.stop()
	log.Printf("SOAK OK: %s", h.summary())
}

// ---- model ----

// keyModel: values written to a key, in order; nil = deleted. Index i is
// the i-th operation; acked is the last acknowledged index (-1: none; the
// key starts absent), tried the last attempted.
//
// vals holds ops [base, tried]; older ones can never be observed again
// and are dropped (prune), else the model grows with every op (a 6 h
// run was OOM-killed at 13 GB after 11 minutes before this).
type keyModel struct {
	vals         [][]byte
	base         int
	acked, tried int
}

// prune drops ops below floor (never below base).
func (k *keyModel) prune(floor int) {
	if floor <= k.base {
		return
	}
	k.vals = append([][]byte(nil), k.vals[floor-k.base:]...)
	k.base = floor
}

// allowed: the observed value v (nil = absent) is the value of some op in
// [acked, tried], or "absent" if acked == -1.
func (k *keyModel) allowed(v []byte, lo, hi int) bool {
	if lo < 0 && v == nil {
		return true
	}
	for i := max(lo, 0, k.base); i <= hi; i++ {
		if x := k.vals[i-k.base]; (x == nil) == (v == nil) && bytes.Equal(x, v) {
			return true
		}
	}
	return false
}

type topicModel struct {
	tried, acked int // next message n is tried+1; acked: highest acked n (-1)
	verified     int // messages 0..verified-1 checked (offsets)
	lastSeen     int // n of the last verified message (-1)
}

type worker struct {
	id    int
	rng   *rand.Rand
	mu    sync.Mutex // guards the model against checkers
	keys  map[string]*keyModel
	topic topicModel
	seq   int
	ops   atomic.Int64
	errs  atomic.Int64
	pairs int
	pin   map[string][2]int // lower bounds of a running backup check
}

// ---- harness ----

type harness struct {
	db       string
	rng      *rand.Rand
	addr     string
	cmd      *exec.Cmd
	srvMu    sync.RWMutex // write-locked while the server is down
	ws       []*worker
	kills    int
	checks   atomic.Int64
	backups  int
	restores int
	compacts atomic.Int64
	gcs      atomic.Int64
	failed   atomic.Pointer[error]
	start    time.Time
	maxRSS   int64
	client   http.Client
}

func (h *harness) fail(err error) {
	h.failed.CompareAndSwap(nil, &err)
}

func (h *harness) startServer() error {
	l, err := net.Listen("tcp", "127.0.0.1:0") // a free port
	if err != nil {
		return err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	h.addr = fmt.Sprintf("http://127.0.0.1:%d", port)
	lf, err := os.OpenFile(filepath.Join(*dir, "server.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	h.cmd = exec.Command(*bin, "serve", "--db", h.db, "--listen", fmt.Sprintf("127.0.0.1:%d", port), "--token", token, "--log-format", "json")
	h.cmd.Stdout, h.cmd.Stderr = lf, lf
	if err := h.cmd.Start(); err != nil {
		return err
	}
	go h.cmd.Wait()
	for i := 0; i < 300; i++ {
		if r, err := h.client.Get(h.addr + "/readyz"); err == nil {
			r.Body.Close()
			if r.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("server did not become ready in 30 s (see server.log)")
}

func (h *harness) stop() {
	if h.cmd != nil && h.cmd.Process != nil {
		h.cmd.Process.Signal(syscall.SIGTERM)
		time.Sleep(time.Second)
		h.cmd.Process.Kill()
	}
}

func (h *harness) do(method, path string, body []byte) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, method, h.addr+path, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	r, err := h.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	return r.StatusCode, b, err
}

func (h *harness) run() error {
	h.start = time.Now()
	if err := h.startServer(); err != nil {
		return err
	}
	for i := 0; i < *workers; i++ {
		w := &worker{id: i, rng: rand.New(rand.NewSource(*seed + int64(i) + 1)), keys: map[string]*keyModel{}, topic: topicModel{tried: -1, acked: -1, lastSeen: -1}}
		h.ws = append(h.ws, w)
	}
	end := time.Now().Add(*duration)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, w := range h.ws {
		wg.Add(1)
		go func() { defer wg.Done(); h.workLoop(w, stop) }()
	}
	bg := func(every time.Duration, f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					h.srvMu.RLock()
					f()
					h.srvMu.RUnlock()
				}
			}
		}()
	}
	bg(*compactE, func() {
		if c, b, err := h.do("POST", "/v1/compact?grace=10s", nil); err == nil && c == 200 {
			h.compacts.Add(1)
		} else if err == nil && c != 503 && c != 409 {
			h.fail(fmt.Errorf("compact: %d %s", c, b))
		}
	})
	bg(*gcE, func() {
		if c, b, err := h.do("POST", "/v1/gc?grace=10s&keep-for=30s", nil); err == nil && c == 200 {
			h.gcs.Add(1)
		} else if err == nil && c != 503 && c != 409 {
			h.fail(fmt.Errorf("gc: %d %s", c, b))
		}
	})
	nextKill := time.Now().Add(h.killGap())
	nextBackup := time.Now().Add(*backupE)
	nextStatus := time.Now().Add(30 * time.Second)
	var err error
	for err == nil && time.Now().Before(end) {
		time.Sleep(200 * time.Millisecond)
		if f := h.failed.Load(); f != nil {
			err = *f
			break
		}
		switch {
		case time.Now().After(nextKill):
			err = h.killRestart()
			nextKill = time.Now().Add(h.killGap())
		case time.Now().After(nextBackup):
			err = h.backupCheck()
			nextBackup = time.Now().Add(*backupE)
		case time.Now().After(nextStatus):
			h.status()
			nextStatus = time.Now().Add(30 * time.Second)
		}
	}
	close(stop)
	wg.Wait()
	if err == nil {
		if f := h.failed.Load(); f != nil {
			err = *f
		}
	}
	if err == nil { // final: one more kill + full check
		err = h.killRestart()
	}
	h.status()
	return err
}

func (h *harness) killGap() time.Duration {
	return *killMin + time.Duration(h.rng.Int63n(int64(*killMax-*killMin)+1))
}

func (h *harness) summary() string {
	var ops, errs int64
	for _, w := range h.ws {
		ops += w.ops.Load()
		errs += w.errs.Load()
	}
	return fmt.Sprintf("%v elapsed, %d ops (%.0f/s), %d unknown-outcome/errors, %d kill -9 restarts, %d full checks, %d compactions, %d GCs, %d backups (%d restored+checked), max server RSS %d MiB",
		time.Since(h.start).Round(time.Second), ops, float64(ops)/time.Since(h.start).Seconds(), errs, h.kills, h.checks.Load(), h.compacts.Load(), h.gcs.Load(), h.backups, h.restores, h.maxRSS>>20)
}

func (h *harness) status() {
	rss := int64(0)
	if h.cmd != nil && h.cmd.Process != nil {
		if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", h.cmd.Process.Pid)); err == nil {
			for _, l := range strings.Split(string(b), "\n") {
				if f, ok := strings.CutPrefix(l, "VmRSS:"); ok {
					kb, _ := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(f), "kB")), 10, 64)
					rss = kb << 10
				}
			}
		}
	}
	h.maxRSS = max(h.maxRSS, rss)
	p99 := ""
	if c, b, err := h.do("GET", "/metrics", nil); err == nil && c == 200 {
		p99 = quantile(string(b), "freshdb_commit_latency_seconds", 0.99)
	}
	var du int64
	filepath.Walk(h.db, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			if st, ok := fi.Sys().(*syscall.Stat_t); ok {
				du += st.Blocks * 512
			}
		}
		return nil
	})
	log.Printf("status: %s; server RSS %d MiB, disk %d MiB, commit p99 (since start) %s", h.summary(), rss>>20, du>>20, p99)
}

// quantile reads a histogram from /metrics text.
func quantile(text, name string, q float64) string {
	var les []string
	var cum []float64
	for _, l := range strings.Split(text, "\n") {
		if rest, ok := strings.CutPrefix(l, name+`_bucket{le="`); ok {
			le, v, _ := strings.Cut(rest, `"} `)
			n, _ := strconv.ParseFloat(v, 64)
			les, cum = append(les, le), append(cum, n)
		}
	}
	if len(cum) == 0 || cum[len(cum)-1] == 0 {
		return "n/a"
	}
	for i, c := range cum {
		if c >= q*cum[len(cum)-1] {
			return "≤" + les[i] + "s"
		}
	}
	return "?"
}

// killRestart: SIGKILL the server, restart it, check everything.
func (h *harness) killRestart() error {
	h.cmd.Process.Kill() // with requests in flight: unknown outcomes
	h.srvMu.Lock()
	defer h.srvMu.Unlock()
	time.Sleep(50 * time.Millisecond)
	h.kills++
	if err := h.startServer(); err != nil {
		return err
	}
	return h.checkAll(h.readLive, "after kill -9 #"+strconv.Itoa(h.kills))
}

// reader returns a key's value (nil if absent) and a topic's messages
// from an offset.
type reader struct {
	get   func(key string) ([]byte, error)
	scan  func(prefix string) (map[string][]byte, error)
	topic func(t string, from int) ([]string, error)
}

func (h *harness) readLive() reader {
	return reader{
		get: func(k string) ([]byte, error) {
			c, b, err := h.do("GET", "/v1/kv/"+url.PathEscape(k), nil)
			switch {
			case err != nil:
				return nil, err
			case c == 404:
				return nil, nil
			case c != 200:
				return nil, fmt.Errorf("get %s: %d %s", k, c, b)
			}
			return b, nil
		},
		scan: func(p string) (map[string][]byte, error) {
			c, b, err := h.do("GET", "/v1/kv?prefix="+url.QueryEscape(p), nil)
			if err != nil || c != 200 {
				return nil, fmt.Errorf("scan %s: %d %s %v", p, c, b, err)
			}
			var kvs []struct{ Key, Value string }
			if err := json.Unmarshal(b, &kvs); err != nil {
				return nil, err
			}
			out := map[string][]byte{}
			for _, kv := range kvs {
				out[kv.Key] = []byte(kv.Value)
			}
			return out, nil
		},
		topic: func(t string, from int) ([]string, error) {
			c, b, err := h.do("GET", fmt.Sprintf("/v1/topics/%s?from=%d&max=100000", url.PathEscape(t), from), nil)
			if err != nil || c != 200 {
				return nil, fmt.Errorf("topic %s: %d %s %v", t, c, b, err)
			}
			var ms []struct{ Value string }
			if err := json.Unmarshal(b, &ms); err != nil {
				return nil, err
			}
			out := make([]string, len(ms))
			for i, m := range ms {
				out[i] = m.Value
			}
			return out, nil
		},
	}
}

func readDB(db *freshdb.DB) reader {
	ctx := context.Background()
	return reader{
		get: func(k string) ([]byte, error) {
			v, err := db.Get(ctx, k)
			if errors.Is(err, freshdb.ErrNotFound) {
				return nil, nil
			}
			return v, err
		},
		scan: func(p string) (map[string][]byte, error) {
			out := map[string][]byte{}
			err := db.View(ctx, func(tx *freshdb.Tx) error {
				kvs, err := tx.Scan(p)
				for _, kv := range kvs {
					out[kv.Key] = kv.Value
				}
				return err
			})
			return out, err
		},
		topic: func(t string, from int) ([]string, error) {
			var out []string
			err := db.View(ctx, func(tx *freshdb.Tx) error {
				ms, err := tx.Topic(t).Read(uint64(from), 1<<30)
				for _, m := range ms {
					out = append(out, string(m.Value))
				}
				return err
			})
			return out, err
		},
	}
}

// bounds is a copy of the model's [acked, tried] per key and topic.
type bounds struct {
	keys   map[string][2]int
	topics map[int][2]int // worker -> acked, tried
}

func (h *harness) snapshotBounds() bounds {
	b := bounds{keys: map[string][2]int{}, topics: map[int][2]int{}}
	for _, w := range h.ws {
		w.mu.Lock()
		for k, m := range w.keys {
			b.keys[k] = [2]int{m.acked, m.tried}
		}
		b.topics[w.id] = [2]int{w.topic.acked, w.topic.tried}
		w.mu.Unlock()
	}
	return b
}

// checkAll checks every key and topic. Workers are idle (server lock held
// exclusively), so the model is stable.
func (h *harness) checkAll(mk func() reader, when string) error {
	b := h.snapshotBounds()
	return h.check(mk(), b, b, when, true)
}

// check: values within [lo.acked, hi.tried]; pairs atomic; topics.
func (h *harness) check(r reader, lo, hi bounds, when string, live bool) error {
	h.checks.Add(1)
	for _, w := range h.ws {
		w.mu.Lock()
		err := h.checkWorker(r, w, lo, hi, when, live)
		w.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *harness) checkWorker(r reader, w *worker, lo, hi bounds, when string, live bool) error {
	all, err := r.scan(fmt.Sprintf("w%d/", w.id))
	if err != nil {
		return err
	}
	for k, m := range w.keys {
		l, okl := lo.keys[k]
		u := hi.keys[k]
		if !okl {
			l = [2]int{-1, -1}
		}
		v, present := all[k]
		if !present {
			v = nil
		}
		if !m.allowed(v, l[0], u[1]) {
			return fmt.Errorf("%s: key %s = %.40q, allowed ops [%d, %d] (last acked %.40q)", when, k, v, l[0], u[1], at(m, l[0]))
		}
	}
	// atomicity: pair keys written together always agree
	for i := 0; i < w.pairs; i++ {
		a, b := all[fmt.Sprintf("w%d/pair%d/a", w.id, i)], all[fmt.Sprintf("w%d/pair%d/b", w.id, i)]
		if !bytes.Equal(a, b) {
			return fmt.Errorf("%s: atomicity: w%d pair%d a=%q b=%q", when, w.id, i, a, b)
		}
	}
	for k := range all {
		if _, ok := w.keys[k]; !ok {
			return fmt.Errorf("%s: key %s exists but was never written", when, k)
		}
	}
	// topic: values "n" strictly increasing by 1 from 0; at least acked+1
	// messages, at most tried+1.
	from := 0
	last := -1
	if live {
		from, last = w.topic.verified, w.topic.lastSeen
	}
	ms, err := r.topic(fmt.Sprintf("t%d", w.id), from)
	if err != nil {
		return err
	}
	for _, s := range ms {
		n, err := strconv.Atoi(s)
		if err != nil || n <= last {
			return fmt.Errorf("%s: topic t%d: message %q after %d (duplicate or out of order)", when, w.id, s, last)
		}
		last = n
	}
	l, u := lo.topics[w.id], hi.topics[w.id]
	if last < l[0] || last > u[1] {
		return fmt.Errorf("%s: topic t%d: last message %d, acked %d, tried %d", when, w.id, last, l[0], u[1])
	}
	if live {
		w.topic.verified += len(ms)
		w.topic.lastSeen = last
	}
	return nil
}

func at(m *keyModel, i int) []byte {
	if i < 0 {
		return nil
	}
	if i < m.base {
		return []byte("(pruned)")
	}
	return m.vals[i-m.base]
}

// pin stops pruning below the given lower bounds (nil: unpin).
func (h *harness) pin(p map[string][2]int) {
	for _, w := range h.ws {
		w.mu.Lock()
		w.pin = p
		w.mu.Unlock()
	}
}

// backupCheck: online backup, then restore and check against the model
// bounds from before (acked) and after (tried) the backup.
func (h *harness) backupCheck() error {
	h.srvMu.RLock()
	lo := h.snapshotBounds()
	h.pin(lo.keys)
	defer h.pin(nil)
	dst := filepath.Join(*dir, fmt.Sprintf("backup%d", h.backups%2))
	c, b, err := h.do("POST", "/v1/backup?to="+url.QueryEscape(dst), nil)
	hi := h.snapshotBounds()
	h.srvMu.RUnlock()
	if err != nil || c != 200 {
		if err == nil && c != 503 {
			return fmt.Errorf("backup: %d %s", c, b)
		}
		return nil // server down or fenced mid-backup: skip
	}
	h.backups++
	ctx := context.Background()
	src, err := store.Open(dst)
	if err != nil {
		return err
	}
	rdir := filepath.Join(*dir, "restored")
	os.RemoveAll(rdir)
	rst, err := store.Open(rdir)
	if err != nil {
		return err
	}
	_, err = freshdb.Restore(ctx, src, rst)
	closeStore(src)
	if err != nil {
		closeStore(rst)
		return fmt.Errorf("restore of backup %d: %w", h.backups, err)
	}
	db, err := freshdb.OpenStore(ctx, rst, freshdb.WithLogf(func(string, ...any) {}))
	if err != nil {
		closeStore(rst)
		return fmt.Errorf("open restored backup %d: %w", h.backups, err)
	}
	defer closeStore(rst)
	defer db.Close()
	if vr, err := db.Verify(ctx); err != nil || !vr.OK {
		return fmt.Errorf("verify restored backup %d: %v %+v", h.backups, err, vr)
	}
	h.restores++
	return h.check(readDB(db), lo, hi, fmt.Sprintf("restored backup %d", h.backups), false)
}

func closeStore(s store.Store) {
	if c, ok := s.(io.Closer); ok {
		c.Close()
	}
}

// ---- workload ----

func (h *harness) workLoop(w *worker, stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		h.srvMu.RLock()
		err := h.step(w)
		h.srvMu.RUnlock()
		if err != nil {
			h.fail(err)
			return
		}
	}
}

func (w *worker) next(prefix string) string {
	w.seq++
	return fmt.Sprintf("%s:%d", prefix, w.seq)
}

func (w *worker) key(k string) *keyModel {
	m := w.keys[k]
	if m == nil {
		m = &keyModel{acked: -1, tried: -1}
		w.keys[k] = m
	}
	return m
}

// record an attempt: returns the op indexes per key.
func (w *worker) attempt(kv map[string][]byte) map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	idx := map[string]int{}
	for k, v := range kv {
		m := w.key(k)
		m.vals = append(m.vals, v)
		m.tried = m.base + len(m.vals) - 1
		idx[k] = m.tried
	}
	return idx
}

func (w *worker) ack(idx map[string]int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for k, i := range idx {
		m := w.keys[k]
		if i <= m.acked {
			continue
		}
		m.acked = i
		floor := i
		if w.pin != nil { // a backup check still needs ops from its start
			lo, ok := w.pin[k]
			floor = min(floor, lo[0])
			if !ok {
				floor = -1
			}
		}
		m.prune(floor)
	}
}

func (h *harness) step(w *worker) error {
	w.ops.Add(1)
	r := w.rng.Intn(100)
	key := fmt.Sprintf("w%d/k%d", w.id, w.rng.Intn(*keys))
	switch {
	case r < 45: // put
		v := []byte(w.next(key))
		if w.rng.Intn(50) == 0 { // large value (chunked)
			v = append(v, bytes.Repeat([]byte{'x'}, 64<<10+w.rng.Intn(256<<10))...)
		}
		idx := w.attempt(map[string][]byte{key: v})
		c, b, err := h.do("PUT", "/v1/kv/"+key, v)
		return h.settle(w, idx, c, b, err)
	case r < 52: // delete
		idx := w.attempt(map[string][]byte{key: nil})
		c, b, err := h.do("DELETE", "/v1/kv/"+key, nil)
		if c == 404 {
			c = 200
		}
		return h.settle(w, idx, c, b, err)
	case r < 67: // atomic pair + publish
		w.mu.Lock()
		p := w.rng.Intn(16)
		if p >= w.pairs {
			p = w.pairs
			w.pairs++
		}
		w.mu.Unlock()
		v := w.next("pair")
		a, b := fmt.Sprintf("w%d/pair%d/a", w.id, p), fmt.Sprintf("w%d/pair%d/b", w.id, p)
		w.mu.Lock()
		n := w.topic.tried + 1
		w.topic.tried = n
		w.mu.Unlock()
		idx := w.attempt(map[string][]byte{a: []byte(v), b: []byte(v)})
		ops, _ := json.Marshal([]map[string]any{
			{"op": "put", "key": a, "value": v}, {"op": "put", "key": b, "value": v},
			{"op": "pub", "topic": fmt.Sprintf("t%d", w.id), "value": strconv.Itoa(n)},
		})
		c, body, err := h.do("POST", "/v1/tx", ops)
		if err == nil && c == 200 {
			w.mu.Lock()
			w.topic.acked = n
			w.mu.Unlock()
		}
		return h.settle(w, idx, c, body, err)
	case r < 72: // publish alone
		w.mu.Lock()
		n := w.topic.tried + 1
		w.topic.tried = n
		w.mu.Unlock()
		c, b, err := h.do("POST", fmt.Sprintf("/v1/topics/t%d", w.id), []byte(strconv.Itoa(n)))
		if err == nil && c == 200 {
			w.mu.Lock()
			w.topic.acked = n
			w.mu.Unlock()
		}
		return h.settle(w, nil, c, b, err)
	default: // read-your-acked-writes
		w.mu.Lock()
		m := w.keys[key]
		lo, hi := -1, -1
		if m != nil {
			lo, hi = m.acked, m.tried
		}
		w.mu.Unlock()
		c, b, err := h.do("GET", "/v1/kv/"+key, nil)
		if err != nil || c >= 500 {
			w.errs.Add(1)
			return nil
		}
		var v []byte
		switch c {
		case 200:
			v = b
		case 404:
		default:
			return fmt.Errorf("get %s: %d %s", key, c, b)
		}
		if m == nil && v != nil || m != nil && !m.allowed(v, lo, hi) {
			return fmt.Errorf("live read: %s = %.40q, allowed ops [%d, %d]", key, v, lo, hi)
		}
		return nil
	}
}

// settle: 200 acks; a transport error or 5xx/503 is an unknown outcome
// (the op may or may not have applied: the model allows both); anything
// else is a bug.
func (h *harness) settle(w *worker, idx map[string]int, c int, b []byte, err error) error {
	switch {
	case err == nil && c == 200:
		w.ack(idx)
	case err != nil || c >= 500:
		w.errs.Add(1)
		time.Sleep(20 * time.Millisecond)
	default:
		return fmt.Errorf("unexpected HTTP %d: %s", c, b)
	}
	return nil
}
