package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/pprof"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raman20/freshdb"
)

// observer holds the HTTP metrics and logging settings.
type observer struct {
	start        time.Time
	version      string
	mu           sync.Mutex
	routes       map[string]*routeStats // by pattern; filled at registration
	inflight     atomic.Int64
	unauthorized atomic.Uint64
	slow         time.Duration // <= 0: off
	access       bool
	log          *slog.Logger

	readyMu  sync.Mutex
	readyAt  time.Time
	readyErr error
}

// routeStats are one route's counters: requests by status code (an array,
// so counting allocates nothing) and latency.
type routeStats struct {
	codes [600]atomic.Uint64 // index = status code; others counted at 0
	lat   freshdb.LatencyHistogram
}

func newObserver(o Options) *observer {
	ob := &observer{routes: map[string]*routeStats{}, start: time.Now(), version: o.Version, slow: o.SlowOp, access: o.AccessLog, log: o.Logger}
	if ob.slow == 0 {
		ob.slow = DefaultSlowOp
	}
	if ob.log == nil {
		ob.log = slog.Default()
	}
	return ob
}

// DefaultSlowOp is the default Options.SlowOp.
const DefaultSlowOp = time.Second

// statusWriter records the status code and body size. It forwards Flush
// (server-sent events) and Unwrap (http.ResponseController).
type statusWriter struct {
	http.ResponseWriter
	code int
	n    int64
}

func (w *statusWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.n += int64(n)
	return n, err
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// wrap instruments one route. Streams and long-polls (?wait=) are counted
// but kept out of the latency histogram and the slow-op log: their
// duration is the client's choice.
func (ob *observer) wrap(route string, f http.HandlerFunc) http.HandlerFunc {
	long := strings.HasSuffix(route, "/stream")
	ob.mu.Lock()
	rs := ob.routes[route]
	if rs == nil {
		rs = new(routeStats)
		ob.routes[route] = rs
	}
	ob.mu.Unlock()
	return func(w http.ResponseWriter, r *http.Request) {
		ob.inflight.Add(1)
		sw := &statusWriter{ResponseWriter: w}
		t0 := time.Now()
		defer func() {
			d := time.Since(t0)
			ob.inflight.Add(-1)
			code := sw.code
			if code == 0 {
				code = http.StatusOK
			}
			if code < 0 || code >= len(rs.codes) {
				code = 0
			}
			rs.codes[code].Add(1)
			waits := long || strings.Contains(r.URL.RawQuery, "wait=") && r.URL.Query().Get("wait") != ""
			if !waits {
				rs.lat.Observe(d)
			}
			slow := !waits && ob.slow > 0 && d >= ob.slow
			if ob.access || slow {
				msg, lvl := "request", slog.LevelInfo
				if slow {
					msg, lvl = "slow request", slog.LevelWarn
				}
				ob.log.LogAttrs(r.Context(), lvl, msg,
					slog.String("method", r.Method), slog.String("path", r.URL.Path),
					slog.String("route", route), slog.Int("status", code),
					slog.Int64("bytes", sw.n), slog.Float64("seconds", d.Seconds()),
					slog.String("remote", r.RemoteAddr), slog.String("token", tokenName(r)))
			}
		}()
		f(sw, r)
	}
}

// tokenName is the caller's token name for logs ("" without auth; never
// the token itself).
func tokenName(r *http.Request) string {
	c, _ := r.Context().Value(authKey{}).(*caller)
	if c == nil {
		return ""
	}
	if c.admin != nil {
		return "admin"
	}
	if g := c.grant(); g != nil {
		return g.Name
	}
	return ""
}

// ready is DB.Ping, cached for a second so probes cannot load the store.
func (ob *observer) ready(ctx context.Context, db *freshdb.DB) error {
	ob.readyMu.Lock()
	defer ob.readyMu.Unlock()
	if time.Since(ob.readyAt) < time.Second {
		return ob.readyErr
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ob.readyErr, ob.readyAt = db.Ping(ctx), time.Now()
	return ob.readyErr
}

// pprofRoutes are the net/http/pprof handlers (registered with admin scope).
func pprofRoutes(h func(string, http.HandlerFunc)) {
	h("GET /debug/pprof/{p...}", pprof.Index)
	h("GET /debug/pprof/cmdline", pprof.Cmdline)
	h("GET /debug/pprof/profile", pprof.Profile)
	h("GET /debug/pprof/symbol", pprof.Symbol)
	h("GET /debug/pprof/trace", pprof.Trace)
}

// ---- Prometheus text exposition (format 0.0.4), written by hand to
// avoid a client-library dependency. ----

type promWriter struct {
	w    io.Writer
	seen map[string]bool
}

func (p *promWriter) head(name, typ, help string) {
	if p.seen[name] {
		return
	}
	p.seen[name] = true
	fmt.Fprintf(p.w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func labels(kv ...string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(kv[i])
		b.WriteString(`="`)
		b.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(kv[i+1]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func num(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func (p *promWriter) counter(name, help string, v uint64, kv ...string) {
	p.head(name, "counter", help)
	fmt.Fprintf(p.w, "%s%s %d\n", name, labels(kv...), v)
}

func (p *promWriter) gauge(name, help string, v float64, kv ...string) {
	p.head(name, "gauge", help)
	fmt.Fprintf(p.w, "%s%s %s\n", name, labels(kv...), num(v))
}

func (p *promWriter) histogram(name, help string, h freshdb.Histogram, kv ...string) {
	p.head(name, "histogram", help)
	var cum uint64
	for i, c := range h.Counts {
		cum += c
		le := math.Inf(1)
		if i < len(freshdb.LatencyBounds) {
			le = freshdb.LatencyBounds[i]
		}
		fmt.Fprintf(p.w, "%s_bucket%s %d\n", name, labels(append(append([]string{}, kv...), "le", num(le))...), cum)
	}
	fmt.Fprintf(p.w, "%s_sum%s %s\n", name, labels(kv...), num(h.Sum))
	fmt.Fprintf(p.w, "%s_count%s %d\n", name, labels(kv...), h.Count)
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var b strings.Builder
	p := &promWriter{w: &b, seen: map[string]bool{}}
	ob := s.obs
	st := s.DB.Stats()

	p.gauge("freshdb_build_info", "Build information.", 1, "version", ob.version, "goversion", runtime.Version())
	p.gauge("freshdb_start_time_seconds", "When the server started (unix seconds).", float64(ob.start.UnixNano())/1e9)

	p.counter("freshdb_commits_total", "Transactions committed.", st.Commits)
	p.counter("freshdb_commit_errors_total", "Commit attempts that failed for a reason other than a conflict.", st.CommitErrors)
	p.counter("freshdb_conflicts_total", "Commit attempts that lost a serializability conflict.", st.Conflicts)
	p.histogram("freshdb_commit_latency_seconds", "Commit latency: submit to acknowledgement.", st.CommitLatency)
	p.counter("freshdb_segments_total", "Group commits (log segments) written.", st.Segments)
	p.counter("freshdb_segment_bytes_total", "Bytes of log segments written.", st.SegmentBytes)
	p.histogram("freshdb_segment_write_seconds", "Store write (with fsync) of one group commit.", st.SegmentWrite)
	p.counter("freshdb_compactions_total", "Compaction runs.", st.Compactions, "kind", "compact")
	p.counter("freshdb_compactions_total", "Compaction runs.", st.GCs, "kind", "gc")
	p.counter("freshdb_compaction_errors_total", "Failed compaction runs.", st.CompactionErrors, "kind", "compact")
	p.counter("freshdb_compaction_errors_total", "Failed compaction runs.", st.GCErrors, "kind", "gc")
	p.histogram("freshdb_compaction_pause_seconds", "Commit pause per compaction or GC run.", st.CompactionPause)
	p.counter("freshdb_snapshots_total", "KV snapshots written.", st.Snapshots)
	p.counter("freshdb_snapshot_errors_total", "KV snapshots that failed (non-fatal).", st.SnapshotErrors)
	p.counter("freshdb_value_cache_hits_total", "Value cache hits.", st.ValueCacheHits)
	p.counter("freshdb_value_cache_misses_total", "Value cache misses.", st.ValueCacheMisses)
	p.gauge("freshdb_value_cache_capacity_bytes", "Value cache capacity.", float64(st.ValueCacheBytes))
	p.gauge("freshdb_branches", "Number of branches.", float64(len(st.Heads)))
	if lsn, ok := st.Heads["main"]; ok {
		p.gauge("freshdb_head_lsn", "Head LSN of the main branch.", float64(lsn), "branch", "main")
	}
	halted := 0.0
	if st.Halted != "" {
		halted = 1
	}
	p.gauge("freshdb_writer_halted", "1 if a writer halted on a storage error (reopen needed).", halted)

	ob.mu.Lock()
	routes := make([]string, 0, len(ob.routes))
	stats := make(map[string]*routeStats, len(ob.routes))
	for rt, rs := range ob.routes {
		routes = append(routes, rt)
		stats[rt] = rs
	}
	ob.mu.Unlock()
	sort.Strings(routes)
	p.head("freshdb_http_requests_total", "counter", "HTTP requests by route and status.")
	for _, rt := range routes {
		rs := stats[rt]
		for code := range rs.codes {
			if v := rs.codes[code].Load(); v > 0 {
				p.counter("freshdb_http_requests_total", "", v, "route", rt, "code", strconv.Itoa(code))
			}
		}
	}
	p.head("freshdb_http_request_duration_seconds", "histogram", "HTTP request latency by route (streams and long-polls excluded).")
	for _, rt := range routes {
		if h := stats[rt].lat.Snapshot(); h.Count > 0 {
			p.histogram("freshdb_http_request_duration_seconds", "", h, "route", rt)
		}
	}
	p.counter("freshdb_http_unauthorized_total", "Requests refused for a missing or unknown token.", ob.unauthorized.Load())
	p.gauge("freshdb_http_inflight", "HTTP requests in progress (including streams).", float64(ob.inflight.Load()))
	if s.streams != nil {
		p.gauge("freshdb_http_streams", "Open server-sent-event streams.", float64(len(s.streams)))
	}
	if s.waiters != nil {
		p.gauge("freshdb_http_waiters", "Long-polls in progress.", float64(len(s.waiters)))
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	p.gauge("go_goroutines", "Goroutines.", float64(runtime.NumGoroutine()))
	p.gauge("go_memstats_heap_alloc_bytes", "Heap bytes allocated and in use.", float64(ms.HeapAlloc))
	p.gauge("go_memstats_heap_sys_bytes", "Heap bytes obtained from the OS.", float64(ms.HeapSys))
	p.gauge("go_memstats_sys_bytes", "Bytes obtained from the OS.", float64(ms.Sys))
	p.counter("go_gc_cycles_total", "Completed GC cycles.", uint64(ms.NumGC))
	p.gauge("go_gc_pause_total_seconds", "Total GC stop-the-world pause.", float64(ms.PauseTotalNs)/1e9)
	io.WriteString(w, b.String())
}
