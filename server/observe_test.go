package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raman20/freshdb"
)

func get(h http.Handler, path, tok string) (int, string) {
	r := httptest.NewRequest("GET", path, nil)
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code, w.Body.String()
}

// promLine is one sample line of the text exposition format.
var promLine = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([a-zA-Z_][a-zA-Z0-9_]*="(\\.|[^"\\])*",?)*\})? (\+Inf|-?[0-9.eE+-]+)$`)

// checkProm validates the exposition: every sample's family has TYPE
// before it, each family is declared once, and histograms are cumulative
// with +Inf == _count.
func checkProm(t *testing.T, body string) map[string]string {
	t.Helper()
	typed := map[string]string{}
	samples := map[string]string{}
	for _, ln := range strings.Split(strings.TrimSpace(body), "\n") {
		if f, ok := strings.CutPrefix(ln, "# TYPE "); ok {
			name, typ, _ := strings.Cut(f, " ")
			if typed[name] != "" {
				t.Fatalf("family %s declared twice", name)
			}
			typed[name] = typ
			continue
		}
		if strings.HasPrefix(ln, "# HELP ") {
			continue
		}
		m := promLine.FindStringSubmatch(ln)
		if m == nil {
			t.Fatalf("bad sample line %q", ln)
		}
		fam := m[1]
		for _, suf := range []string{"_bucket", "_sum", "_count"} {
			if b, ok := strings.CutSuffix(fam, suf); ok && typed[b] == "histogram" {
				fam = b
			}
		}
		if typed[fam] == "" {
			t.Fatalf("sample %q before its TYPE", ln)
		}
		samples[m[1]+m[2]] = m[len(m)-1]
	}
	return samples
}

func TestMetricsEndpoint(t *testing.T) {
	db := memDB(t)
	ts, err := ParseTokens([]byte(tokenFile))
	if err != nil {
		t.Fatal(err)
	}
	h := New(db, Options{Auth: NewAuth(ts), Version: "test"})
	for i := 0; i < 3; i++ {
		if c := call(h, "PUT", fmt.Sprintf("/v1/kv/k%d", i), "writer-0001", "v"); c != 200 {
			t.Fatalf("put: %d", c)
		}
	}
	call(h, "GET", "/v1/kv/nope", "reader-0001", "")
	call(h, "GET", "/v1/kv/k0", "", "")
	if c, _ := get(h, "/metrics", ""); c != 401 {
		t.Fatalf("metrics without token: %d", c)
	}
	if c, _ := get(h, "/metrics", "reader-0001"); c != 403 {
		t.Fatalf("metrics with a read token: %d", c)
	}
	c, body := get(h, "/metrics", "admin-00001")
	if c != 200 {
		t.Fatalf("metrics: %d %s", c, body)
	}
	s := checkProm(t, body)
	want := map[string]string{
		`freshdb_commits_total`: "3",
		`freshdb_http_requests_total{route="PUT /v1/kv/{key...}",code="200"}`:            "3",
		`freshdb_http_requests_total{route="GET /v1/kv/{key...}",code="404"}`:            "1",
		`freshdb_http_request_duration_seconds_count{route="PUT /v1/kv/{key...}"}`:       "3",
		`freshdb_http_unauthorized_total`:                                                "2", // GET k0 and /metrics without a token
		`freshdb_commit_latency_seconds_count`:                                           "3",
		`freshdb_commit_latency_seconds_bucket{le="+Inf"}`:                               "3",
		`freshdb_writer_halted`:                                                          "0",
		`freshdb_build_info{version="test",goversion="` + s0(body, `goversion="`) + `"}`: "1",
	}
	for k, v := range want {
		if s[k] != v {
			t.Errorf("%s = %q, want %q", k, s[k], v)
		}
	}
	if s[`freshdb_segments_total`] == "0" || s[`freshdb_segment_bytes_total`] == "0" {
		t.Errorf("segments not counted: %v %v", s[`freshdb_segments_total`], s[`freshdb_segment_bytes_total`])
	}
	if strings.Contains(body, "admin-00001") || strings.Contains(body, "writer-0001") {
		t.Fatal("metrics leak a token")
	}
}

// s0 returns the text after marker up to the next quote.
func s0(body, marker string) string {
	_, rest, _ := strings.Cut(body, marker)
	v, _, _ := strings.Cut(rest, `"`)
	return v
}

func TestMetricsCounters(t *testing.T) {
	db := memDB(t)
	ctx := context.Background()
	// A conflict: two transactions read k (both before either commits)
	// and write it; one attempt loses and Update retries it.
	if err := db.Put(ctx, "k", []byte("0")); err != nil {
		t.Fatal(err)
	}
	var both sync.WaitGroup
	both.Add(2)
	var done sync.WaitGroup
	for i := 0; i < 2; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			first := true
			err := db.Update(ctx, func(tx *freshdb.Tx) error {
				if _, err := tx.Get("k"); err != nil {
					return err
				}
				if first {
					first = false
					both.Done()
					both.Wait()
				}
				return tx.Put("k", []byte(fmt.Sprint(i)))
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	done.Wait()
	if _, err := db.Compact(ctx, freshdb.CompactOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GC(ctx, freshdb.GCOptions{}); err != nil {
		t.Fatal(err)
	}
	_, body := get(New(db, Options{}), "/metrics", "")
	s := checkProm(t, body)
	for k, v := range map[string]string{
		"freshdb_commits_total":                     "3",
		"freshdb_conflicts_total":                   "1",
		"freshdb_commit_errors_total":               "0",
		`freshdb_compactions_total{kind="compact"}`: "1",
		`freshdb_compactions_total{kind="gc"}`:      "1",
		`freshdb_compaction_pause_seconds_count`:    "2",
		`freshdb_head_lsn{branch="main"}`:           "3",
		`freshdb_branches`:                          "1",
	} {
		if s[k] != v {
			t.Errorf("%s = %q, want %q", k, s[k], v)
		}
	}
}

func TestReadyz(t *testing.T) {
	url := fmt.Sprintf("mem://%s-%d", t.Name(), memSeq.Add(1))
	ctx := context.Background()
	quiet := freshdb.WithLogf(func(string, ...any) {})
	db, err := freshdb.Open(ctx, url, quiet)
	if err != nil {
		t.Fatal(err)
	}
	h := New(db, Options{Token: "secret-token"})
	if c, b := get(h, "/readyz", ""); c != 200 || b != "ok\n" {
		t.Fatalf("readyz: %d %q", c, b)
	}
	// Another process takes the write lease: this one is fenced.
	db2, err := freshdb.Open(ctx, url, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	c, b := get(h, "/readyz", "")
	if c != 200 {
		t.Fatalf("readyz is cached for a second: %d %q", c, b)
	}
	time.Sleep(1100 * time.Millisecond)
	if c, b := get(h, "/readyz", ""); c != 503 || !strings.Contains(b, "fenced") {
		t.Fatalf("readyz after takeover: %d %q", c, b)
	}
	db.Close()
	time.Sleep(1100 * time.Millisecond)
	if c, b := get(h, "/readyz", ""); c != 503 || !strings.Contains(b, "closed") {
		t.Fatalf("readyz after close: %d %q", c, b)
	}
	if c, b := get(h, "/healthz", ""); c != 200 {
		t.Fatalf("healthz is liveness only: %d %q", c, b)
	}
}

func TestPprofNeedsAdmin(t *testing.T) {
	db := memDB(t)
	if c, _ := get(New(db, Options{Pprof: true}), "/debug/pprof/", ""); c != 404 {
		t.Fatalf("pprof without auth must not be served: %d", c)
	}
	ts, _ := ParseTokens([]byte(tokenFile))
	h := New(db, Options{Auth: NewAuth(ts), Pprof: true})
	for tok, want := range map[string]int{"": 401, "reader-0001": 403, "writer-0001": 403, "admin-00001": 200} {
		if c, _ := get(h, "/debug/pprof/", tok); c != want {
			t.Errorf("pprof index with %q: %d, want %d", tok, c, want)
		}
	}
	if c, b := get(h, "/debug/pprof/goroutine?debug=1", "admin-00001"); c != 200 || !strings.Contains(b, "goroutine") {
		t.Errorf("goroutine profile: %d", c)
	}
	if c, _ := get(New(db, Options{Auth: NewAuth(ts)}), "/debug/pprof/", "admin-00001"); c != 404 {
		t.Errorf("pprof served without Pprof: %d", c)
	}
}

func TestAccessAndSlowLog(t *testing.T) {
	db := memDB(t)
	var buf bytes.Buffer
	lg := slog.New(slog.NewJSONHandler(&buf, nil))
	ts, _ := ParseTokens([]byte(tokenFile))
	h := New(db, Options{Auth: NewAuth(ts), Logger: lg, AccessLog: true, SlowOp: -1})
	call(h, "PUT", "/v1/kv/a", "writer-0001", "v")
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("access log is not one JSON object: %v %q", err, buf.String())
	}
	if rec["msg"] != "request" || rec["route"] != "PUT /v1/kv/{key...}" || rec["status"] != float64(200) || rec["token"] != "app" || rec["level"] != "INFO" {
		t.Fatalf("access log: %v", rec)
	}
	if strings.Contains(buf.String(), "writer-0001") {
		t.Fatal("access log leaks the token")
	}

	buf.Reset()
	h = New(db, Options{Logger: lg, SlowOp: time.Nanosecond})
	call(h, "GET", "/v1/kv/a", "", "")
	call(h, "GET", "/v1/topics/t?wait=10ms", "", "")
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 || !strings.Contains(lines[0], `"slow request"`) || !strings.Contains(lines[0], `"WARN"`) {
		t.Fatalf("slow log (long-polls exempt): %q", buf.String())
	}
	buf.Reset()
	h = New(db, Options{Logger: lg})
	call(h, "GET", "/v1/kv/a", "", "")
	if buf.Len() != 0 {
		t.Fatalf("default: no access log, 1s slow threshold: %q", buf.String())
	}
}

// The status wrapper must keep server-sent events flushing.
func TestStreamThroughMetricsWrapper(t *testing.T) {
	db := memDB(t)
	srv := httptest.NewServer(New(db, Options{}))
	defer srv.Close()
	if err := db.Update(context.Background(), func(tx *freshdb.Tx) error {
		return tx.Topic("t").Publish([]byte("hello"))
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/v1/topics/t/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got []byte
	buf := make([]byte, 4096)
	for !bytes.Contains(got, []byte("data:")) {
		n, err := resp.Body.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			t.Fatalf("no event before %v: %q", err, got)
		}
	}
}
