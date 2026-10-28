package server

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/raman20/freshdb"
)

func pushServer(t *testing.T) (*freshdb.DB, *httptest.Server) {
	db, err := freshdb.Open(context.Background(), t.TempDir(), freshdb.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(New(db, Options{}))
	t.Cleanup(func() { ts.Close(); db.Close() })
	return db, ts
}

func getMsgs(t *testing.T, method, url string) ([]msgOut, time.Duration) {
	start := time.Now()
	req, _ := http.NewRequest(method, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("%s %s: %d", method, url, resp.StatusCode)
	}
	var ms []msgOut
	if err := json.NewDecoder(resp.Body).Decode(&ms); err != nil {
		t.Fatal(err)
	}
	return ms, time.Since(start)
}

func publishLater(t *testing.T, ts *httptest.Server, d time.Duration, v string) {
	go func() {
		time.Sleep(d)
		resp, err := http.Post(ts.URL+"/v1/topics/t", "text/plain", strings.NewReader(v))
		if err == nil {
			resp.Body.Close()
		}
	}()
}

func TestLongPoll(t *testing.T) {
	_, ts := pushServer(t)
	// nothing there: returns [] after the wait
	ms, el := getMsgs(t, "GET", ts.URL+"/v1/topics/t?wait=150ms")
	if len(ms) != 0 || el < 140*time.Millisecond {
		t.Fatalf("empty wait: %v after %v", ms, el)
	}
	// a message published during the wait is returned at once
	publishLater(t, ts, 100*time.Millisecond, "m0")
	ms, el = getMsgs(t, "GET", ts.URL+"/v1/topics/t?wait=10s")
	if len(ms) != 1 || ms[0].Value != "m0" || el > 5*time.Second {
		t.Fatalf("read wait: %v after %v", ms, el)
	}
	// existing data: no wait
	ms, el = getMsgs(t, "GET", ts.URL+"/v1/topics/t?wait=10s")
	if len(ms) != 1 || el > 2*time.Second {
		t.Fatalf("immediate: %v after %v", ms, el)
	}
	// group next: consumes m0, then waits for m1
	ms, _ = getMsgs(t, "POST", ts.URL+"/v1/topics/t/groups/g/next?wait=10s")
	if len(ms) != 1 || ms[0].Value != "m0" {
		t.Fatalf("next: %v", ms)
	}
	publishLater(t, ts, 100*time.Millisecond, "m1")
	ms, el = getMsgs(t, "POST", ts.URL+"/v1/topics/t/groups/g/next?wait=10s")
	if len(ms) != 1 || ms[0].Value != "m1" || el > 5*time.Second {
		t.Fatalf("next wait: %v after %v", ms, el)
	}
	ms, _ = getMsgs(t, "POST", ts.URL+"/v1/topics/t/groups/g/next?wait=50ms")
	if len(ms) != 0 {
		t.Fatalf("acked messages redelivered: %v", ms)
	}
	// bad wait
	resp, _ := http.Get(ts.URL + "/v1/topics/t?wait=soon")
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("bad wait: %d", resp.StatusCode)
	}
}

func TestSSE(t *testing.T) {
	_, ts := pushServer(t)
	for _, v := range []string{"a", "b"} {
		resp, _ := http.Post(ts.URL+"/v1/topics/t", "text/plain", strings.NewReader(v))
		resp.Body.Close()
	}
	read := func(lastID string, want int) []msgOut {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"/v1/topics/t/stream", nil)
		if lastID != "" {
			req.Header.Set("Last-Event-ID", lastID)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
			t.Fatalf("content type %q", ct)
		}
		var out []msgOut
		var id string
		sc := bufio.NewScanner(resp.Body)
		for len(out) < want && sc.Scan() {
			l := sc.Text()
			switch {
			case strings.HasPrefix(l, "id: "):
				id = l[4:]
			case strings.HasPrefix(l, "data: "):
				var m msgOut
				if err := json.Unmarshal([]byte(l[6:]), &m); err != nil {
					t.Fatal(err)
				}
				if id == "" {
					t.Fatal("event without id")
				}
				out = append(out, m)
				if len(out) == 2 {
					publishLater(t, ts, 50*time.Millisecond, "c")
				}
			}
		}
		return out
	}
	got := read("", 3)
	if len(got) != 3 || got[0].Value != "a" || got[1].Value != "b" || got[2].Value != "c" {
		t.Fatalf("stream: %+v", got)
	}
	got = read("1", 1) // resume after offset 1
	if len(got) != 1 || got[0].Value != "c" {
		t.Fatalf("resume: %+v", got)
	}
}
