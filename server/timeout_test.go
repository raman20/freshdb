package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/raman20/freshdb"
)

// TestTimeouts: a stalled upload is cut off after ReadTimeout; long polls
// and streams outlive WriteTimeout (renewed), and a slow handler is not
// cut by the read deadline once its body is read.
func TestTimeouts(t *testing.T) {
	db, err := freshdb.Open(context.Background(), "mem://", freshdb.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old := streamPing
	streamPing = 100 * time.Millisecond
	defer func() { streamPing = old }()
	srv := httptest.NewServer(New(db, Options{ReadTimeout: 300 * time.Millisecond, WriteTimeout: 300 * time.Millisecond}))
	defer srv.Close()

	// stalled body
	c, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	io.WriteString(c, "PUT /v1/kv/stalled HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nabc")
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	t0 := time.Now()
	resp, _ := bufio.NewReader(c).ReadString('\n')
	if time.Since(t0) > 2*time.Second {
		t.Fatalf("stalled upload held the connection %v", time.Since(t0))
	}
	if resp != "" && !strings.HasPrefix(resp, "HTTP/1.1 408") { // closed, or 408
		t.Fatalf("stalled upload acknowledged: %q", resp)
	}
	if _, err := db.Get(context.Background(), "stalled"); err == nil {
		t.Fatal("stalled upload was written")
	}

	// long poll longer than WriteTimeout
	t0 = time.Now()
	r, err := http.Get(srv.URL + "/v1/topics/t?wait=1s")
	if err != nil || r.StatusCode != 200 {
		t.Fatalf("long poll: %v %v", err, r)
	}
	r.Body.Close()
	if d := time.Since(t0); d < 900*time.Millisecond {
		t.Fatalf("long poll returned after %v", d)
	}

	// a stream outlives WriteTimeout
	r, err = http.Get(srv.URL + "/v1/topics/t/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	sc := bufio.NewScanner(r.Body)
	pings := 0
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && sc.Scan() {
		if sc.Text() == ": ping" {
			pings++
		}
	}
	if pings < 5 {
		t.Fatalf("stream: %d pings in 1 s (err %v)", pings, sc.Err())
	}

	// body read, then a handler slower than ReadTimeout: still answered
	r, err = http.Post(srv.URL+"/v1/topics/t2", "text/plain", strings.NewReader("x"))
	if err != nil || r.StatusCode != 200 {
		t.Fatalf("publish: %v %v", err, r)
	}
	r.Body.Close()
}

// TestDrain: closing Options.Drain flips readyz and ends streams and
// long polls.
func TestDrain(t *testing.T) {
	db, err := freshdb.Open(context.Background(), "mem://", freshdb.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	drain := make(chan struct{})
	srv := httptest.NewServer(New(db, Options{Drain: drain}))
	defer srv.Close()
	sr, err := http.Get(srv.URL + "/v1/topics/t/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Body.Close()
	done := make(chan string, 2)
	go func() { b, _ := io.ReadAll(sr.Body); done <- string(b) }()
	go func() {
		r, err := http.Get(srv.URL + "/v1/topics/t?wait=60s")
		if err != nil {
			done <- err.Error()
			return
		}
		r.Body.Close()
		done <- r.Status
	}()
	time.Sleep(200 * time.Millisecond)
	close(drain)
	for i := 0; i < 2; i++ {
		select {
		case s := <-done:
			if !strings.Contains(s, "event: shutdown") && !strings.HasPrefix(s, "200") {
				t.Errorf("on drain: %q", s)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("stream or long poll still open after drain")
		}
	}
	r, _ := http.Get(srv.URL + "/readyz")
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != 503 || !strings.Contains(string(b), "draining") {
		t.Fatalf("readyz while draining: %d %q", r.StatusCode, b)
	}
}

// BenchmarkLoopbackPut: PUT over a real loopback connection, per-request
// deadlines on (defaults) vs off.
func BenchmarkLoopbackPut(b *testing.B) {
	for _, tc := range []struct {
		name string
		to   time.Duration
	}{{"deadlines", 0}, {"none", -1}} {
		b.Run(tc.name, func(b *testing.B) {
			db, err := freshdb.Open(context.Background(), "mem://", freshdb.WithLogf(func(string, ...any) {}))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			srv := httptest.NewServer(New(db, Options{ReadTimeout: tc.to, WriteTimeout: tc.to}))
			defer srv.Close()
			c := srv.Client()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				req, _ := http.NewRequest("PUT", srv.URL+"/v1/kv/k", strings.NewReader("value"))
				r, err := c.Do(req)
				if err != nil {
					b.Fatal(err)
				}
				io.Copy(io.Discard, r.Body)
				r.Body.Close()
			}
		})
	}
}
