//go:build unix

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type proc struct {
	cmd  *exec.Cmd
	addr string
	mu   sync.Mutex
	out  bytes.Buffer
	done chan error
}

func (p *proc) Write(b []byte) (int, error) { p.mu.Lock(); defer p.mu.Unlock(); return p.out.Write(b) }
func (p *proc) log() string                 { p.mu.Lock(); defer p.mu.Unlock(); return p.out.String() }

func freePort(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func startServe(t *testing.T, bin, db, addr string) *proc {
	t.Helper()
	p := &proc{addr: addr, done: make(chan error, 1)}
	p.cmd = exec.Command(bin, "serve", "--db", db, "--listen", addr, "--token", "tok")
	p.cmd.Stdout, p.cmd.Stderr = p, p
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() { p.cmd.Process.Kill() })
	for i := 0; i < 100; i++ {
		if r, err := http.Get("http://" + addr + "/readyz"); err == nil {
			r.Body.Close()
			if r.StatusCode == 200 {
				return p
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("serve did not come up:\n%s", p.log())
	return nil
}

func call(method, url, body string) (int, string) {
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer tok")
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return r.StatusCode, string(b)
}

// TestServeLifecycle: a port clash fences nobody; a second server names
// whom it fenced and the first says who fenced it; SIGTERM drains streams
// and long polls, then closes cleanly.
func TestServeLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "freshdb")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	db := filepath.Join(tmp, "db")
	a := startServe(t, bin, db, freePort(t))
	base := "http://" + a.addr
	if c, b := call("PUT", base+"/v1/kv/k", "v1"); c != 200 {
		t.Fatalf("put: %d %s", c, b)
	}

	// 1. port clash: exits, A is not fenced
	out, err := exec.Command(bin, "serve", "--db", db, "--listen", a.addr, "--token", "tok").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "nothing opened") {
		t.Fatalf("serve on a busy port: %v\n%s", err, out)
	}
	if c, b := call("PUT", base+"/v1/kv/k", "v2"); c != 200 {
		t.Fatalf("A fenced by a server that could not bind: %d %s", c, b)
	}

	// 2. takeover messages
	b := startServe(t, bin, db, freePort(t))
	if l := b.log(); !strings.Contains(l, "took the write lease (epoch") || !strings.Contains(l, "serve, pid") {
		t.Errorf("B does not say whom it fenced:\n%s", l)
	}
	if c, body := call("PUT", base+"/v1/kv/k", "v3"); c != 503 || !strings.Contains(body, "now held by epoch") {
		t.Errorf("A after takeover: %d %s", c, body)
	}
	a.cmd.Process.Kill()

	// 3. graceful shutdown of B with a stream and a long poll open
	bbase := "http://" + b.addr
	if c, body := call("PUT", bbase+"/v1/kv/k", "v4"); c != 200 {
		t.Fatalf("put on B: %d %s", c, body)
	}
	call("POST", bbase+"/v1/topics/t", "m0")
	req, _ := http.NewRequest("GET", bbase+"/v1/topics/t/stream", nil)
	req.Header.Set("Authorization", "Bearer tok")
	sr, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Body.Close()
	stream := make(chan string, 1)
	go func() {
		var all strings.Builder
		sc := bufio.NewScanner(sr.Body)
		for sc.Scan() {
			all.WriteString(sc.Text() + "\n")
		}
		stream <- all.String()
	}()
	poll := make(chan string, 1)
	go func() {
		c, body := call("GET", bbase+"/v1/topics/t?from=1&wait=60s", "")
		poll <- fmt.Sprint(c, " ", body)
	}()
	time.Sleep(300 * time.Millisecond)
	t0 := time.Now()
	b.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case s := <-stream:
		if !strings.Contains(s, "event: shutdown") || !strings.Contains(s, "m0") {
			t.Errorf("stream on drain:\n%s", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stream still open 10 s after SIGTERM")
	}
	select {
	case s := <-poll:
		if !strings.HasPrefix(s, "200 ") {
			t.Errorf("long poll on drain: %s", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("long poll still open 10 s after SIGTERM")
	}
	select {
	case err := <-b.done:
		if err != nil || !strings.Contains(b.log(), "closed cleanly") {
			t.Fatalf("exit: %v\n%s", err, b.log())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("still running 10 s after SIGTERM:\n%s", b.log())
	}
	if d := time.Since(t0); d > 5*time.Second {
		t.Errorf("shutdown took %v", d)
	}
	got, err := exec.Command(bin, "get", "k", "--db", db).CombinedOutput()
	if err != nil || strings.TrimSpace(string(got)) != "v4" {
		t.Errorf("after shutdown: %v %q", err, got)
	}
}
