package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/freshdb"
)

var memSeq atomic.Int64

func memDB(t *testing.T) *freshdb.DB {
	t.Helper()
	db, err := freshdb.Open(context.Background(), fmt.Sprintf("mem://%s-%d", t.Name(), memSeq.Add(1)), freshdb.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func call(h http.Handler, method, path, tok, body string) int {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code
}

const tokenFile = `# scoped tokens
read   reader-0001
write  writer-0001 name=app
admin  admin-00001
write  limited-001 name=svc kv:users/* topic:orders docs:people vectors:emb
read   ro-limited1 kv:pub/*
legacy-admin-token
`

func TestScopedTokens(t *testing.T) {
	db := memDB(t)
	ts, err := ParseTokens([]byte(tokenFile))
	if err != nil {
		t.Fatal(err)
	}
	h := New(db, Options{Token: "static-admin", Auth: NewAuth(ts)})
	for _, tc := range []struct {
		method, path, tok, body string
		want                    int
	}{
		{"PUT", "/v1/kv/a", "admin-00001", "v", 200},
		{"PUT", "/v1/kv/a", "static-admin", "v", 200},
		{"PUT", "/v1/kv/a", "legacy-admin-token", "v", 200},
		{"GET", "/v1/kv/a", "reader-0001", "", 200},
		{"PUT", "/v1/kv/a", "reader-0001", "v", 403},
		{"PUT", "/v1/kv/a", "writer-0001", "v", 200},
		{"POST", "/v1/compact", "writer-0001", "", 403},
		{"POST", "/v1/gc", "writer-0001", "", 403},
		{"POST", "/v1/shred/x", "writer-0001", "", 403},
		{"POST", "/v1/gc", "admin-00001", "", 200},
		{"GET", "/v1/kv/a", "nobody-00001", "", 401},
		// limited token: its collections only, nothing database-wide
		{"PUT", "/v1/kv/users/1", "limited-001", "v", 200},
		{"PUT", "/v1/kv/a", "limited-001", "v", 403},
		{"GET", "/v1/kv?prefix=users/", "limited-001", "", 200},
		{"GET", "/v1/kv", "limited-001", "", 403},
		{"POST", "/v1/topics/orders", "limited-001", "m", 200},
		{"POST", "/v1/topics/other", "limited-001", "m", 403},
		{"PUT", "/v1/docs/people/1", "limited-001", `{"a":1}`, 200},
		{"PUT", "/v1/docs/other/1", "limited-001", `{"a":1}`, 403},
		{"PUT", "/v1/vectors/emb/1", "limited-001", `[1,2]`, 200},
		{"PUT", "/v1/vectors/x/1", "limited-001", `[1,2]`, 403},
		{"GET", "/v1/log", "limited-001", "", 403},
		{"GET", "/v1/branches", "limited-001", "", 403},
		{"POST", "/v1/tx", "limited-001", `[{"op":"put","key":"users/2","value":"v"},{"op":"pub","topic":"orders","value":"m"}]`, 200},
		{"POST", "/v1/tx", "limited-001", `[{"op":"put","key":"users/3","value":"v"},{"op":"put","key":"admin","value":"v"}]`, 403},
		{"GET", "/v1/kv/pub/x", "ro-limited1", "", 404},
		{"GET", "/v1/kv/users/1", "ro-limited1", "", 403},
		{"PUT", "/v1/kv/pub/x", "ro-limited1", "v", 403},
	} {
		if got := call(h, tc.method, tc.path, tc.tok, tc.body); got != tc.want {
			t.Errorf("%s %s as %s: %d, want %d", tc.method, tc.path, tc.tok, got, tc.want)
		}
	}
	// the refused tx wrote nothing
	if got := call(h, "GET", "/v1/kv/users/3", "admin-00001", ""); got != 404 {
		t.Fatalf("refused tx left users/3: %d", got)
	}
	for _, bad := range []string{"", "root tok-12345", "read short", "read tok-12345 files:x", "read dup-12345\nwrite dup-12345"} {
		if _, err := ParseTokens([]byte(bad)); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

// TestTokenRotation: the file is re-read without a restart; a bad file
// keeps the old tokens; an SSE stream of a revoked token ends.
func TestTokenRotation(t *testing.T) {
	db := memDB(t)
	f := filepath.Join(t.TempDir(), "tokens")
	write := func(s string) {
		t.Helper()
		tmp := f + ".tmp"
		if err := os.WriteFile(tmp, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, f); err != nil {
			t.Fatal(err)
		}
	}
	write("read old-token-1\n")
	a, err := LoadTokenFile(f)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(db, Options{Auth: a}))
	defer srv.Close()
	h := srv.Config.Handler
	if c := call(h, "GET", "/v1/kv/a", "old-token-1", ""); c != 404 {
		t.Fatal("old token refused", c)
	}
	// open a stream with the old token
	req, _ := http.NewRequest("GET", srv.URL+"/v1/topics/t/stream", nil)
	req.Header.Set("Authorization", "Bearer old-token-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatal(resp, err)
	}
	defer resp.Body.Close()
	lines := make(chan string, 16)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	// rotate: both during the overlap, then only the new one
	write("read old-token-1\nadmin new-token-1\n")
	if ok, err := a.Reload(); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if call(h, "GET", "/v1/kv/a", "old-token-1", "") != 404 || call(h, "PUT", "/v1/kv/a", "new-token-1", "v") != 200 {
		t.Fatal("overlap")
	}
	write("admin new-token-1\n")
	a.Reload()
	if call(h, "GET", "/v1/kv/a", "old-token-1", "") != 401 {
		t.Fatal("revoked token still works")
	}
	// a write wakes the stream, which notices the revocation
	if call(h, "POST", "/v1/topics/t", "new-token-1", "m") != 200 {
		t.Fatal("publish")
	}
	deadline := time.After(10 * time.Second)
	for done := false; !done; {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatal("stream closed without an error event")
			}
			if strings.Contains(l, "revoked") {
				done = true
			}
			if strings.HasPrefix(l, "event: message") {
				t.Fatal("revoked stream delivered a message")
			}
		case <-deadline:
			t.Fatal("stream of a revoked token still open")
		}
	}
	// a broken file keeps the current tokens
	write("superuser x\n")
	if _, err := a.Reload(); err == nil {
		t.Fatal("bad file accepted")
	}
	if call(h, "GET", "/v1/kv/a", "new-token-1", "") != 200 {
		t.Fatal("tokens lost on a bad reload")
	}
}

func TestStreamAndWaiterLimits(t *testing.T) {
	db := memDB(t)
	s := newServer(db, Options{MaxStreams: 1, MaxWaiters: 1})
	srv := httptest.NewServer(s.routes())
	defer srv.Close()
	until := func(what string, ok func() bool) {
		t.Helper()
		for i := 0; i < 500 && !ok(); i++ {
			time.Sleep(10 * time.Millisecond)
		}
		if !ok() {
			t.Fatal(what)
		}
	}
	resp, err := http.Get(srv.URL + "/v1/topics/t/stream")
	if err != nil || resp.StatusCode != 200 {
		t.Fatal(resp, err)
	}
	r2, err := http.Get(srv.URL + "/v1/topics/t/stream")
	if err != nil {
		t.Fatal(err)
	}
	r2.Body.Close()
	if r2.StatusCode != 503 || r2.Header.Get("Retry-After") == "" {
		t.Fatalf("second stream: %d", r2.StatusCode)
	}
	resp.Body.Close() // frees the slot once the handler sees it
	until("stream slot not released", func() bool { return len(s.streams) == 0 })
	// long-polls: one waits, the next is refused, a plain read is not
	done := make(chan struct{})
	go func() {
		defer close(done)
		r, err := http.Get(srv.URL + "/v1/topics/q?wait=3s")
		if err == nil {
			r.Body.Close()
		}
	}()
	until("long-poll not waiting", func() bool { return len(s.waiters) == 1 })
	r, err := http.Get(srv.URL + "/v1/topics/q?wait=3s")
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != 503 {
		t.Fatalf("second long-poll: %d", r.StatusCode)
	}
	if r, err := http.Get(srv.URL + "/v1/topics/q"); err != nil || r.StatusCode != 200 {
		t.Fatal("plain read refused", err)
	}
	<-done
	until("waiter slot not released", func() bool { return len(s.waiters) == 0 })
}

func writePair(t *testing.T, dir string, serial int64) (string, string) {
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "127.0.0.1"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"localhost"}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kd, _ := x509.MarshalECPrivateKey(k)
	c, kf := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	os.WriteFile(c+".tmp", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	os.WriteFile(kf+".tmp", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd}), 0o600)
	os.Rename(c+".tmp", c)
	os.Rename(kf+".tmp", kf)
	return c, kf
}

func TestCertReload(t *testing.T) {
	dir := t.TempDir()
	cf, kf := writePair(t, dir, 1)
	c, err := LoadCerts(cf, kf)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(New(memDB(t), Options{}))
	srv.Listener = tls.NewListener(srv.Listener, &tls.Config{GetCertificate: c.GetCertificate})
	srv.Start()
	defer srv.Close()
	url := "https://" + srv.Listener.Addr().String()
	serial := func() int64 {
		t.Helper()
		cl := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DisableKeepAlives: true}}
		r, err := cl.Get(url + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		return r.TLS.PeerCertificates[0].SerialNumber.Int64()
	}
	if s := serial(); s != 1 {
		t.Fatalf("serial %d", s)
	}
	time.Sleep(10 * time.Millisecond) // a distinct mtime
	writePair(t, dir, 2)
	if ok, err := c.Reload(); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if s := serial(); s != 2 {
		t.Fatalf("after reload: serial %d", s)
	}
	// a half-written renewal (key without its cert) is refused
	os.WriteFile(kf, []byte("garbage"), 0o600)
	if _, err := c.Reload(); err == nil {
		t.Fatal("bad pair accepted")
	}
	if s := serial(); s != 2 {
		t.Fatalf("after a bad reload: serial %d", s)
	}
}
