package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/raman20/freshdb"
)

func TestBearerAuth(t *testing.T) {
	db, err := freshdb.Open(context.Background(), "mem://", freshdb.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, useTLS := range []bool{false, true} {
		h := New(db, Options{Token: "s3cret"})
		var ts *httptest.Server
		if useTLS {
			ts = httptest.NewTLSServer(h)
		} else {
			ts = httptest.NewServer(h)
		}
		c := ts.Client()
		do := func(method, path, tok, body string) int {
			r, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
			if tok != "" {
				r.Header.Set("Authorization", tok)
			}
			resp, err := c.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode == 401 && resp.Header.Get("WWW-Authenticate") == "" {
				t.Fatal("401 without WWW-Authenticate")
			}
			return resp.StatusCode
		}
		for _, tc := range []struct {
			method, path, tok string
			want              int
		}{
			{"GET", "/healthz", "", 200},
			{"PUT", "/v1/kv/a", "", 401},
			{"PUT", "/v1/kv/a", "Bearer wrong", 401},
			{"PUT", "/v1/kv/a", "s3cret", 401},
			{"PUT", "/v1/kv/a", "Basic s3cret", 401},
			{"PUT", "/v1/kv/a", "Bearer s3cret", 200},
			{"GET", "/v1/kv/a", "", 401},
			{"GET", "/v1/kv/a", "Bearer s3cret", 200},
		} {
			if got := do(tc.method, tc.path, tc.tok, "v"); got != tc.want {
				t.Fatalf("tls=%v %s %s %q: %d, want %d", useTLS, tc.method, tc.path, tc.tok, got, tc.want)
			}
		}
		ts.Close()
	}
	// no token: open, healthz still served
	ts := httptest.NewServer(New(db, Options{}))
	defer ts.Close()
	for _, p := range []string{"/healthz", "/v1/kv/a"} {
		resp, err := http.Get(ts.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s without auth: %d", p, resp.StatusCode)
		}
	}
}

// BenchmarkAuthGet: in-process GET /v1/kv/a with and without a token.
func BenchmarkAuthGet(b *testing.B) {
	db, err := freshdb.Open(context.Background(), "mem://", freshdb.WithLogf(func(string, ...any) {}))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.Update(context.Background(), func(tx *freshdb.Tx) error { return tx.Put("a", []byte("v")) })
	ts, err := ParseTokens([]byte("read s3cret-token-of-typical-length-0123456789 kv:a*\n"))
	if err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{"noauth", "token", "scoped"} {
		tok, o := "", Options{}
		switch name {
		case "token":
			tok = "s3cret-token-of-typical-length-0123456789"
			o.Token = tok
		case "scoped":
			tok = "s3cret-token-of-typical-length-0123456789"
			o.Auth = NewAuth(ts)
		}
		b.Run(name, func(b *testing.B) {
			h := New(db, o)
			r := httptest.NewRequest("GET", "/v1/kv/a", nil)
			if tok != "" {
				r.Header.Set("Authorization", "Bearer "+tok)
			}
			for i := 0; i < b.N; i++ {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != 200 {
					b.Fatal(w.Code)
				}
			}
		})
	}
}
