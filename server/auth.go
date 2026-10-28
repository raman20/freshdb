package server

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/raman20/freshdb"
)

// Options configure New.
type Options struct {
	// Token, if non-empty, is required on every request except GET
	// /healthz as "Authorization: Bearer <Token>".
	Token string
}

// New returns the API handler with Options applied. Handler(db) is
// New(db, Options{}): no auth.
func New(db *freshdb.DB, o Options) http.Handler {
	api := Handler(db)
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("ok\n"))
	})
	if o.Token == "" {
		m.Handle("/", api)
		return m
	}
	want := []byte(o.Token)
	m.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="freshdb"`)
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		api.ServeHTTP(w, r)
	}))
	return m
}
