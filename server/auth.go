package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raman20/freshdb"
)

// Options configure New.
type Options struct {
	// Token, if non-empty, is an admin token: "Authorization: Bearer <Token>".
	Token string
	// Auth holds scoped tokens (ParseTokens, LoadTokenFile); it may be
	// reloaded while serving. Token and Auth combine.
	Auth *Auth
	// MaxStreams caps concurrent SSE streams and MaxWaiters concurrent
	// long-polls (?wait= > 0); over the cap the request gets 503 with
	// Retry-After. 0 = the defaults (1024, 4096), < 0 = no cap.
	MaxStreams, MaxWaiters int
	// MaxBody caps a request body in bytes (default DefaultMaxBody); a
	// larger one is refused with 413.
	MaxBody int
	// Version is reported in freshdb_build_info on /metrics.
	Version string
	// Logger receives the access log and the slow-request log
	// (default slog.Default()).
	Logger *slog.Logger
	// AccessLog logs every request (method, route, status, bytes,
	// seconds, token name) at Info.
	AccessLog bool
	// SlowOp logs, at Warn, requests slower than this (default
	// DefaultSlowOp; < 0 = never). Streams and long-polls are exempt.
	SlowOp time.Duration
	// Pprof serves net/http/pprof under /debug/pprof/ to admin tokens.
	// Ignored without auth (Token or Auth).
	Pprof bool
	// Drain, when closed, starts a graceful shutdown: /readyz answers 503
	// "draining", SSE streams end with an "event: shutdown" (clients
	// resume with Last-Event-ID), long polls return what they have.
	// Close it before http.Server.Shutdown, which waits for handlers.
	Drain <-chan struct{}
	// ReadTimeout bounds reading one request body (default
	// DefaultReadTimeout; < 0 = none). WriteTimeout bounds writing one
	// response (default DefaultWriteTimeout; < 0 = none); streams renew it
	// per event, long polls get it on top of ?wait=, admin routes
	// (compact, gc, backup, ...) are exempt. Set per request, so slow
	// clients cannot hold a handler forever.
	ReadTimeout, WriteTimeout time.Duration
}

// Default per-request timeouts (Options.ReadTimeout, WriteTimeout).
const (
	DefaultReadTimeout  = 5 * time.Minute
	DefaultWriteTimeout = 2 * time.Minute
)

// DefaultMaxBody is the default request body cap: one transaction's limit
// (freshdb.MaxTxBytes) plus room for JSON/base64 encoding.
const DefaultMaxBody = 96 << 20

// Scope is what a token may do: read < write < admin.
type Scope int

const (
	ScopeRead Scope = 1 + iota
	ScopeWrite
	ScopeAdmin
)

func (s Scope) String() string {
	switch s {
	case ScopeRead:
		return "read"
	case ScopeWrite:
		return "write"
	case ScopeAdmin:
		return "admin"
	}
	return "none"
}

// Grant is one token's rights. Resources, if any, limit it to those
// collections (kind "kv" key prefixes, "topic", "docs", "vectors"); a
// limited token cannot use routes that span the database (branches, log,
// verify, admin routes).
type Grant struct {
	Name      string
	Scope     Scope
	Resources []Resource
}

// Resource names one collection of one kind; Prefix matches every name
// starting with Name (written "kind:name*").
type Resource struct {
	Kind, Name string
	Prefix     bool
}

func (g *Grant) allows(kind, name string) bool {
	if len(g.Resources) == 0 {
		return true
	}
	for _, r := range g.Resources {
		if r.Kind == kind && (name == r.Name || r.Prefix && strings.HasPrefix(name, r.Name)) {
			return true
		}
	}
	return false
}

// Tokens is an immutable set of tokens, looked up by SHA-256.
type Tokens struct{ m map[[32]byte]*Grant }

// ParseTokens reads a token file: one token per line, "#" comments.
//
//	SCOPE TOKEN [name=LABEL] [KIND:NAME[*] ...]
//	TOKEN                      (a bare token: admin, the old --token-file)
//
// SCOPE is read, write or admin; KIND is kv, topic, docs or vectors.
func ParseTokens(b []byte) (*Tokens, error) {
	t := &Tokens{m: map[[32]byte]*Grant{}}
	for i, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || strings.HasPrefix(f[0], "#") {
			continue
		}
		g := &Grant{Scope: ScopeAdmin, Name: fmt.Sprintf("line %d", i+1)}
		tok := f[0]
		if len(f) > 1 {
			switch f[0] {
			case "read":
				g.Scope = ScopeRead
			case "write":
				g.Scope = ScopeWrite
			case "admin":
			default:
				return nil, fmt.Errorf("line %d: scope %q (want read, write or admin)", i+1, f[0])
			}
			tok = f[1]
			for _, x := range f[2:] {
				if n, ok := strings.CutPrefix(x, "name="); ok {
					g.Name = n
					continue
				}
				kind, name, ok := strings.Cut(x, ":")
				switch kind {
				case "kv", "topic", "docs", "vectors":
				default:
					ok = false
				}
				if !ok {
					return nil, fmt.Errorf("line %d: resource %q (want kv:, topic:, docs: or vectors:NAME[*])", i+1, x)
				}
				r := Resource{Kind: kind, Name: name}
				r.Name, r.Prefix = strings.CutSuffix(name, "*")
				g.Resources = append(g.Resources, r)
			}
		}
		if len(tok) < 8 {
			return nil, fmt.Errorf("line %d: token shorter than 8 characters", i+1)
		}
		h := sha256.Sum256([]byte(tok))
		if _, dup := t.m[h]; dup {
			return nil, fmt.Errorf("line %d: duplicate token", i+1)
		}
		t.m[h] = g
	}
	if len(t.m) == 0 {
		return nil, errors.New("no tokens")
	}
	return t, nil
}

// Auth is the current token set, swappable while serving.
type Auth struct {
	cur  atomic.Pointer[Tokens]
	file string

	mu   sync.Mutex
	stat fileStamp
}

// NewAuth returns an Auth holding t.
func NewAuth(t *Tokens) *Auth {
	a := &Auth{}
	a.cur.Store(t)
	return a
}

// LoadTokenFile reads a token file; Reload and Watch re-read it.
func LoadTokenFile(path string) (*Auth, error) {
	a := &Auth{file: path}
	if _, err := a.Reload(); err != nil {
		return nil, err
	}
	return a, nil
}

// Set replaces the token set.
func (a *Auth) Set(t *Tokens) { a.cur.Store(t) }

// Reload re-reads the file if it changed. On error the old set stays.
func (a *Auth) Reload() (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	st, err := stampOf(a.file)
	if err != nil {
		return false, err
	}
	if st == a.stat && a.cur.Load() != nil {
		return false, nil
	}
	b, err := os.ReadFile(a.file)
	if err != nil {
		return false, err
	}
	t, err := ParseTokens(b)
	if err != nil {
		return false, fmt.Errorf("%s: %w", a.file, err)
	}
	a.cur.Store(t)
	a.stat = st
	return true, nil
}

// Watch reloads every interval until ctx ends.
func (a *Auth) Watch(ctx context.Context, every time.Duration, logf func(string, ...any)) {
	watch(ctx, every, func() {
		if ok, err := a.Reload(); err != nil {
			logf("freshdb: token file: %v (keeping the previous tokens)", err)
		} else if ok {
			logf("freshdb: token file reloaded (%d tokens)", len(a.cur.Load().m))
		}
	})
}

func (a *Auth) lookup(h [32]byte) *Grant {
	if a == nil {
		return nil
	}
	if t := a.cur.Load(); t != nil {
		return t.m[h]
	}
	return nil
}

// fileStamp detects a changed file (rename-into-place included).
type fileStamp struct {
	mod  time.Time
	size int64
}

func stampOf(path string) (fileStamp, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return fileStamp{}, err
	}
	return fileStamp{fi.ModTime(), fi.Size()}, nil
}

func watch(ctx context.Context, every time.Duration, f func()) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}

type authKey struct{}

// caller is the authenticated identity of a request.
type caller struct {
	h     [32]byte
	auth  *Auth
	admin []byte // the static Options.Token, if that matched
}

// grant returns the caller's current rights (nil: revoked).
func (c *caller) grant() *Grant {
	if c.admin != nil {
		return &Grant{Name: "token", Scope: ScopeAdmin}
	}
	return c.auth.lookup(c.h)
}

// New returns the API handler with Options applied. Handler(db) is
// New(db, Options{}): no auth.
func New(db *freshdb.DB, o Options) http.Handler {
	s := newServer(db, o)
	api := s.routes()
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("ok\n"))
	})
	// readyz: can this server take writes now? (DB.Ping, cached 1s.)
	// Unauthenticated like healthz, so it says only which class of problem.
	m.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		if s.draining() {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "not ready: draining\n")
			return
		}
		if err := s.obs.ready(r.Context(), db); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "not ready: %s\n", notReady(err))
			return
		}
		w.Write([]byte("ok\n"))
	})
	if o.Token == "" && o.Auth == nil {
		m.Handle("/", api)
		return m
	}
	static := []byte(o.Token)
	m.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		c := &caller{h: sha256.Sum256([]byte(got)), auth: o.Auth}
		if ok && len(static) > 0 && subtle.ConstantTimeCompare([]byte(got), static) == 1 {
			c.admin = static
		}
		if !ok || c.grant() == nil {
			s.obs.unauthorized.Add(1)
			w.Header().Set("WWW-Authenticate", `Bearer realm="freshdb"`)
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		api.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), authKey{}, c)))
	}))
	return m
}

var errForbidden = errors.New("forbidden")

// permit checks the caller's current grant for scope on (kind, name);
// kind "" is a database-wide route, "tx" checks only the scope. No auth configured: always nil.
func permit(r *http.Request, scope Scope, kind, name string) error {
	c, _ := r.Context().Value(authKey{}).(*caller)
	if c == nil {
		return nil
	}
	g := c.grant()
	switch {
	case g == nil:
		return fmt.Errorf("%w: token revoked", errForbidden)
	case g.Scope < scope:
		return fmt.Errorf("%w: needs %s scope, token %q has %s", errForbidden, scope, g.Name, g.Scope)
	case kind == "" && len(g.Resources) > 0:
		return fmt.Errorf("%w: token %q is limited to some collections", errForbidden, g.Name)
	case kind != "" && kind != "tx" && !g.allows(kind, name): // tx: checked per op
		return fmt.Errorf("%w: token %q may not use %s %q", errForbidden, g.Name, kind, name)
	}
	return nil
}

// notReady classifies a DB.Ping error for /readyz.
func notReady(err error) string {
	switch {
	case errors.Is(err, freshdb.ErrClosed):
		return "closed"
	case errors.Is(err, freshdb.ErrFenced):
		return "fenced (another writer took over)"
	case errors.Is(err, freshdb.ErrUnknownOutcome):
		return "writer halted on a storage error (restart needed)"
	default:
		return "storage unreachable"
	}
}
