// Package server exposes a freshdb DB over a small JSON/HTTP API.
//
// Every endpoint takes ?branch= (default main). Writes accept ?subject= to
// encrypt the value under that subject's key (crypto-shreddable).
//
//	GET    /v1/kv?prefix=&at=           scan            POST /v1/tx            atomic batch
//	GET    /v1/kv/{key}?at=             read (time travel with at=LSN)
//	PUT    /v1/kv/{key}                 write (body = value)
//	DELETE /v1/kv/{key}
//	POST   /v1/topics/{t}               publish (body = message)
//	GET    /v1/topics/{t}?from=&max=&at=&wait=   (wait=30s: long-poll, max 60s)
//	POST   /v1/topics/{t}/groups/{g}/next?max=&wait=   fetch + ack in ONE txn
//	GET    /v1/topics/{t}/stream?from=   server-sent events (Last-Event-ID resumes)
//	PUT    /v1/docs/{coll}/{id}  GET /v1/docs/{coll}/{id}  GET /v1/docs/{coll}?field=&value=
//	PUT    /v1/vectors/{coll}/{id} (body [f,...])  POST /v1/vectors/{coll}/search {"vector":[..],"k":5}
//	GET    /v1/branches   POST /v1/branches {"name","from","at"}
//	POST   /v1/branches/{b}/rewind {"lsn"}   POST /v1/branches/{b}/promote {"to"}
//	DELETE /v1/branches/{b}
//	GET    /v1/log?branch=&from=          POST /v1/shred/{subject}    GET /v1/verify
package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/raman20/freshdb"
	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

type Server struct {
	DB *freshdb.DB

	streams, waiters chan struct{} // nil: no cap
	maxBody          int64
	obs              *observer
	pprof            bool
	drainCtx         context.Context // canceled when Options.Drain closes; nil: never
	readTO, writeTO  time.Duration
}

func newServer(db *freshdb.DB, o Options) *Server {
	s := &Server{DB: db, obs: newObserver(o), pprof: o.Pprof && (o.Token != "" || o.Auth != nil)}
	mk := func(n, def int) chan struct{} {
		if n == 0 {
			n = def
		}
		if n < 0 {
			return nil
		}
		return make(chan struct{}, n)
	}
	s.streams, s.waiters = mk(o.MaxStreams, 1024), mk(o.MaxWaiters, 4096)
	s.maxBody = int64(o.MaxBody)
	if s.maxBody <= 0 {
		s.maxBody = DefaultMaxBody
	}
	to := func(d, def time.Duration) time.Duration {
		if d == 0 {
			return def
		}
		return max(d, 0)
	}
	s.readTO, s.writeTO = to(o.ReadTimeout, DefaultReadTimeout), to(o.WriteTimeout, DefaultWriteTimeout)
	if o.Drain != nil {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { <-o.Drain; cancel() }()
		s.drainCtx = ctx
	}
	return s
}

func (s *Server) draining() bool { return s.drainCtx != nil && s.drainCtx.Err() != nil }

// drainable returns ctx, also canceled when draining starts (for streams
// and long polls).
func (s *Server) drainable(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	if s.drainCtx == nil {
		return ctx, cancel
	}
	stop := context.AfterFunc(s.drainCtx, cancel)
	return ctx, func() { stop(); cancel() }
}

// deadlineBody clears the connection's read deadline once the body is
// read, so it bounds the upload only, not the handler.
type deadlineBody struct {
	io.ReadCloser
	rc *http.ResponseController
}

func (b *deadlineBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF { // on a timeout the deadline stays: the server's discard of the rest fails fast too
		b.rc.SetReadDeadline(time.Time{})
	}
	return n, err
}

// writeDeadline (re)arms the response write deadline d from now.
func writeDeadline(w http.ResponseWriter, d time.Duration) {
	if d > 0 {
		http.NewResponseController(w).SetWriteDeadline(time.Now().Add(d))
	}
}

// Handler is the API without auth and with the default limits.
func Handler(db *freshdb.DB) http.Handler { return newServer(db, Options{}).routes() }

func (s *Server) routes() http.Handler {
	m := http.NewServeMux()
	path := func(name string) func(*http.Request) string {
		return func(r *http.Request) string { return r.PathValue(name) }
	}
	// h registers a route that needs scope on (kind, name(r)); kind ""
	// is database-wide, "tx" checks each op in the handler.
	h := func(pattern string, scope Scope, kind string, name func(*http.Request) string, f http.HandlerFunc) {
		m.HandleFunc(pattern, s.obs.wrap(pattern, func(w http.ResponseWriter, r *http.Request) {
			k, n := kind, ""
			if name != nil {
				n = name(r)
			}
			if err := permit(r, scope, k, n); err != nil {
				fail(w, err)
				return
			}
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, s.maxBody)
				if s.readTO > 0 && r.ContentLength != 0 {
					rc := http.NewResponseController(w)
					if rc.SetReadDeadline(time.Now().Add(s.readTO)) == nil {
						r.Body = &deadlineBody{r.Body, rc}
					}
				}
			}
			if scope != ScopeAdmin {
				writeDeadline(w, s.writeTO)
			}
			f(w, r)
		}))
	}
	const R, W, A = ScopeRead, ScopeWrite, ScopeAdmin
	prefix := func(r *http.Request) string { return r.URL.Query().Get("prefix") }
	h("GET /v1/kv", R, "kv", prefix, s.scan)
	h("GET /v1/kv/{key...}", R, "kv", path("key"), s.get)
	h("PUT /v1/kv/{key...}", W, "kv", path("key"), s.put)
	h("DELETE /v1/kv/{key...}", W, "kv", path("key"), s.del)
	h("POST /v1/tx", W, "tx", nil, s.tx)
	h("POST /v1/topics/{t}", W, "topic", path("t"), s.pub)
	h("GET /v1/topics/{t}", R, "topic", path("t"), s.read)
	h("POST /v1/topics/{t}/groups/{g}/next", R, "topic", path("t"), s.next)
	h("GET /v1/topics/{t}/stream", R, "topic", path("t"), s.stream)
	h("PUT /v1/docs/{c}/{id}", W, "docs", path("c"), s.docPut)
	h("GET /v1/docs/{c}/{id}", R, "docs", path("c"), s.docGet)
	h("GET /v1/docs/{c}", R, "docs", path("c"), s.docFind)
	h("PUT /v1/vectors/{c}/{id}", W, "vectors", path("c"), s.vecPut)
	h("DELETE /v1/vectors/{c}/{id}", W, "vectors", path("c"), s.vecDel)
	h("POST /v1/vectors/{c}/search", R, "vectors", path("c"), s.vecSearch)
	h("GET /v1/branches", R, "", nil, s.branches)
	h("POST /v1/branches", W, "", nil, s.branchCreate)
	h("POST /v1/branches/{b}/rewind", A, "", nil, s.rewind)
	h("POST /v1/branches/{b}/promote", A, "", nil, s.promote)
	h("DELETE /v1/branches/{b}", A, "", nil, s.branchDelete)
	h("GET /v1/log", R, "", nil, s.log)
	h("POST /v1/shred/{subject}", A, "", nil, s.shred)
	h("GET /v1/verify", R, "", nil, s.verify)
	h("POST /v1/indexes", A, "", nil, s.index)
	h("POST /v1/vectors/{c}/ann", A, "", nil, s.vecANN)
	h("POST /v1/compact", A, "", nil, s.compact)
	h("POST /v1/gc", A, "", nil, s.gc)
	h("POST /v1/backup", A, "", nil, s.backup)
	h("GET /metrics", A, "", nil, s.metrics)
	if s.pprof {
		pprofRoutes(func(p string, f http.HandlerFunc) { h(p, A, "", nil, f) })
	}
	return m
}

// acquire takes a slot of a capped pool, or answers 503.
func acquire(w http.ResponseWriter, pool chan struct{}, what string) (release func(), ok bool) {
	if pool == nil {
		return func() {}, true
	}
	select {
	case pool <- struct{}{}:
		return func() { <-pool }, true
	default:
		w.Header().Set("Retry-After", "1")
		fail(w, fmt.Errorf("%w: too many concurrent %s (limit %d)", errBusy, what, cap(pool)))
		return nil, false
	}
}

var errBusy = errors.New("busy")

// ---- helpers ----

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, freshdb.ErrNotFound), errors.Is(err, freshdb.ErrNoBranch):
		code = http.StatusNotFound
	case errors.Is(err, freshdb.ErrConflict), errors.Is(err, freshdb.ErrBranchExists), errors.Is(err, freshdb.ErrNotFastForward), errors.Is(err, freshdb.ErrMainBranch), errors.Is(err, freshdb.ErrBusy):
		code = http.StatusConflict
	case errors.Is(err, freshdb.ErrShredded), errors.Is(err, freshdb.ErrCompacted):
		code = http.StatusGone
	case errors.As(err, new(*http.MaxBytesError)), errors.Is(err, freshdb.ErrTooLarge):
		code = http.StatusRequestEntityTooLarge
	case errors.Is(err, os.ErrDeadlineExceeded):
		code = http.StatusRequestTimeout // --read-timeout while reading the body
	case errors.Is(err, freshdb.ErrBadLSN), errors.Is(err, errBad):
		code = http.StatusBadRequest
	case errors.Is(err, freshdb.ErrFenced), errors.Is(err, errBusy):
		code = http.StatusServiceUnavailable
	case errors.Is(err, errForbidden):
		code = http.StatusForbidden
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

var errBad = errors.New("bad request")

func br(r *http.Request) *freshdb.Branch {
	b := r.URL.Query().Get("branch")
	if b == "" {
		b = "main"
	}
	return r.Context().Value(dbKey{}).(*freshdb.DB).Branch(b)
}

type dbKey struct{}

func (s *Server) with(r *http.Request) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), dbKey{}, s.DB))
}

func u64(r *http.Request, name string) (uint64, bool, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return 0, false, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("%w: %s", errBad, name)
	}
	return n, true, nil
}

func opts(r *http.Request) []freshdb.WriteOpt {
	if s := r.URL.Query().Get("subject"); s != "" {
		return []freshdb.WriteOpt{freshdb.Subject(s)}
	}
	return nil
}

// view runs fn at ?at= (time travel) or at the head.
func view(r *http.Request, fn func(*freshdb.Tx) error) error {
	at, ok, err := u64(r, "at")
	if err != nil {
		return err
	}
	b := br(r)
	if ok {
		return b.ViewAt(r.Context(), at, fn)
	}
	return b.View(r.Context(), fn)
}

func update(r *http.Request, fn func(*freshdb.Tx) error) (uint64, error) {
	return br(r).UpdateLSN(r.Context(), fn)
}

// body reads the request body; the route wrapper caps it (MaxBody), and
// a larger body is an error (413), never a silent truncation.
func body(r *http.Request) ([]byte, error) { return io.ReadAll(r.Body) }

// ---- KV ----

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	var v []byte
	err := view(r, func(tx *freshdb.Tx) (err error) { v, err = tx.Get(r.PathValue("key")); return })
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(v)
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	b, err := body(r)
	if err != nil {
		fail(w, err)
		return
	}
	lsn, err := update(r, func(tx *freshdb.Tx) error { return tx.Put(r.PathValue("key"), b, opts(r)...) })
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"lsn": lsn})
}

func (s *Server) del(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	lsn, err := update(r, func(tx *freshdb.Tx) error { return tx.Delete(r.PathValue("key")) })
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"lsn": lsn})
}

type kvOut struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) scan(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	var out []kvOut
	err := view(r, func(tx *freshdb.Tx) error {
		kvs, err := tx.Scan(r.URL.Query().Get("prefix"))
		for _, p := range kvs {
			out = append(out, kvOut{p.Key, string(p.Value)})
		}
		return err
	})
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, out)
}

// TxOp is one operation of an atomic POST /v1/tx batch.
type TxOp struct {
	Op      string          `json:"op"` // put, del, pub, ack, doc, vec
	Key     string          `json:"key,omitempty"`
	Value   json.RawMessage `json:"value,omitempty"` // string for put/pub, object for doc, array for vec
	Topic   string          `json:"topic,omitempty"`
	Group   string          `json:"group,omitempty"`
	Offset  uint64          `json:"offset,omitempty"`
	Coll    string          `json:"coll,omitempty"`
	Subject string          `json:"subject,omitempty"`
}

func asBytes(raw json.RawMessage) []byte {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return []byte(s)
	}
	return raw
}

func (s *Server) tx(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	var ops []TxOp
	if err := json.NewDecoder(r.Body).Decode(&ops); err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	for _, o := range ops {
		kind, name := "kv", o.Key
		switch o.Op {
		case "pub", "ack":
			kind, name = "topic", o.Topic
		case "doc":
			kind, name = "docs", o.Coll
		case "vec":
			kind, name = "vectors", o.Coll
		}
		if err := permit(r, ScopeWrite, kind, name); err != nil {
			fail(w, err)
			return
		}
	}
	lsn, err := update(r, func(tx *freshdb.Tx) error {
		for _, o := range ops {
			var wo []freshdb.WriteOpt
			if o.Subject != "" {
				wo = append(wo, freshdb.Subject(o.Subject))
			}
			var err error
			switch o.Op {
			case "put":
				err = tx.Put(o.Key, asBytes(o.Value), wo...)
			case "del":
				err = tx.Delete(o.Key)
			case "pub":
				err = tx.Topic(o.Topic).Publish(asBytes(o.Value), wo...)
			case "ack":
				err = tx.Topic(o.Topic).Group(o.Group).Seek(o.Offset + 1)
			case "doc":
				err = tx.Doc(o.Coll).Put(o.Key, o.Value, wo...)
			case "vec":
				var v []float32
				if err = json.Unmarshal(o.Value, &v); err == nil {
					err = tx.Vector(o.Coll).Put(o.Key, v, wo...)
				}
			default:
				err = fmt.Errorf("%w: unknown op %q", errBad, o.Op)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"lsn": lsn, "ops": len(ops)})
}

// ---- topics ----

type msgOut struct {
	Offset   uint64 `json:"offset"`
	LSN      uint64 `json:"lsn"`
	Value    string `json:"value"`
	Redacted bool   `json:"redacted,omitempty"`
}

func msgs(ms []freshdb.Message) []msgOut {
	out := []msgOut{}
	for _, m := range ms {
		out = append(out, msgOut{m.Offset, m.LSN, string(m.Value), m.Redacted})
	}
	return out
}

func (s *Server) pub(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	b, err := body(r)
	if err != nil {
		fail(w, err)
		return
	}
	lsn, err := update(r, func(tx *freshdb.Tx) error { return tx.Topic(r.PathValue("t")).Publish(b, opts(r)...) })
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"lsn": lsn})
}

func (s *Server) read(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	from, _, err := u64(r, "from")
	if err != nil {
		fail(w, err)
		return
	}
	max, _, _ := u64(r, "max")
	_, timeTravel, _ := u64(r, "at")
	wait, err := waitParam(r)
	if err != nil {
		fail(w, err)
		return
	}
	if wait > 0 {
		release, ok := acquire(w, s.waiters, "long-polls")
		if !ok {
			return
		}
		defer release()
		writeDeadline(w, wait+s.writeTO)
	}
	dctx, dcancel := s.drainable(r.Context())
	defer dcancel()
	ctx, cancel := context.WithTimeout(dctx, wait)
	defer cancel()
	for {
		var ms []freshdb.Message
		var at uint64
		err = view(r, func(tx *freshdb.Tx) (err error) {
			at = tx.At()
			ms, err = tx.Topic(r.PathValue("t")).Read(from, int(max))
			return
		})
		if err != nil {
			fail(w, err)
			return
		}
		if len(ms) > 0 || wait == 0 || timeTravel {
			reply(w, msgs(ms))
			return
		}
		if more, err := s.waitHead(ctx, r, at); err != nil {
			fail(w, err)
			return
		} else if !more {
			reply(w, msgs(ms))
			return
		}
	}
}

// maxWait caps ?wait= for long-polls.
const maxWait = 60 * time.Second

// waitParam parses ?wait= (a Go duration like 30s, or seconds); 0 = none.
func waitParam(r *http.Request) (time.Duration, error) {
	v := r.URL.Query().Get("wait")
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		n, err2 := strconv.ParseUint(v, 10, 32)
		if err2 != nil {
			return 0, fmt.Errorf("%w: wait", errBad)
		}
		d = time.Duration(n) * time.Second
	}
	if d < 0 {
		return 0, fmt.Errorf("%w: wait", errBad)
	}
	return min(d, maxWait), nil
}

// waitHead waits for the branch head to move past seen. It returns false
// (and no error) when the wait is over: deadline passed or client gone.
func (s *Server) waitHead(ctx context.Context, r *http.Request, seen uint64) (bool, error) {
	b := r.URL.Query().Get("branch")
	if b == "" {
		b = "main"
	}
	_, err := s.DB.WaitHead(ctx, b, seen)
	if err == nil {
		return true, nil
	}
	if ctx.Err() != nil {
		return false, nil
	}
	return false, err
}

// next fetches up to max messages for a consumer group and advances its
// cursor in the same transaction (at-most-once delivery to the HTTP client;
// embed freshdb and use Fetch/Ack inside your own Update for exactly-once).
func (s *Server) next(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	max, ok, _ := u64(r, "max")
	if !ok {
		max = 10
	}
	wait, err := waitParam(r)
	if err != nil {
		fail(w, err)
		return
	}
	if wait > 0 {
		release, ok := acquire(w, s.waiters, "long-polls")
		if !ok {
			return
		}
		defer release()
		writeDeadline(w, wait+s.writeTO)
	}
	dctx, dcancel := s.drainable(r.Context())
	defer dcancel()
	ctx, cancel := context.WithTimeout(dctx, wait)
	defer cancel()
	for {
		var ms []freshdb.Message
		var at uint64
		_, err := update(r, func(tx *freshdb.Tx) (err error) {
			at = tx.At()
			g := tx.Topic(r.PathValue("t")).Group(r.PathValue("g"))
			if ms, err = g.Fetch(int(max)); err != nil || len(ms) == 0 {
				return err
			}
			return g.Ack(ms[len(ms)-1])
		})
		if err != nil {
			fail(w, err)
			return
		}
		if len(ms) > 0 || wait == 0 {
			reply(w, msgs(ms))
			return
		}
		if more, err := s.waitHead(ctx, r, at); err != nil {
			fail(w, err)
			return
		} else if !more {
			reply(w, msgs(ms))
			return
		}
	}
}

// stream sends a topic as server-sent events from ?from= (or the
// Last-Event-ID header + 1) until the client goes away.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	from, _, err := u64(r, "from")
	if err != nil {
		fail(w, err)
		return
	}
	if id := r.Header.Get("Last-Event-ID"); id != "" {
		n, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			fail(w, fmt.Errorf("%w: Last-Event-ID", errBad))
			return
		}
		from = n + 1
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		fail(w, errors.New("streaming unsupported"))
		return
	}
	release, ok := acquire(w, s.streams, "streams")
	if !ok {
		return
	}
	defer release()
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": freshdb stream\n\n")
	fl.Flush()
	b := r.URL.Query().Get("branch")
	if b == "" {
		b = "main"
	}
	ctx, cancel := s.drainable(r.Context())
	defer cancel()
	for {
		writeDeadline(w, s.writeTO)
		// a token revoked (or narrowed) by a reload ends its streams
		if err := permit(r, ScopeRead, "topic", r.PathValue("t")); err != nil {
			fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
			fl.Flush()
			return
		}
		var ms []freshdb.Message
		var at uint64
		err := br(r).View(ctx, func(tx *freshdb.Tx) (err error) {
			at = tx.At()
			ms, err = tx.Topic(r.PathValue("t")).Read(from, 256)
			return
		})
		if err != nil {
			fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
			fl.Flush()
			return
		}
		for _, m := range msgs(ms) {
			j, _ := json.Marshal(m)
			fmt.Fprintf(w, "event: message\nid: %d\ndata: %s\n\n", m.Offset, j)
			from = m.Offset + 1
		}
		if len(ms) > 0 {
			fl.Flush()
			continue
		}
		pctx, pcancel := context.WithTimeout(ctx, streamPing)
		_, err = s.DB.WaitHead(pctx, b, at)
		pcancel()
		if ctx.Err() != nil {
			if r.Context().Err() == nil { // draining: tell the client to resume elsewhere/later
				writeDeadline(w, s.writeTO)
				fmt.Fprint(w, "event: shutdown\ndata: \"server shutting down; reconnect with Last-Event-ID\"\n\n")
				fl.Flush()
			}
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		} else if err != nil {
			fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
			fl.Flush()
			return
		}
	}
}

// streamPing is the SSE keep-alive interval.
var streamPing = 15 * time.Second

// ---- docs & vectors ----

func (s *Server) docPut(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	b, err := body(r)
	if err != nil || !json.Valid(b) {
		fail(w, fmt.Errorf("%w: body must be JSON", errBad))
		return
	}
	lsn, err := update(r, func(tx *freshdb.Tx) error {
		return tx.Doc(r.PathValue("c")).Put(r.PathValue("id"), json.RawMessage(b), opts(r)...)
	})
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"lsn": lsn})
}

func (s *Server) docGet(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	var d json.RawMessage
	err := view(r, func(tx *freshdb.Tx) (err error) { d, err = tx.Doc(r.PathValue("c")).Get(r.PathValue("id")); return })
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(d)
}

func (s *Server) docFind(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	var val any
	raw := r.URL.Query().Get("value")
	if json.Unmarshal([]byte(raw), &val) != nil {
		val = raw // bare strings are allowed
	}
	var ids []string
	err := view(r, func(tx *freshdb.Tx) (err error) {
		ids, err = tx.Doc(r.PathValue("c")).Find(r.URL.Query().Get("field"), val)
		return
	})
	if err != nil {
		fail(w, err)
		return
	}
	if ids == nil {
		ids = []string{}
	}
	reply(w, ids)
}

func (s *Server) vecPut(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	var v []float32
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		fail(w, fmt.Errorf("%w: body must be a JSON array of numbers", errBad))
		return
	}
	lsn, err := update(r, func(tx *freshdb.Tx) error { return tx.Vector(r.PathValue("c")).Put(r.PathValue("id"), v, opts(r)...) })
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"lsn": lsn})
}

func (s *Server) vecDel(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	lsn, err := update(r, func(tx *freshdb.Tx) error { return tx.Vector(r.PathValue("c")).Delete(r.PathValue("id")) })
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"lsn": lsn})
}

func (s *Server) vecSearch(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	var q struct {
		Vector []float32 `json:"vector"`
		K      int       `json:"k"`
		Exact  bool      `json:"exact"` // always scan, even with an HNSW graph
		Ef     int       `json:"ef"`    // HNSW beam width (0 = the collection's)
	}
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	if q.K == 0 {
		q.K = 5
	}
	type hit struct {
		ID    string  `json:"id"`
		Score float32 `json:"score"`
	}
	out := []hit{}
	err := view(r, func(tx *freshdb.Tx) error {
		vt := tx.Vector(r.PathValue("c"))
		var hs []lens.Hit
		var err error
		if q.Exact {
			hs, err = vt.SearchExact(q.Vector, q.K)
		} else {
			hs, err = vt.SearchANN(q.Vector, q.K, q.Ef)
		}
		for _, h := range hs {
			out = append(out, hit{h.ID, h.Score})
		}
		return err
	})
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, out)
}

// vecANN enables (or reconfigures) the HNSW graph of a vector collection.
func (s *Server) vecANN(w http.ResponseWriter, r *http.Request) {
	var o freshdb.ANNOptions
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&o); err != nil {
			fail(w, fmt.Errorf("%w: %w", errBad, err))
			return
		}
	}
	if err := s.DB.EnableANN(r.Context(), r.PathValue("c"), o); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true})
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	var q struct{ Coll, Field string }
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	if err := s.DB.RegisterIndex(r.Context(), q.Coll, q.Field); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true})
}

// ---- branches ----

func (s *Server) branches(w http.ResponseWriter, r *http.Request) {
	m := s.DB.Manifest()
	reply(w, map[string]any{"heads": s.DB.Branches(), "refs": m.Refs, "reflog": m.Reflog, "epoch": m.Epoch})
}

func (s *Server) branchCreate(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Name, From string
		At         uint64
	}
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	if q.From == "" {
		q.From = "main"
	}
	if err := s.DB.CreateBranch(r.Context(), q.Name, q.From, q.At); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true, "heads": s.DB.Branches()})
}

func (s *Server) rewind(w http.ResponseWriter, r *http.Request) {
	var q struct{ LSN uint64 }
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	if err := s.DB.Rewind(r.Context(), r.PathValue("b"), q.LSN); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true, "heads": s.DB.Branches()})
}

func (s *Server) branchDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.DB.DeleteBranch(r.Context(), r.PathValue("b")); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true, "heads": s.DB.Branches()})
}

func (s *Server) promote(w http.ResponseWriter, r *http.Request) {
	var q struct{ To string }
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	if err := s.DB.Promote(r.Context(), r.PathValue("b"), q.To); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true, "heads": s.DB.Branches()})
}

// LogOp / LogEntry: the raw log (ciphertext of subject values is not shown).
type LogOp struct {
	Kind    string `json:"kind"`
	Space   string `json:"space"`
	Key     string `json:"key"`
	Subject string `json:"subject,omitempty"`
	Value   string `json:"value,omitempty"`
}
type LogEntry struct {
	LSN uint64  `json:"lsn"`
	TS  int64   `json:"ts"`
	Log string  `json:"log"`
	Ops []LogOp `json:"ops"`
}

func (s *Server) log(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	b := r.URL.Query().Get("branch")
	if b == "" {
		b = "main"
	}
	from, _, _ := u64(r, "from")
	es, err := s.DB.Entries(b)
	if err != nil {
		fail(w, err)
		return
	}
	out := []LogEntry{}
	kinds := map[format.Kind]string{format.OpPut: "put", format.OpDel: "del", format.OpAppend: "append", format.OpShred: "shred"}
	for _, e := range es {
		if e.LSN < from {
			continue
		}
		le := LogEntry{LSN: e.LSN, TS: e.TS, Log: e.Log}
		for _, op := range e.Ops {
			lo := LogOp{Kind: kinds[op.Kind], Space: op.Space, Key: op.Key, Subject: op.Subject}
			switch {
			case op.Subject != "" && op.Kind != format.OpShred:
				lo.Value = fmt.Sprintf("<encrypted %d bytes, sha256 %s…>", len(op.Value), hex.EncodeToString(sum(op.Value))[:12])
			case len(op.Value) <= 120:
				lo.Value = string(op.Value)
			default:
				lo.Value = fmt.Sprintf("<%d bytes>", len(op.Value))
			}
			le.Ops = append(le.Ops, lo)
		}
		out = append(out, le)
	}
	reply(w, out)
}

// ---- shred & verify ----

func (s *Server) shred(w http.ResponseWriter, r *http.Request) {
	c, err := s.DB.Shred(r.Context(), r.PathValue("subject"))
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"certificate": c, "valid_signature": freshdb.VerifyCertificate(c)})
}

// compact runs a compaction; ?grace=DURATION (default 5m) delays deleting
// replaced objects for readers of older manifests.
func (s *Server) compact(w http.ResponseWriter, r *http.Request) {
	o := freshdb.CompactOptions{Grace: 5 * time.Minute}
	if g := r.URL.Query().Get("grace"); g != "" {
		d, err := time.ParseDuration(g)
		if err != nil || d < 0 {
			fail(w, fmt.Errorf("%w: grace %q", errBad, g))
			return
		}
		o.Grace = d
	}
	st, err := s.DB.Compact(r.Context(), o)
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, st)
}

// backup copies a consistent snapshot to ?to=URL (a store URL the server
// can write: a local path, journal:///, fs:///, s3://). Admin only: the
// server writes wherever it can reach.
func (s *Server) backup(w http.ResponseWriter, r *http.Request) {
	to := r.URL.Query().Get("to")
	if to == "" {
		fail(w, fmt.Errorf("%w: ?to= (destination store URL) is required", errBad))
		return
	}
	dst, err := store.Open(to)
	if err != nil {
		fail(w, fmt.Errorf("%w: backup destination: %v", errBad, err))
		return
	}
	if c, ok := dst.(io.Closer); ok {
		defer c.Close()
	}
	rep, err := s.DB.Backup(r.Context(), dst)
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, rep)
}

// gc runs GC with retention: ?grace= (default 5m), keep-for=DURATION,
// keep-commits=N, keep-reflog=N, keep-manifests=N (see freshdb.GCOptions).
func (s *Server) gc(w http.ResponseWriter, r *http.Request) {
	o, err := ParseGCOptions(r.URL.Query().Get, 5*time.Minute)
	if err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	st, err := s.DB.GC(r.Context(), o)
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, st)
}

// ParseGCOptions reads GC options named grace, keep-for, keep-commits,
// keep-reflog and keep-manifests through get ("" = default).
func ParseGCOptions(get func(string) string, grace time.Duration) (freshdb.GCOptions, error) {
	o := freshdb.GCOptions{Grace: grace}
	dur := func(name string, d *time.Duration) error {
		if v := get(name); v != "" {
			x, err := time.ParseDuration(v)
			if err != nil || x < 0 {
				return fmt.Errorf("%s %q: want a duration like 24h", name, v)
			}
			*d = x
		}
		return nil
	}
	num := func(name string, n *uint64) error {
		if v := get(name); v != "" {
			x, err := strconv.ParseUint(v, 10, 63)
			if err != nil {
				return fmt.Errorf("%s %q: want a number", name, v)
			}
			*n = x
		}
		return nil
	}
	var reflog, mans uint64
	for _, err := range []error{dur("grace", &o.Grace), dur("keep-for", &o.KeepFor), num("keep-commits", &o.KeepCommits),
		num("keep-reflog", &reflog), num("keep-manifests", &mans)} {
		if err != nil {
			return o, err
		}
	}
	o.KeepReflog, o.KeepManifests = int(min(reflog, 1<<30)), int(min(mans, 1<<30))
	return o, nil
}

func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	rep, err := s.DB.Verify(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, rep)
}
