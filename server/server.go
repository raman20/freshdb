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
//	GET    /v1/topics/{t}?from=&max=&at=
//	POST   /v1/topics/{t}/groups/{g}/next?max=   fetch + ack in ONE txn
//	PUT    /v1/docs/{coll}/{id}  GET /v1/docs/{coll}/{id}  GET /v1/docs/{coll}?field=&value=
//	PUT    /v1/vectors/{coll}/{id} (body [f,...])  POST /v1/vectors/{coll}/search {"vector":[..],"k":5}
//	GET    /v1/branches   POST /v1/branches {"name","from","at"}
//	POST   /v1/branches/{b}/rewind {"lsn"}   POST /v1/branches/{b}/promote {"to"}
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
	"strconv"

	"github.com/raman20/freshdb"
	"github.com/raman20/freshdb/format"
)

type Server struct{ DB *freshdb.DB }

func Handler(db *freshdb.DB) http.Handler {
	s := &Server{db}
	m := http.NewServeMux()
	m.HandleFunc("GET /v1/kv", s.scan)
	m.HandleFunc("GET /v1/kv/{key...}", s.get)
	m.HandleFunc("PUT /v1/kv/{key...}", s.put)
	m.HandleFunc("DELETE /v1/kv/{key...}", s.del)
	m.HandleFunc("POST /v1/tx", s.tx)
	m.HandleFunc("POST /v1/topics/{t}", s.pub)
	m.HandleFunc("GET /v1/topics/{t}", s.read)
	m.HandleFunc("POST /v1/topics/{t}/groups/{g}/next", s.next)
	m.HandleFunc("PUT /v1/docs/{c}/{id}", s.docPut)
	m.HandleFunc("GET /v1/docs/{c}/{id}", s.docGet)
	m.HandleFunc("GET /v1/docs/{c}", s.docFind)
	m.HandleFunc("PUT /v1/vectors/{c}/{id}", s.vecPut)
	m.HandleFunc("DELETE /v1/vectors/{c}/{id}", s.vecDel)
	m.HandleFunc("POST /v1/vectors/{c}/search", s.vecSearch)
	m.HandleFunc("GET /v1/branches", s.branches)
	m.HandleFunc("POST /v1/branches", s.branchCreate)
	m.HandleFunc("POST /v1/branches/{b}/rewind", s.rewind)
	m.HandleFunc("POST /v1/branches/{b}/promote", s.promote)
	m.HandleFunc("GET /v1/log", s.log)
	m.HandleFunc("POST /v1/shred/{subject}", s.shred)
	m.HandleFunc("GET /v1/verify", s.verify)
	m.HandleFunc("POST /v1/indexes", s.index)
	return m
}

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
	case errors.Is(err, freshdb.ErrConflict), errors.Is(err, freshdb.ErrBranchExists), errors.Is(err, freshdb.ErrNotFastForward):
		code = http.StatusConflict
	case errors.Is(err, freshdb.ErrShredded):
		code = http.StatusGone
	case errors.Is(err, freshdb.ErrBadLSN), errors.Is(err, errBad):
		code = http.StatusBadRequest
	case errors.Is(err, freshdb.ErrFenced):
		code = http.StatusServiceUnavailable
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

func body(r *http.Request) ([]byte, error) { return io.ReadAll(io.LimitReader(r.Body, 64<<20)) }

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
		fail(w, fmt.Errorf("%w: %v", errBad, err))
		return
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
	var ms []freshdb.Message
	err = view(r, func(tx *freshdb.Tx) (err error) { ms, err = tx.Topic(r.PathValue("t")).Read(from, int(max)); return })
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, msgs(ms))
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
	var ms []freshdb.Message
	_, err := update(r, func(tx *freshdb.Tx) (err error) {
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
	reply(w, msgs(ms))
}

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
	}
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %v", errBad, err))
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
		hs, err := tx.Vector(r.PathValue("c")).Search(q.Vector, q.K)
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

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	var q struct{ Coll, Field string }
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %v", errBad, err))
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
		fail(w, fmt.Errorf("%w: %v", errBad, err))
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
		fail(w, fmt.Errorf("%w: %v", errBad, err))
		return
	}
	if err := s.DB.Rewind(r.Context(), r.PathValue("b"), q.LSN); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true, "heads": s.DB.Branches()})
}

func (s *Server) promote(w http.ResponseWriter, r *http.Request) {
	var q struct{ To string }
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %v", errBad, err))
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

func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	rep, err := s.DB.Verify(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, rep)
}
