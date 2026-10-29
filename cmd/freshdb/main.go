// Command freshdb is the single-binary CLI and server for freshdb.
//
// One config value: the database URL (--db or FRESHDB_URL), e.g.
//
//	./data                       local directory
//	s3://bucket/prefix?endpoint=host:9000&insecure=1   any S3-compatible store
//	mem://                       in-memory (tests)
//
// Without --addr/FRESHDB_ADDR the CLI opens the database itself (embedded).
// NOTE: opening takes the write lease and fences any other writer, including
// a running `freshdb serve` on the same URL — talk to a server with --addr.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/raman20/freshdb"
	"github.com/raman20/freshdb/server"
	"github.com/raman20/freshdb/store"
)

const usage = `freshdb — one log, many lenses (KV, topics, docs, vectors), with branches,
time travel and crypto-shredding. Config: --db URL (or FRESHDB_URL), default ./freshdb-data.

  freshdb serve [--listen :7070]              run the HTTP API
        [--compact-every 1h] [--compact-grace 5m]   optional background compaction
        [--gc-every 1h] [--keep-for 24h] [--keep-commits N] ...   or background GC (see gc)
        [--token T]                           admin token: "Authorization: Bearer T" (or FRESHDB_TOKEN)
        [--token-file F]                      scoped tokens, one per line: SCOPE TOKEN [name=N] [kv:PFX*|topic:T|docs:C|vectors:C ...]
                                              (SCOPE read|write|admin; a bare TOKEN line is admin);
                                              re-read when it changes (and on SIGHUP)
        [--tls-cert F --tls-key F]            serve HTTPS (or FRESHDB_TLS_CERT/_KEY); reloaded when renewed
        [--max-streams 1024] [--max-waiters 4096]   caps on SSE streams and long-polls (503 over it)
        [--insecure-no-auth]                  allow a non-loopback --listen without a token
        [--max-body 100663296]                request body cap in bytes (413 over it)
        [--log-format text|json]              log format (json: one slog object per line)
        [--access-log] [--slow-op 1s]         log every request; log requests slower than this (0: off)
        [--pprof]                             serve /debug/pprof/ to admin tokens (needs auth)
        [--read-timeout 5m] [--write-timeout 2m]   per-request body read / response write bounds
        [--shutdown-timeout 30s]              SIGTERM: drain, wait this long for requests, close
                                              also: GET /metrics (Prometheus; admin token), /healthz, /readyz
  freshdb put KEY VALUE [--subject S]         write a key
  freshdb get KEY [--at LSN]                  read a key (time travel with --at)
  freshdb del KEY
  freshdb scan [PREFIX] [--at LSN]
  freshdb pub TOPIC MESSAGE [--subject S]     publish
  freshdb sub TOPIC [--group G] [--from N] [--max N] [--follow]
  freshdb doc put COLL ID JSON | doc get COLL ID | doc find COLL FIELD VALUE | doc index COLL FIELD
  freshdb vec put COLL ID 0.1,0.2,... | vec del COLL ID | vec search COLL 0.1,0.2,... [-k N] [--exact] [--ef N]
  freshdb vec ann COLL [--m 16] [--ef-construction 100] [--ef-search 128]   (opt-in HNSW index)
  freshdb tx '[{"op":"put","key":"k","value":"v"},{"op":"pub","topic":"t","value":"m"}]'
  freshdb branch [list] | branch create NAME [--from B] [--at LSN] | branch promote SRC DST | branch delete NAME
  freshdb rewind BRANCH LSN                   move a branch back (old head kept in reflog)
  freshdb log [--from LSN]                    show the raw log
  freshdb shred SUBJECT                       crypto-shred a subject everywhere; prints certificate
  freshdb verify                              check CRCs, hash chains, fencing, shred state
  freshdb compact [--grace 0s]                drop overwritten/deleted/shredded data and the
                                              history below each branch head (no time travel
                                              below it afterwards); --grace delays deletion
  freshdb gc [--keep-for 24h] [--keep-commits N] [--keep-reflog 64] [--keep-manifests 16] [--grace 0s] [--shrink]
                                              compact keeping a history window, drop logs no branch
                                              or kept reflog entry reaches, prune the reflog, delete
                                              old manifests; --shrink (local journal, no server)
                                              then rewrites journal.fj with only live objects
  freshdb migrate [--to journal|files]         convert a stopped local DB's on-disk layout
  freshdb backup DEST_URL                      online, consistent copy (path, journal:///, s3://...);
                                              re-running into the same DEST copies only new objects
  freshdb restore BACKUP_URL --db DEST [--at LSN] [--branch main]
                                              copy a complete backup into an empty DEST, check its
                                              digest, open + verify; --at rewinds the branch (PITR)
  freshdb version

Every command takes --branch B (default main). Use --addr http://host:7070
(or FRESHDB_ADDR) to talk to a running server instead of opening the DB;
--token / FRESHDB_TOKEN is sent as the bearer token.
Key-encryption key: --kek-file F or FRESHDB_KEK (64 hex chars or base64 of
32 bytes). Without one the DB runs in dev-key mode (key stored in the bucket).
A damaged final log segment makes open fail (ErrCorrupt); --repair-tail
(with serve or any local command) keeps its valid prefix instead and copies
the damaged object to quarantine/. Prefer restoring from a backup.
`

type cli struct {
	args  []string
	flags map[string]string
	do    func(*http.Request) (*http.Response, error)
	close func()
}

func parse(argv []string) ([]string, map[string]string) {
	var args []string
	flags := map[string]string{}
	bools := map[string]bool{"follow": true, "help": true, "h": true, "exact": true, "insecure-no-auth": true, "shrink": true, "repair-tail": true, "access-log": true, "pprof": true}
	for i := 0; i < len(argv); i++ {
		a := argv[i]
		if strings.HasPrefix(a, "-") && len(a) > 1 && !isNumberish(a) {
			name := strings.TrimLeft(a, "-")
			if k, v, ok := strings.Cut(name, "="); ok {
				flags[k] = v
			} else if bools[name] || i+1 >= len(argv) {
				flags[name] = "true"
			} else {
				flags[name] = argv[i+1]
				i++
			}
			continue
		}
		args = append(args, a)
	}
	return args, flags
}

func isNumberish(s string) bool { // allow negative vector components like -0.5,1
	return len(s) > 1 && (s[1] >= '0' && s[1] <= '9' || s[1] == '.')
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	log.SetFlags(0)
	args, flags := parse(os.Args[1:])
	if len(args) == 0 || flags["help"] != "" || flags["h"] != "" {
		fmt.Print(usage)
		return
	}
	dbURL := flags["db"]
	if dbURL == "" {
		dbURL = env("FRESHDB_URL", "./freshdb-data")
	}
	addr := flags["addr"]
	if addr == "" {
		addr = os.Getenv("FRESHDB_ADDR")
	}
	if args[0] == "version" {
		fmt.Println("freshdb", buildVersion())
		fmt.Println("formats:", freshdb.Formats())
		return
	}
	if args[0] == "restore" {
		os.Exit(restore(dbURL, args, flags))
	}
	if args[0] == "migrate" {
		os.Exit(migrate(dbURL, flags["to"]))
	}
	if args[0] == "serve" {
		serve(dbURL, flags)
		return
	}
	c := &cli{args: args, flags: flags}
	if addr != "" {
		base := strings.TrimRight(addr, "/")
		if !strings.Contains(base, "://") {
			base = "http://" + base
		}
		c.do = func(r *http.Request) (*http.Response, error) {
			u, _ := url.Parse(base + r.URL.String())
			r.URL = u
			if tok := secret(flags, "token"); tok != "" {
				r.Header.Set("Authorization", "Bearer "+tok)
			}
			return http.DefaultClient.Do(r)
		}
		c.close = func() {}
	} else {
		db, err := freshdb.Open(context.Background(), dbURL, append(keyOpts(flags), freshdb.WithLogf(func(string, ...any) {}))...)
		if err != nil {
			log.Fatalf("freshdb: open %s: %v", dbURL, err)
		}
		h := server.Handler(db)
		c.do = func(r *http.Request) (*http.Response, error) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			return rec.Result(), nil
		}
		c.close = func() { db.Close() }
	}
	if args[0] == "gc" && flags["shrink"] != "" && addr != "" {
		log.Fatalf("freshdb gc --shrink: works on a local directory only (drop --addr, stop the server)")
	}
	code := c.run()
	c.close()
	if code == 0 && args[0] == "gc" && flags["shrink"] != "" {
		code = shrink(dbURL)
	}
	os.Exit(code)
}

// migrate converts a local directory between the files layout (v0.1.0) and
// the journal layout. Every process using the directory must be stopped.
func migrate(dbURL, to string) int {
	dir := strings.TrimPrefix(dbURL, "file://")
	if strings.Contains(dir, "://") {
		log.Printf("freshdb migrate: %s is not a local directory", dbURL)
		return 2
	}
	if to == "" {
		to = string(store.DefaultLayout())
	}
	if err := store.Migrate(context.Background(), dir, store.Layout(to), log.Printf); err != nil {
		log.Printf("freshdb migrate: %v", err)
		return 1
	}
	log.Printf("freshdb migrate: %s uses the %s layout", dir, to)
	return 0
}

// version is set at release build time: -ldflags "-X main.version=v0.1.0".
var version = ""

func buildVersion() string {
	if version != "" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "dev"
}

func serve(dbURL string, flags map[string]string) {
	switch flags["log-format"] {
	case "", "text":
	case "json":
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	default:
		log.Fatalf("freshdb: --log-format: want text or json, got %q", flags["log-format"])
	}
	slowOp := server.DefaultSlowOp
	if v := flags["slow-op"]; v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Fatalf("freshdb: --slow-op: %v", err)
		}
		if slowOp = d; d == 0 {
			slowOp = -1
		}
	}
	listen := flags["listen"]
	if listen == "" {
		listen = env("FRESHDB_LISTEN", "127.0.0.1:7070")
	}
	token := flags["token"]
	if token == "" {
		token = os.Getenv("FRESHDB_TOKEN")
	}
	var auth *server.Auth
	if f := flags["token-file"]; f != "" {
		var err error
		if auth, err = server.LoadTokenFile(f); err != nil {
			log.Fatalf("freshdb: --token-file: %v", err)
		}
	}
	cert, key := flags["tls-cert"], flags["tls-key"]
	if cert == "" {
		cert = os.Getenv("FRESHDB_TLS_CERT")
	}
	if key == "" {
		key = os.Getenv("FRESHDB_TLS_KEY")
	}
	if (cert == "") != (key == "") {
		log.Fatal("freshdb: --tls-cert and --tls-key go together")
	}
	if token == "" && auth == nil && !isLoopback(listen) {
		if flags["insecure-no-auth"] == "" {
			log.Fatalf("freshdb: refusing to listen on %s (not loopback) without auth: set --token / FRESHDB_TOKEN, or pass --insecure-no-auth", listen)
		}
		log.Printf("freshdb: WARNING: --insecure-no-auth: anyone who can reach %s can read and write every branch", listen)
	}
	if flags["pprof"] != "" && token == "" && auth == nil {
		log.Fatal("freshdb: --pprof needs auth (--token or --token-file): profiles expose memory contents")
	}
	// Bind before opening: opening takes the write lease (fencing any
	// running writer), so a port clash must fail first.
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("freshdb: %v (nothing opened; any running writer is untouched)", err)
	}
	db, err := freshdb.Open(context.Background(), dbURL, keyOpts(flags)...)
	if err != nil {
		log.Fatalf("freshdb: open %s: %v", dbURL, err)
	}
	drain := make(chan struct{})
	opts := server.Options{Drain: drain, ReadTimeout: durFlag(flags, "read-timeout"), WriteTimeout: durFlag(flags, "write-timeout"), Token: token, Auth: auth, MaxStreams: intFlag(flags, "max-streams"), MaxWaiters: intFlag(flags, "max-waiters"), MaxBody: intFlag(flags, "max-body"),
		Version: buildVersion(), AccessLog: flags["access-log"] != "", SlowOp: slowOp, Pprof: flags["pprof"] != ""}
	srv := &http.Server{Addr: listen, Handler: server.New(db, opts), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	var certs *server.Certs
	if cert != "" {
		if certs, err = server.LoadCerts(cert, key); err != nil {
			log.Fatalf("freshdb: TLS: %v", err)
		}
		srv.TLSConfig.GetCertificate = certs.GetCertificate
	}
	bg, stopWatch := context.WithCancel(context.Background())
	defer stopWatch()
	if auth != nil {
		go auth.Watch(bg, 2*time.Second, log.Printf)
	}
	if certs != nil {
		go certs.Watch(bg, 10*time.Second, log.Printf)
	}
	go func() { // SIGHUP: reload now
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		for range ch {
			if auth != nil {
				if _, err := auth.Reload(); err != nil {
					log.Printf("freshdb: SIGHUP: token file: %v (keeping the previous tokens)", err)
				}
			}
			if certs != nil {
				if _, err := certs.Reload(); err != nil {
					log.Printf("freshdb: SIGHUP: TLS certificate: %v (keeping the previous one)", err)
				}
			}
			log.Printf("freshdb: SIGHUP: reloaded")
		}
	}()
	// SIGTERM/SIGINT: drain (readyz 503, streams and long polls end),
	// stop accepting, wait for in-flight requests up to
	// --shutdown-timeout, then close the database. A second signal exits
	// at once (the log is crash-safe; nothing acknowledged is lost).
	shutTO := 30 * time.Second
	if d := durFlag(flags, "shutdown-timeout"); d > 0 {
		shutTO = d
	}
	stopped := make(chan struct{})
	go func() {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		sig := <-ch
		log.Printf("freshdb: %v: draining (up to %v)", sig, shutTO)
		go func() { <-ch; log.Printf("freshdb: second signal: exiting now"); os.Exit(1) }()
		close(drain)
		ctx, cancel := context.WithTimeout(context.Background(), shutTO)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("freshdb: shutdown: %v; closing remaining connections", err)
			srv.Close()
		}
		close(stopped)
	}()
	if every := flags["compact-every"]; every != "" {
		startCompactor(db, every, flags["compact-grace"])
	}
	if every := flags["gc-every"]; every != "" {
		startGC(db, every, flags)
	}
	scheme := "http"
	if cert != "" {
		scheme = "https"
	}
	log.Printf("freshdb: serving %s on %s://%s (epoch %d, auth %v)", dbURL, scheme, listen, db.Epoch(), token != "" || auth != nil)
	if cert != "" {
		err = srv.ServeTLS(ln, "", "")
	} else {
		err = srv.Serve(ln)
	}
	if err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-stopped // Serve returns when Shutdown starts; wait for in-flight requests
	if err := db.Close(); err != nil {
		log.Fatalf("freshdb: close: %v", err)
	}
	log.Printf("freshdb: closed cleanly")
}

// durFlag parses a duration flag ("" = 0); exits on a bad value.
func durFlag(flags map[string]string, name string) time.Duration {
	v := flags[name]
	if v == "" {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("freshdb: --%s %q: want a duration like 30s", name, v)
	}
	return d
}

// secret returns --NAME, else the first line of --NAME-file, else
// $FRESHDB_NAME.
func secret(flags map[string]string, name string) string {
	if v := flags[name]; v != "" {
		return v
	}
	if f := flags[name+"-file"]; f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			log.Fatalf("freshdb: --%s-file: %v", name, err)
		}
		return strings.TrimSpace(string(b))
	}
	return os.Getenv("FRESHDB_" + strings.ToUpper(name))
}

// keyOpts picks the KEK source: --kek-file, else $FRESHDB_KEK, else none
// (dev mode).
func keyOpts(flags map[string]string) []freshdb.Option {
	o := []freshdb.Option{freshdb.WithHolder(fmt.Sprintf("freshdb %s, %s, %s", buildVersion(), strings.Join(os.Args[1:2], ""), freshdb.HolderName()))}
	if flags["repair-tail"] != "" {
		o = append(o, freshdb.WithRepairTail(true))
	}
	if f := flags["kek-file"]; f != "" {
		return append(o, freshdb.WithKeyProvider(freshdb.KeyFromFile(f)))
	}
	if os.Getenv("FRESHDB_KEK") != "" {
		return append(o, freshdb.WithKeyProvider(freshdb.KeyFromEnv("FRESHDB_KEK")))
	}
	return o
}

// isLoopback reports whether a listen address binds only loopback. An
// empty host (":7070") binds every interface.
func isLoopback(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		host = listen
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// startCompactor compacts every interval in the background.
func startCompactor(db *freshdb.DB, every, grace string) {
	d, err := time.ParseDuration(every)
	if err != nil || d <= 0 {
		log.Fatalf("freshdb: --compact-every %q: want a positive duration like 1h", every)
	}
	g := 5 * time.Minute
	if grace != "" {
		if g, err = time.ParseDuration(grace); err != nil || g < 0 {
			log.Fatalf("freshdb: --compact-grace %q: want a duration like 5m", grace)
		}
	}
	go func() {
		for range time.Tick(d) {
			st, err := db.Compact(context.Background(), freshdb.CompactOptions{Grace: g})
			if errors.Is(err, freshdb.ErrClosed) {
				return
			}
			if err != nil {
				log.Printf("freshdb: compaction: %v", err)
				continue
			}
			log.Printf("freshdb: compacted %d logs (%d keys, %d messages, %d bytes written, %d objects deleted, %d garbage items pending)",
				st.Logs, st.Keys, st.Messages, st.BaseBytes, st.Deleted, st.Pending)
		}
	}()
}

// startGC runs GC every interval in the background with the gc flags
// (--keep-for, --keep-commits, --keep-reflog, --keep-manifests,
// --compact-grace for the grace period).
func startGC(db *freshdb.DB, every string, flags map[string]string) {
	d, err := time.ParseDuration(every)
	if err != nil || d <= 0 {
		log.Fatalf("freshdb: --gc-every %q: want a positive duration like 1h", every)
	}
	o, err := server.ParseGCOptions(func(k string) string {
		if k == "grace" {
			k = "compact-grace"
		}
		return flags[k]
	}, 5*time.Minute)
	if err != nil {
		log.Fatalf("freshdb: %v", err)
	}
	go func() {
		for range time.Tick(d) {
			st, err := db.GC(context.Background(), o)
			if errors.Is(err, freshdb.ErrClosed) {
				return
			}
			if err != nil {
				log.Printf("freshdb: gc: %v", err)
				continue
			}
			log.Printf("freshdb: gc: based %d logs, dropped %d, pruned %d reflog entries, deleted %d manifests and %d objects (%d garbage items pending)",
				st.Logs, st.LogsDropped, st.ReflogPruned, st.ManifestsDeleted, st.Deleted, st.Pending)
		}
	}()
}

// shrink rewrites a stopped local journal (freshdb gc --shrink).
func shrink(dbURL string) int {
	dir := strings.TrimPrefix(strings.TrimPrefix(dbURL, "file://"), "journal://")
	if strings.Contains(dir, "://") {
		log.Printf("freshdb gc --shrink: %s is not a local directory", dbURL)
		return 2
	}
	st, err := store.ShrinkJournal(dir)
	if err != nil {
		log.Printf("freshdb gc --shrink: %v", err)
		return 1
	}
	b, _ := json.Marshal(st)
	fmt.Println(string(b))
	return 0
}

// ---- commands ----

func (c *cli) arg(i int, name string) string {
	if i >= len(c.args) {
		log.Fatalf("freshdb %s: missing %s\n\n%s", c.args[0], name, usage)
	}
	return c.args[i]
}

func (c *cli) q(extra ...string) string {
	v := url.Values{}
	for _, k := range []string{"branch", "subject", "at", "from", "max"} {
		if c.flags[k] != "" {
			v.Set(k, c.flags[k])
		}
	}
	for i := 0; i+1 < len(extra); i += 2 {
		v.Set(extra[i], extra[i+1])
	}
	if len(v) == 0 {
		return ""
	}
	return "?" + v.Encode()
}

func esc(s string) string { return url.PathEscape(s) }

// call performs a request; raw=true prints the body verbatim.
func (c *cli) call(method, path string, body []byte, raw bool) (int, []byte) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, _ := http.NewRequest(method, path, rd)
	resp, err := c.do(req)
	if err != nil {
		log.Fatalf("freshdb: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		var e struct{ Error string }
		json.Unmarshal(b, &e)
		fmt.Fprintf(os.Stderr, "error (%d): %s\n", resp.StatusCode, e.Error)
		return resp.StatusCode, nil
	}
	if raw {
		os.Stdout.Write(b)
		if len(b) > 0 && b[len(b)-1] != '\n' {
			fmt.Println()
		}
	} else {
		var v any
		if json.Unmarshal(b, &v) == nil {
			out, _ := json.Marshal(v) // short replies on one line, long ones indented
			if len(out) > 100 {
				out, _ = json.MarshalIndent(v, "", "  ")
			}
			fmt.Println(string(out))
		} else {
			os.Stdout.Write(b)
		}
	}
	return resp.StatusCode, b
}

func vec(s string) []byte {
	var parts []string
	for _, p := range strings.Split(s, ",") {
		parts = append(parts, strings.TrimSpace(p))
	}
	return []byte("[" + strings.Join(parts, ",") + "]")
}

func (c *cli) run() int {
	st := 200
	switch c.args[0] {
	case "put":
		st, _ = c.call("PUT", "/v1/kv/"+esc(c.arg(1, "KEY"))+c.q(), []byte(c.arg(2, "VALUE")), false)
	case "get":
		st, _ = c.call("GET", "/v1/kv/"+esc(c.arg(1, "KEY"))+c.q(), nil, true)
	case "del":
		st, _ = c.call("DELETE", "/v1/kv/"+esc(c.arg(1, "KEY"))+c.q(), nil, false)
	case "scan":
		p := ""
		if len(c.args) > 1 {
			p = c.args[1]
		}
		st, _ = c.call("GET", "/v1/kv"+c.q("prefix", p), nil, false)
	case "pub":
		st, _ = c.call("POST", "/v1/topics/"+esc(c.arg(1, "TOPIC"))+c.q(), []byte(c.arg(2, "MESSAGE")), false)
	case "sub":
		st = c.sub()
	case "tx":
		st, _ = c.call("POST", "/v1/tx"+c.q(), []byte(c.arg(1, "JSON")), false)
	case "doc":
		switch c.arg(1, "put|get|find|index") {
		case "put":
			st, _ = c.call("PUT", "/v1/docs/"+esc(c.arg(2, "COLL"))+"/"+esc(c.arg(3, "ID"))+c.q(), []byte(c.arg(4, "JSON")), false)
		case "get":
			st, _ = c.call("GET", "/v1/docs/"+esc(c.arg(2, "COLL"))+"/"+esc(c.arg(3, "ID"))+c.q(), nil, true)
		case "find":
			st, _ = c.call("GET", "/v1/docs/"+esc(c.arg(2, "COLL"))+c.q("field", c.arg(3, "FIELD"), "value", c.arg(4, "VALUE")), nil, false)
		case "index":
			b, _ := json.Marshal(map[string]string{"coll": c.arg(2, "COLL"), "field": c.arg(3, "FIELD")})
			st, _ = c.call("POST", "/v1/indexes", b, false)
		}
	case "vec":
		switch c.arg(1, "put|del|search|ann") {
		case "ann":
			n := func(f string) int { v, _ := strconv.Atoi(c.flags[f]); return v }
			b, _ := json.Marshal(map[string]int{"m": n("m"), "ef_construction": n("ef-construction"), "ef_search": n("ef-search")})
			st, _ = c.call("POST", "/v1/vectors/"+esc(c.arg(2, "COLL"))+"/ann", b, false)
		case "put":
			st, _ = c.call("PUT", "/v1/vectors/"+esc(c.arg(2, "COLL"))+"/"+esc(c.arg(3, "ID"))+c.q(), vec(c.arg(4, "VECTOR")), false)
		case "del":
			st, _ = c.call("DELETE", "/v1/vectors/"+esc(c.arg(2, "COLL"))+"/"+esc(c.arg(3, "ID"))+c.q(), nil, false)
		case "search":
			k := c.flags["k"]
			if k == "" {
				k = "5"
			}
			st, _ = c.call("POST", "/v1/vectors/"+esc(c.arg(2, "COLL"))+"/search"+c.q(), []byte(fmt.Sprintf(`{"vector":%s,"k":%s,"exact":%v,"ef":%d}`, vec(c.arg(3, "VECTOR")), k, c.flags["exact"] == "true", atoiOr(c.flags["ef"]))), false)
		}
	case "branch":
		sub := "list"
		if len(c.args) > 1 {
			sub = c.args[1]
		}
		switch sub {
		case "list":
			var out struct {
				Heads map[string]uint64 `json:"heads"`
			}
			s, b := c.call("GET", "/v1/branches", nil, true)
			_ = s
			json.Unmarshal(b, &out)
		case "create":
			at := c.flags["at"]
			if at == "" {
				at = "0"
			}
			from := c.flags["from"]
			if from == "" {
				from = "main"
			}
			st, _ = c.call("POST", "/v1/branches", []byte(fmt.Sprintf(`{"name":%q,"from":%q,"at":%s}`, c.arg(2, "NAME"), from, at)), false)
		case "delete":
			st, _ = c.call("DELETE", "/v1/branches/"+esc(c.arg(2, "NAME")), nil, false)
		case "promote":
			st, _ = c.call("POST", "/v1/branches/"+esc(c.arg(2, "SRC"))+"/promote", []byte(fmt.Sprintf(`{"to":%q}`, c.arg(3, "DST"))), false)
		default:
			log.Fatalf("freshdb branch: unknown subcommand %q", sub)
		}
	case "rewind":
		st, _ = c.call("POST", "/v1/branches/"+esc(c.arg(1, "BRANCH"))+"/rewind", []byte(fmt.Sprintf(`{"lsn":%s}`, c.arg(2, "LSN"))), false)
	case "log":
		st, _ = c.call("GET", "/v1/log"+c.q(), nil, false)
	case "shred":
		st, _ = c.call("POST", "/v1/shred/"+esc(c.arg(1, "SUBJECT")), nil, false)
	case "compact":
		g := c.flags["grace"]
		if g == "" {
			g = "0s"
		}
		st, _ = c.call("POST", "/v1/compact?grace="+url.QueryEscape(g), nil, false)
	case "gc":
		q := url.Values{}
		q.Set("grace", "0s")
		for _, k := range []string{"grace", "keep-for", "keep-commits", "keep-reflog", "keep-manifests"} {
			if v := c.flags[k]; v != "" {
				q.Set(k, v)
			}
		}
		st, _ = c.call("POST", "/v1/gc?"+q.Encode(), nil, false)
	case "backup":
		if len(c.args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: freshdb backup DEST_URL")
			return 2
		}
		st, _ = c.call("POST", "/v1/backup?to="+url.QueryEscape(c.args[1]), nil, false)
	case "verify":
		var b []byte
		st, b = c.call("GET", "/v1/verify", nil, false)
		var r struct{ OK bool }
		if json.Unmarshal(b, &r) == nil && !r.OK {
			return 2
		}
	default:
		fmt.Fprintf(os.Stderr, "freshdb: unknown command %q\n\n%s", c.args[0], usage)
		return 2
	}
	if st >= 300 {
		return 1
	}
	return 0
}

func (c *cli) sub() int {
	t := esc(c.arg(1, "TOPIC"))
	g := c.flags["group"]
	var wait []string
	if c.flags["follow"] != "" {
		wait = []string{"wait", "30s"} // long-poll: the server answers when messages arrive
	}
	for {
		var st int
		var b []byte
		if g != "" {
			max := c.flags["max"]
			if max == "" {
				max = "100"
			}
			st, b = c.call("POST", "/v1/topics/"+t+"/groups/"+esc(g)+"/next"+c.q(append([]string{"max", max}, wait...)...), nil, false)
		} else {
			st, b = c.call("GET", "/v1/topics/"+t+c.q(wait...), nil, false)
		}
		if st >= 300 {
			return 1
		}
		if c.flags["follow"] == "" {
			return 0
		}
		var ms []map[string]any
		json.Unmarshal(b, &ms)
		if g == "" && len(ms) > 0 {
			c.flags["from"] = fmt.Sprint(uint64(ms[len(ms)-1]["offset"].(float64)) + 1)
		}
	}
}

func atoiOr(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// intFlag parses an integer flag (0 if unset).
func intFlag(flags map[string]string, name string) int {
	v := flags[name]
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("freshdb: --%s: %v", name, err)
	}
	return n
}

// restore: copy a backup into an empty --db, check it, open and verify
// it, and optionally rewind a branch to --at (point in time).
func restore(dbURL string, args []string, flags map[string]string) int {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: freshdb restore BACKUP_URL --db DEST [--at LSN] [--branch main]")
		return 2
	}
	ctx := context.Background()
	src, err := store.Open(args[1])
	if err != nil {
		log.Printf("freshdb restore: %v", err)
		return 1
	}
	dst, err := store.Open(dbURL)
	if err != nil {
		log.Printf("freshdb restore: %v", err)
		return 1
	}
	rep, err := freshdb.Restore(ctx, src, dst)
	for _, s := range []store.Store{src, dst} {
		if c, ok := s.(io.Closer); ok {
			c.Close()
		}
	}
	if err != nil {
		log.Printf("freshdb restore: %v", err)
		return 1
	}
	log.Printf("freshdb restore: copied %d objects (%d bytes), digest matches; backup of manifest %d taken %s",
		rep.Objects, rep.Bytes, rep.Info.Manifest, rep.Info.Started.Format(time.RFC3339))
	db, err := freshdb.Open(ctx, dbURL, keyOpts(flags)...)
	if err != nil {
		log.Printf("freshdb restore: open restored database: %v", err)
		return 1
	}
	defer db.Close()
	vr, err := db.Verify(ctx)
	if err != nil || !vr.OK {
		log.Printf("freshdb restore: verify: %v %+v", err, vr)
		return 1
	}
	if at := flags["at"]; at != "" {
		lsn, err := strconv.ParseUint(at, 10, 64)
		if err != nil {
			log.Printf("freshdb restore: --at: %v", err)
			return 2
		}
		b := flags["branch"]
		if b == "" {
			b = "main"
		}
		if err := db.Rewind(ctx, b, lsn); err != nil {
			log.Printf("freshdb restore: rewind %s to %d: %v", b, lsn, err)
			return 1
		}
		log.Printf("freshdb restore: %s rewound to LSN %d (the previous head is in the reflog)", b, lsn)
	}
	out, _ := json.Marshal(db.Branches())
	log.Printf("freshdb restore: verified; branch heads %s", out)
	return 0
}
