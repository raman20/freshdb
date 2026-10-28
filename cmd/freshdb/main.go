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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"runtime/debug"
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
  freshdb put KEY VALUE [--subject S]         write a key
  freshdb get KEY [--at LSN]                  read a key (time travel with --at)
  freshdb del KEY
  freshdb scan [PREFIX] [--at LSN]
  freshdb pub TOPIC MESSAGE [--subject S]     publish
  freshdb sub TOPIC [--group G] [--from N] [--max N] [--follow]
  freshdb doc put COLL ID JSON | doc get COLL ID | doc find COLL FIELD VALUE | doc index COLL FIELD
  freshdb vec put COLL ID 0.1,0.2,... | vec del COLL ID | vec search COLL 0.1,0.2,... [-k N]
  freshdb tx '[{"op":"put","key":"k","value":"v"},{"op":"pub","topic":"t","value":"m"}]'
  freshdb branch [list] | branch create NAME [--from B] [--at LSN] | branch promote SRC DST
  freshdb rewind BRANCH LSN                   move a branch back (old head kept in reflog)
  freshdb log [--from LSN]                    show the raw log
  freshdb shred SUBJECT                       crypto-shred a subject everywhere; prints certificate
  freshdb verify                              check CRCs, hash chains, fencing, shred state
  freshdb compact [--grace 0s]                drop overwritten/deleted/shredded data and the
                                              history below each branch head (no time travel
                                              below it afterwards); --grace delays deletion
  freshdb migrate [--to journal|files]         convert a stopped local DB's on-disk layout
  freshdb version

Every command takes --branch B (default main). Use --addr http://host:7070
(or FRESHDB_ADDR) to talk to a running server instead of opening the DB.
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
	bools := map[string]bool{"follow": true, "help": true, "h": true}
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
		return
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
			return http.DefaultClient.Do(r)
		}
		c.close = func() {}
	} else {
		db, err := freshdb.Open(context.Background(), dbURL, freshdb.WithLogf(func(string, ...any) {}))
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
	code := c.run()
	c.close()
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
	listen := flags["listen"]
	if listen == "" {
		listen = env("FRESHDB_LISTEN", "127.0.0.1:7070")
	}
	db, err := freshdb.Open(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("freshdb: open %s: %v", dbURL, err)
	}
	srv := &http.Server{Addr: listen, Handler: server.Handler(db), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()
	if every := flags["compact-every"]; every != "" {
		startCompactor(db, every, flags["compact-grace"])
	}
	log.Printf("freshdb: serving %s on http://%s (epoch %d)", dbURL, listen, db.Epoch())
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	db.Close()
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
		switch c.arg(1, "put|del|search") {
		case "put":
			st, _ = c.call("PUT", "/v1/vectors/"+esc(c.arg(2, "COLL"))+"/"+esc(c.arg(3, "ID"))+c.q(), vec(c.arg(4, "VECTOR")), false)
		case "del":
			st, _ = c.call("DELETE", "/v1/vectors/"+esc(c.arg(2, "COLL"))+"/"+esc(c.arg(3, "ID"))+c.q(), nil, false)
		case "search":
			k := c.flags["k"]
			if k == "" {
				k = "5"
			}
			st, _ = c.call("POST", "/v1/vectors/"+esc(c.arg(2, "COLL"))+"/search"+c.q(), []byte(fmt.Sprintf(`{"vector":%s,"k":%s}`, vec(c.arg(3, "VECTOR")), k)), false)
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
	for {
		var st int
		var b []byte
		if g != "" {
			max := c.flags["max"]
			if max == "" {
				max = "100"
			}
			st, b = c.call("POST", "/v1/topics/"+t+"/groups/"+esc(g)+"/next"+c.q("max", max), nil, false)
		} else {
			st, b = c.call("GET", "/v1/topics/"+t+c.q(), nil, false)
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
		time.Sleep(500 * time.Millisecond)
	}
}
