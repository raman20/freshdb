package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/raman20/freshdb"
)

func TestHTTPAPI(t *testing.T) {
	db, err := freshdb.Open(context.Background(), "mem://", freshdb.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ts := httptest.NewServer(Handler(db))
	defer ts.Close()
	do := func(method, path, body string, want int) string {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, b)
		}
		return string(b)
	}
	do("PUT", "/v1/kv/pref?subject=alice", "tea", 200)
	if got := do("GET", "/v1/kv/pref", "", 200); got != "tea" {
		t.Fatal(got)
	}
	do("POST", "/v1/topics/tasks", "t1", 200)
	do("POST", "/v1/tx", `[{"op":"put","key":"done/t1","value":"ok"},{"op":"ack","topic":"tasks","group":"w","offset":0}]`, 200)
	if got := do("POST", "/v1/topics/tasks/groups/w/next", "", 200); strings.TrimSpace(got) != "[]" {
		t.Fatalf("cursor not advanced atomically: %s", got)
	}
	do("PUT", "/v1/vectors/mem/m1?subject=alice", "[1,0]", 200)
	do("PUT", "/v1/vectors/mem/m2", "[0,1]", 200)
	do("POST", "/v1/vectors/mem/ann", `{"m":8}`, 200)
	do("POST", "/v1/vectors/mem/ann", "", 200)
	do("POST", "/v1/vectors/mem/ann", "{", 400)
	for _, body := range []string{`{"vector":[1,0.1],"k":1}`, `{"vector":[1,0.1],"k":1,"exact":true}`, `{"vector":[1,0.1],"k":1,"ef":200}`} {
		if got := do("POST", "/v1/vectors/mem/search", body, 200); !strings.Contains(got, `"m1"`) {
			t.Fatalf("%s: %s", body, got)
		}
	}
	do("PUT", "/v1/docs/notes/n1?subject=alice", `{"user":"alice","text":"likes tea"}`, 200)
	do("POST", "/v1/indexes", `{"coll":"notes","field":"user"}`, 200)
	if got := do("GET", "/v1/docs/notes?field=user&value=alice", "", 200); !strings.Contains(got, "n1") {
		t.Fatal(got)
	}
	// numbers by exact value (no float64 rounding of the query)
	do("PUT", "/v1/docs/nums/a", `{"n":12345678901234567891}`, 200)
	do("PUT", "/v1/docs/nums/b", `{"n":1.0}`, 200)
	for q, want := range map[string]string{"12345678901234567891": `["a"]`, "12345678901234567890": `[]`, "1": `["b"]`} {
		if got := strings.TrimSpace(do("GET", "/v1/docs/nums?field=n&value="+q, "", 200)); got != want {
			t.Fatalf("value=%s: %s, want %s", q, got, want)
		}
	}
	do("POST", "/v1/branches", `{"name":"exp","from":"main","at":0}`, 200)
	do("PUT", "/v1/kv/pref?branch=exp&subject=alice", "coffee", 200)
	if got := do("GET", "/v1/kv/pref", "", 200); got != "tea" {
		t.Fatal("branch leaked into main")
	}
	do("POST", "/v1/branches/main/rewind", `{"lsn":1}`, 200)
	do("GET", "/v1/kv/done/t1", "", 404)
	do("POST", "/v1/shred/alice", "", 200)
	do("GET", "/v1/kv/pref", "", 404)
	do("GET", "/v1/kv/pref?branch=exp", "", 404)
	do("GET", "/v1/kv/pref?at=1", "", 404)
	do("PUT", "/v1/kv/x?subject=alice", "y", 410)
	var r struct{ OK bool }
	json.Unmarshal([]byte(do("GET", "/v1/verify", "", 200)), &r)
	if !r.OK {
		t.Fatal("verify")
	}
}
