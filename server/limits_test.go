package server

import (
	"net/http"
	"strings"
	"testing"
)

// Bodies over MaxBody are refused with 413 (they used to be silently
// truncated at 64 MiB and stored), JSON bodies included; at the limit
// they are accepted.
func TestMaxBody(t *testing.T) {
	db := memDB(t)
	h := New(db, Options{MaxBody: 1000})
	if c := call(h, "PUT", "/v1/kv/a", "", strings.Repeat("x", 1000)); c != http.StatusOK && c != http.StatusNoContent {
		t.Fatalf("at limit: %d", c)
	}
	if c := call(h, "PUT", "/v1/kv/b", "", strings.Repeat("x", 1001)); c != http.StatusRequestEntityTooLarge {
		t.Fatalf("over limit: %d, want 413", c)
	}
	if c := call(h, "POST", "/v1/tx", "", `[{"op":"put","key":"c","value":"`+strings.Repeat("x", 2000)+`"}]`); c != http.StatusRequestEntityTooLarge {
		t.Fatalf("JSON over limit: %d, want 413", c)
	}
	if c := call(h, "GET", "/v1/kv/b", "", ""); c != http.StatusNotFound {
		t.Fatalf("refused put stored: %d", c)
	}
}
