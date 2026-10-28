//go:build unix

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestGCCommand runs the built binary: churn, `gc --shrink`, data intact,
// journal smaller.
func TestGCCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "freshdb")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	dir := filepath.Join(tmp, "db")
	run := func(args ...string) string {
		t.Helper()
		out, err := exec.Command(bin, append(args, "--db", dir)...).CombinedOutput()
		if err != nil {
			t.Fatalf("freshdb %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	big := strings.Repeat("v", 100000)
	for i := 0; i < 120; i++ {
		run("put", fmt.Sprintf("k%d", i%5), big)
	}
	before := fsize(t, dir)
	out := run("gc", "--keep-commits", "2", "--shrink")
	var st struct {
		Objects    int64 `json:"objects"`
		SizeBefore int64 `json:"size_before"`
		SizeAfter  int64 `json:"size_after"`
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &st); err != nil {
		t.Fatalf("%v: %q", err, out)
	}
	// the gc itself appends (and may grow the file ahead) before the shrink
	if st.SizeBefore < before || st.SizeAfter >= st.SizeBefore || st.SizeAfter >= before {
		t.Fatalf("shrink did not shrink: %+v (before %d)\n%s", st, before, out)
	}
	if got := run("get", "k3"); !strings.Contains(got, big) {
		t.Fatalf("k3 lost after gc: %.80q", got)
	}
	run("verify")
	if _, err := exec.Command(bin, "gc", "--shrink", "--db", dir, "--addr", "127.0.0.1:1").CombinedOutput(); err == nil {
		t.Fatal("--shrink with --addr must fail")
	}
}

func fsize(t *testing.T, dir string) int64 {
	fi, err := os.Stat(filepath.Join(dir, "journal.fj"))
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}
