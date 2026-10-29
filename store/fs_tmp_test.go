package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A-D6: opening a directory used to delete every temp file, including a
// put in flight in another process.
func TestNewFSKeepsFreshTmp(t *testing.T) {
	root := t.TempDir()
	if _, err := NewFS(root); err != nil {
		t.Fatal(err)
	}
	fresh, old := filepath.Join(root, tmpDir, "inflight"), filepath.Join(root, tmpDir, "crashed")
	for _, p := range []string{fresh, old} {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-2 * staleTmpAge)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFS(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatalf("in-flight temp file removed: %v", err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("stale temp file kept: %v", err)
	}
}
