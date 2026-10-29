package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Conformance suite run against every backend.
func conformance(t *testing.T, s Store) {
	ctx := context.Background()
	if err := s.PutIfAbsent(ctx, "a/b/1", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.PutIfAbsent(ctx, "a/b/1", []byte("y")); !errors.Is(err, ErrExists) {
		t.Fatalf("create-only violated: %v", err)
	}
	b, err := s.Get(ctx, "a/b/1")
	if err != nil || string(b) != "x" {
		t.Fatalf("get %q %v", b, err)
	}
	if _, err := s.Get(ctx, "a/b/nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("notfound: %v", err)
	}
	s.PutIfAbsent(ctx, "a/b/0", []byte("z"))
	s.PutIfAbsent(ctx, "a/b/c/deep", []byte("z"))
	l, err := s.List(ctx, "a/b/")
	if err != nil || len(l) != 2 || l[0] != "a/b/0" || l[1] != "a/b/1" {
		t.Fatalf("list %v %v", l, err)
	}
	if err := s.Delete(ctx, "a/b/0"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "a/b/0"); !errors.Is(err, ErrNotFound) {
		t.Fatal("delete failed")
	}
	// ranged reads (positions of values inside immutable segments)
	if err := s.PutIfAbsent(ctx, "r/obj", []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // twice: the second read may use a cached handle
		if b, err := GetRange(ctx, s, "r/obj", 6, 5); err != nil || string(b) != "world" {
			t.Fatalf("range %q %v", b, err)
		}
	}
	if _, err := GetRange(ctx, s, "r/obj", 8, 10); err == nil {
		t.Fatal("range past the end must fail")
	}
	if _, err := GetRange(ctx, s, "r/nope", 0, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("range notfound: %v", err)
	}
	if err := s.Delete(ctx, "r/obj"); err != nil {
		t.Fatal(err)
	}
	if _, err := GetRange(ctx, s, "r/obj", 0, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("range after delete: %v", err)
	}
	// racing creators: exactly one wins
	var wg sync.WaitGroup
	wins := make(chan int, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if s.PutIfAbsent(ctx, "race/k", []byte{byte(i)}) == nil {
				wins <- i
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	if n := len(wins); n != 1 {
		t.Fatalf("%d racing creators won", n)
	}
}

func TestMemConformance(t *testing.T) { conformance(t, NewMem()) }

func TestFSConformance(t *testing.T) {
	d := t.TempDir()
	s, err := NewFS(d)
	if err != nil {
		t.Fatal(err)
	}
	conformance(t, s)
	// stale leftover temp files are cleaned on open and never visible
	// (fresh ones may be a put in flight: TestNewFSKeepsFreshTmp)
	junk := filepath.Join(d, tmpDir, "junk")
	os.WriteFile(junk, []byte("partial"), 0o644)
	past := time.Now().Add(-2 * staleTmpAge)
	os.Chtimes(junk, past, past)
	if _, err := NewFS(d); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(d, tmpDir, "junk")); !os.IsNotExist(err) {
		t.Fatal("temp not cleaned")
	}
}

func TestS3Conformance(t *testing.T) {
	u := os.Getenv("FRESHDB_TEST_S3")
	if u == "" {
		t.Skip("set FRESHDB_TEST_S3=s3://bucket/prefix?endpoint=127.0.0.1:9000&insecure=1 to run against MinIO/S3")
	}
	pu, _ := url.Parse(u)
	pu.Path += fmt.Sprintf("/conf-%d", time.Now().UnixNano())
	s, err := Open(pu.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckConditional(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	conformance(t, s)
}
