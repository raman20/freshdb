package freshdb

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"
)

// TestS3EndToEnd runs the engine against a real S3-compatible endpoint
// (e.g. local MinIO): FRESHDB_TEST_S3=s3://bucket?endpoint=127.0.0.1:9000&insecure=1&create=1
func TestS3EndToEnd(t *testing.T) {
	raw := os.Getenv("FRESHDB_TEST_S3")
	if raw == "" {
		t.Skip("FRESHDB_TEST_S3 not set")
	}
	u, _ := url.Parse(raw)
	u.Path += fmt.Sprintf("/e2e-%d", time.Now().UnixNano())
	a, err := Open(ctx, u.String(), quiet())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := a.Update(ctx, func(tx *Tx) error {
			tx.Put(fmt.Sprint("k", i), []byte(fmt.Sprint(i)), Subject("u-1"))
			return tx.Topic("t").Publish([]byte(fmt.Sprint(i)))
		}); err != nil {
			t.Fatal(err)
		}
	}
	// second writer takes over via conditional PUT; first is fenced
	b, err := Open(ctx, u.String(), quiet())
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Put(ctx, "z", []byte("zombie")); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie not fenced on S3: %v", err)
	}
	if v, err := b.Get(ctx, "k19"); err != nil || string(v) != "19" {
		t.Fatalf("%q %v", v, err)
	}
	if _, err := b.Shred(ctx, "u-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, "k1"); !errors.Is(err, ErrNotFound) {
		t.Fatal("shredded readable")
	}
	r, err := b.Verify(ctx)
	if err != nil || !r.OK {
		t.Fatalf("%+v %v", r, err)
	}
	a.Close()
	b.Close()
}
