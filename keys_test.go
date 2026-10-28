package freshdb

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestParseKey(t *testing.T) {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i * 7)
	}
	for _, s := range []string{hex.EncodeToString(k), base64.StdEncoding.EncodeToString(k), base64.RawURLEncoding.EncodeToString(k), " " + hex.EncodeToString(k) + "\n"} {
		got, err := ParseKey(s)
		if err != nil || string(got) != string(k) {
			t.Fatalf("ParseKey(%q) = %x, %v", s, got, err)
		}
	}
	for _, s := range []string{"", "abc", hex.EncodeToString(k[:16]), base64.StdEncoding.EncodeToString(k[:31])} {
		if _, err := ParseKey(s); err == nil {
			t.Fatalf("ParseKey(%q) accepted", s)
		}
	}
}

func TestKeyProviders(t *testing.T) {
	ctx := context.Background()
	quiet := WithLogf(func(string, ...any) {})
	dir := t.TempDir()
	k1, k2 := make([]byte, 32), make([]byte, 32)
	k2[0] = 1
	kf := filepath.Join(dir, "kek")
	if err := os.WriteFile(kf, []byte(hex.EncodeToString(k1)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	url := filepath.Join(dir, "db")

	// create with the file provider and write encrypted + plain data
	db, err := Open(ctx, url, quiet, WithKeyProvider(KeyFromFile(kf)))
	if err != nil {
		t.Fatal(err)
	}
	if db.DevKEK() {
		t.Fatal("DevKEK with a provider")
	}
	if err := db.Update(ctx, func(tx *Tx) error { return tx.Put("a", []byte("secret"), Subject("alice")) }); err != nil {
		t.Fatal(err)
	}
	db.Close()

	// same key through the env provider reads it back
	t.Setenv("FRESHDB_TEST_KEK", base64.StdEncoding.EncodeToString(k1))
	db, err = Open(ctx, url, quiet, WithKeyProvider(KeyFromEnv("FRESHDB_TEST_KEK")))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(ctx, func(tx *Tx) error {
		v, err := tx.Get("a")
		if err == nil && string(v) != "secret" {
			t.Fatalf("got %q", v)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	db.Close()

	// wrong key, and the dev key, are refused
	if _, err := Open(ctx, url, quiet, WithKEK(k2)); !errors.Is(err, ErrWrongKEK) {
		t.Fatalf("wrong KEK: %v", err)
	}
	if _, err := Open(ctx, url, quiet); !errors.Is(err, ErrWrongKEK) {
		t.Fatalf("dev KEK on a keyed DB: %v", err)
	}
	// provider errors and bad lengths surface
	if _, err := Open(ctx, url, quiet, WithKeyProvider(KeyFromEnv("FRESHDB_TEST_UNSET"))); err == nil {
		t.Fatal("unset env accepted")
	}
	short := KeyFunc(func(context.Context) ([]byte, error) { return k1[:16], nil })
	if _, err := Open(ctx, url, quiet, WithKeyProvider(short)); err == nil {
		t.Fatal("16-byte KEK accepted")
	}
}

// A database with no DEKs (nothing encrypted yet) still rejects a wrong KEK,
// and one created before the check object existed gets it on open.
func TestKEKCheckWithoutDEKs(t *testing.T) {
	ctx := context.Background()
	quiet := WithLogf(func(string, ...any) {})
	url := filepath.Join(t.TempDir(), "db")
	k1, k2 := make([]byte, 32), make([]byte, 32)
	k2[5] = 9
	db, err := Open(ctx, url, quiet, WithKEK(k1))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(ctx, func(tx *Tx) error { return tx.Put("plain", []byte("x")) }); err != nil {
		t.Fatal(err)
	}
	// simulate a pre-check database
	if err := db.st.Delete(ctx, kekCheckKey); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = Open(ctx, url, quiet, WithKEK(k1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.st.Get(ctx, kekCheckKey); err != nil {
		t.Fatalf("check object not recreated: %v", err)
	}
	db.Close()
	if _, err := Open(ctx, url, quiet, WithKEK(k2)); !errors.Is(err, ErrWrongKEK) {
		t.Fatalf("wrong KEK without DEKs: %v", err)
	}
}

func TestDevKEK(t *testing.T) {
	db, err := Open(context.Background(), "mem://", WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if !db.DevKEK() {
		t.Fatal("DevKEK false without a key")
	}
}
