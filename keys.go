package freshdb

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/raman20/freshdb/store"
)

// KeyProvider supplies the 32-byte key-encryption key (KEK) at open. Wrap a
// KMS or secret manager in one; KeyFromEnv and KeyFromFile are built in.
type KeyProvider interface {
	KEK(ctx context.Context) ([]byte, error)
}

// KeyFunc adapts a function to KeyProvider.
type KeyFunc func(ctx context.Context) ([]byte, error)

func (f KeyFunc) KEK(ctx context.Context) ([]byte, error) { return f(ctx) }

// WithKeyProvider makes Open fetch the KEK from p (overrides WithKEK).
func WithKeyProvider(p KeyProvider) Option { return func(o *Options) { o.Keys = p } }

// KeyFromEnv reads the KEK from an environment variable: 64 hex characters
// or the standard base64 of 32 bytes.
func KeyFromEnv(name string) KeyProvider {
	return KeyFunc(func(context.Context) ([]byte, error) {
		v, ok := os.LookupEnv(name)
		if !ok || v == "" {
			return nil, fmt.Errorf("freshdb: KEK env %s is not set", name)
		}
		k, err := ParseKey(v)
		if err != nil {
			return nil, fmt.Errorf("freshdb: KEK env %s: %w", name, err)
		}
		return k, nil
	})
}

// KeyFromFile reads the KEK from a file holding 64 hex characters or the
// base64 of 32 bytes (surrounding whitespace ignored).
func KeyFromFile(path string) KeyProvider {
	return KeyFunc(func(context.Context) ([]byte, error) {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("freshdb: KEK file: %w", err)
		}
		k, err := ParseKey(string(b))
		if err != nil {
			return nil, fmt.Errorf("freshdb: KEK file %s: %w", path, err)
		}
		return k, nil
	})
}

// ParseKey decodes a 32-byte key from 64 hex characters or base64.
func ParseKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if len(s) == 64 {
		if k, err := hex.DecodeString(s); err == nil {
			return k, nil
		}
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if k, err := enc.DecodeString(s); err == nil && len(k) == 32 {
			return k, nil
		}
	}
	return nil, errors.New("want 32 bytes as 64 hex characters or base64")
}

// kekCheckKey holds a constant sealed under the KEK, so a wrong KEK fails at
// open even on a database that has no DEKs yet.
const kekCheckKey = "keys/_kekcheck"

var kekCheckText = []byte("freshdb-kek-check-v1")

// verifyKEKCheck reports whether the check object exists, and ErrWrongKEK
// if it does not open under k's KEK.
func (k *keyring) verifyKEKCheck(ctx context.Context) (bool, error) {
	b, err := k.st.Get(ctx, kekCheckKey)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ns := k.kek.NonceSize()
	if len(b) < ns {
		return true, ErrWrongKEK
	}
	if p, err := k.kek.Open(nil, b[:ns], b[ns:], []byte(kekCheckKey)); err != nil || string(p) != string(kekCheckText) {
		return true, ErrWrongKEK
	}
	return true, nil
}

// writeKEKCheck creates the check object (once; a racing writer's copy is
// verified instead).
func (k *keyring) writeKEKCheck(ctx context.Context) error {
	n, err := k.randBytes(k.kek.NonceSize())
	if err != nil {
		return err
	}
	b := append(n, k.kek.Seal(nil, n, kekCheckText, []byte(kekCheckKey))...)
	if err := k.st.PutIfAbsent(ctx, kekCheckKey, b); err != nil {
		if !errors.Is(err, store.ErrExists) {
			return err
		}
		_, err = k.verifyKEKCheck(ctx)
		return err
	}
	return nil
}
