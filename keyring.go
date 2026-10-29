package freshdb

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// keyring holds per-subject data-encryption keys (DEKs), wrapped by a
// database key-encryption key (KEK), plus the set of shredded subjects.
// Shredding deletes the wrapped DEK object: every ciphertext for that
// subject, in every segment, snapshot and branch, becomes unreadable.
type keyring struct {
	st   store.Store
	rand io.Reader
	kek  cipher.AEAD
	sign ed25519.PrivateKey

	mu       sync.RWMutex
	deks     map[string]cipher.AEAD // subject hash -> DEK
	shredded map[string]bool        // subject hash
}

func SubjectHash(subject string) string {
	h := sha256.Sum256([]byte("freshdb-subject:" + subject))
	return hex.EncodeToString(h[:16])
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

// getOrCreate fetches a small secret object, creating it if absent.
func getOrCreate(ctx context.Context, st store.Store, key string, gen func() ([]byte, error)) ([]byte, error) {
	b, err := st.Get(ctx, key)
	if err == nil {
		return b, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	nb, err := gen()
	if err != nil {
		return nil, err
	}
	if err := st.PutIfAbsent(ctx, key, nb); err != nil && !errors.Is(err, store.ErrExists) {
		return nil, err
	}
	return st.Get(ctx, key)
}

func openKeyring(ctx context.Context, st store.Store, rnd io.Reader, kek []byte) (*keyring, error) {
	k := &keyring{st: st, rand: rnd, deks: map[string]cipher.AEAD{}, shredded: map[string]bool{}}
	var err error
	if kek == nil {
		// Dev mode: KEK stored next to the data. Production should pass
		// WithKEK from a KMS; otherwise backups of the bucket keep the KEK.
		kek, err = getOrCreate(ctx, st, "keys/_kek", func() ([]byte, error) { return k.randBytes(32) })
		if err != nil {
			return nil, err
		}
	}
	if k.kek, err = newAEAD(kek); err != nil {
		return nil, err
	}
	seed, err := getOrCreate(ctx, st, "keys/_sign", func() ([]byte, error) { return k.randBytes(ed25519.SeedSize) })
	if err != nil {
		return nil, err
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("%w: keys/_sign holds %d bytes, want %d", ErrCorrupt, len(seed), ed25519.SeedSize)
	}
	k.sign = ed25519.NewKeyFromSeed(seed)
	hasCheck, err := k.verifyKEKCheck(ctx)
	if err != nil {
		return nil, err
	}
	sh, err := st.List(ctx, "keys/shredded/")
	if err != nil {
		return nil, err
	}
	for _, s := range sh {
		k.shredded[s[strings.LastIndex(s, "/")+1:]] = true
	}
	// Load every DEK eagerly so decryption (and therefore every lens) is a
	// deterministic function of (log, shredded set), never of IO timing.
	ds, err := st.List(ctx, "keys/dek/")
	if err != nil {
		return nil, err
	}
	for _, d := range ds {
		h := d[strings.LastIndex(d, "/")+1:]
		if k.shredded[h] {
			// a shred crashed between marking and deleting: finish it now
			if err := st.Delete(ctx, d); err != nil {
				return nil, err
			}
			continue
		}
		b, err := st.Get(ctx, d)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		a, err := k.unwrap(h, b)
		if err != nil {
			return nil, fmt.Errorf("%w (cannot unwrap DEK %s: %v)", ErrWrongKEK, h, err)
		}
		k.deks[h] = a
	}
	if !hasCheck { // new database, or one created before the check existed
		if err := k.writeKEKCheck(ctx); err != nil {
			return nil, err
		}
	}
	return k, nil
}

func (k *keyring) randBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(k.rand, b)
	return b, err
}

func (k *keyring) unwrap(h string, b []byte) (cipher.AEAD, error) {
	ns := k.kek.NonceSize()
	if len(b) < ns {
		return nil, errors.New("short")
	}
	raw, err := k.kek.Open(nil, b[:ns], b[ns:], []byte(h))
	if err != nil {
		return nil, err
	}
	return newAEAD(raw)
}

// isShredded takes a subject hash (the log only ever stores hashes).
func (k *keyring) isShredded(h string) bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.shredded[h]
}

func (k *keyring) shreddedList() []string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return sortedMapKeys(k.shredded)
}

// dek returns the subject's DEK, creating it if needed (writer side).
func (k *keyring) dek(ctx context.Context, subject string) (cipher.AEAD, error) {
	h := SubjectHash(subject)
	k.mu.RLock()
	a, ok := k.deks[h]
	sh := k.shredded[h]
	k.mu.RUnlock()
	if sh {
		return nil, ErrShredded
	}
	if ok {
		return a, nil
	}
	b, err := getOrCreate(ctx, k.st, "keys/dek/"+h, func() ([]byte, error) {
		raw, err := k.randBytes(32)
		if err != nil {
			return nil, err
		}
		n, err := k.randBytes(k.kek.NonceSize())
		if err != nil {
			return nil, err
		}
		return append(n, k.kek.Seal(nil, n, raw, []byte(h))...), nil
	})
	if err != nil {
		return nil, err
	}
	a, err = k.unwrap(h, b)
	if err != nil {
		return nil, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.shredded[h] {
		return nil, ErrShredded
	}
	k.deks[h] = a
	return a, nil
}

func aad(log string, lsn uint64, idx int, space, key string) []byte {
	b := []byte(log)
	b = append(b, 0)
	b = binary.LittleEndian.AppendUint64(b, lsn)
	b = binary.LittleEndian.AppendUint32(b, uint32(idx))
	b = append(b, space...)
	b = append(b, 0)
	return append(b, key...)
}

func (k *keyring) seal(a cipher.AEAD, plain, ad []byte) ([]byte, error) {
	n, err := k.randBytes(a.NonceSize())
	if err != nil {
		return nil, err
	}
	return append(n, a.Seal(nil, n, plain, ad)...), nil
}

// open decrypts a value of subject hash h; ok=false if shredded or not authentic.
func (k *keyring) open(h string, ct, ad []byte) ([]byte, bool) {
	k.mu.RLock()
	a, ok := k.deks[h]
	sh := k.shredded[h]
	k.mu.RUnlock()
	if sh || !ok || len(ct) < a.NonceSize() {
		return nil, false
	}
	p, err := a.Open(nil, ct[:a.NonceSize()], ct[a.NonceSize():], ad)
	if err != nil {
		return nil, false
	}
	if p == nil {
		p = []byte{}
	}
	return p, true
}

// reseal encrypts plain under subject hash h's DEK (compaction re-seals
// values under the based log's AAD). false if the subject is shredded.
func (k *keyring) reseal(h string, plain, ad []byte) ([]byte, bool, error) {
	k.mu.RLock()
	a, ok := k.deks[h]
	sh := k.shredded[h]
	k.mu.RUnlock()
	if sh || !ok {
		return nil, false, nil
	}
	ct, err := k.seal(a, plain, ad)
	return ct, err == nil, err
}

// env implements lens.Env on top of the keyring.
type env struct{ k *keyring }

func (e env) Shredded(s string) bool { return e.k.isShredded(s) }
func (e env) Open(en *lens.Entry, i int) ([]byte, bool) {
	op := en.Ops[i]
	if op.Subject == "" {
		return op.Value, true
	}
	return e.k.open(op.Subject, op.Value, aad(en.Log, en.LSN, i, op.Space, op.Key))
}
