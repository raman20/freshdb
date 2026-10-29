package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
)

// ErrInjected is returned by Faulty for injected failures.
var ErrInjected = errors.New("store: injected fault")

// FaultProbs are per-call probabilities for the deterministic fault injector.
type FaultProbs struct {
	FailBefore float64 // request fails, no effect
	FailAfter  float64 // effect applied, response lost (ambiguous)
	Delay      float64 // response is a timeout; effect lands LATER (reordered)
	Dup        float64 // effect applied, client sees a duplicated-retry 412
	ReadFail   float64 // Get/List fails
	Torn       float64 // segment/snapshot put: a random PREFIX is stored, then error (crash mid-write)

	// Read-path faults. They are transient: the stored object is unchanged
	// and a re-read may return the right bytes. They hit only the log's
	// objects (readFaultable); other prefixes join as their components are
	// reviewed (docs/components/log.md).
	BitFlip   float64 // Get returns the object with one bit flipped
	Misdirect float64 // Get returns another object of the same directory
	ShortRead float64 // the transfer is cut short: ErrShortRead (Store.Get contract)
	StaleList float64 // List of log/ omits the newest one or two keys
}

// Faulty wraps a Store with a seeded fault injector. With a single caller
// goroutine every run is a pure function of the seed.
type Faulty struct {
	Inner   Store
	mu      sync.Mutex
	rng     *rand.Rand
	P       FaultProbs
	On      bool
	delayed []pending
	Trace   []string
}

type pending struct {
	key  string
	data []byte
}

func NewFaulty(inner Store, seed int64, p FaultProbs) *Faulty {
	return &Faulty{Inner: inner, rng: rand.New(rand.NewSource(seed)), P: p, On: true}
}

func (f *Faulty) roll(p float64) bool { return f.On && p > 0 && f.rng.Float64() < p }

// landDelayed lets some delayed writes land (in random order).
func (f *Faulty) landDelayed(ctx context.Context, all bool) {
	keep := f.delayed[:0]
	for _, d := range f.delayed {
		if all || f.rng.Intn(3) == 0 {
			err := f.Inner.PutIfAbsent(ctx, d.key, d.data)
			f.Trace = append(f.Trace, fmt.Sprintf("land %s %v", d.key, err))
		} else {
			keep = append(keep, d)
		}
	}
	f.delayed = keep
}

// Flush lands every delayed write (e.g. after a simulated crash).
func (f *Faulty) Flush(ctx context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.landDelayed(ctx, true)
}

func (f *Faulty) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.landDelayed(ctx, false)
	switch {
	case f.roll(f.P.FailBefore):
		f.Trace = append(f.Trace, "put-fail-before "+key)
		return ErrInjected
	case f.roll(f.P.Delay):
		f.Trace = append(f.Trace, "put-delay "+key)
		f.delayed = append(f.delayed, pending{key, append([]byte{}, data...)})
		return fmt.Errorf("%w: timeout", ErrInjected)
	case f.roll(f.P.FailAfter):
		err := f.Inner.PutIfAbsent(ctx, key, data)
		f.Trace = append(f.Trace, fmt.Sprintf("put-fail-after %s %v", key, err))
		return ErrInjected
	case f.roll(f.P.Torn) && tearable(key):
		cut := f.rng.Intn(len(data) + 1)
		err := f.Inner.PutIfAbsent(ctx, key, data[:cut])
		f.Trace = append(f.Trace, fmt.Sprintf("put-torn %s %d/%d %v", key, cut, len(data), err))
		return ErrInjected
	case f.roll(f.P.Dup):
		err := f.Inner.PutIfAbsent(ctx, key, data)
		f.Trace = append(f.Trace, fmt.Sprintf("put-dup %s %v", key, err))
		return ErrExists
	}
	err := f.Inner.PutIfAbsent(ctx, key, data)
	f.Trace = append(f.Trace, fmt.Sprintf("put %s %v", key, err))
	return err
}

func (f *Faulty) Get(ctx context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.roll(f.P.ReadFail) {
		return nil, ErrInjected
	}
	b, err := f.Inner.Get(ctx, key)
	if err != nil || !readFaultable(key) {
		return b, err
	}
	switch {
	case len(b) > 0 && f.roll(f.P.BitFlip):
		c := append([]byte{}, b...)
		bit := f.rng.Intn(len(c) * 8)
		c[bit/8] ^= 1 << (bit % 8)
		f.Trace = append(f.Trace, fmt.Sprintf("get-bitflip %s %d", key, bit))
		return c, nil
	case f.roll(f.P.Misdirect):
		dir := key[:strings.LastIndex(key, "/")+1]
		keys, lerr := f.Inner.List(ctx, dir)
		if lerr == nil && len(keys) > 1 {
			other := keys[f.rng.Intn(len(keys))]
			if other != key {
				f.Trace = append(f.Trace, fmt.Sprintf("get-misdirect %s -> %s", key, other))
				return f.Inner.Get(ctx, other)
			}
		}
	case len(b) > 0 && f.roll(f.P.ShortRead):
		n := f.rng.Intn(len(b))
		f.Trace = append(f.Trace, fmt.Sprintf("get-short %s %d/%d", key, n, len(b)))
		return nil, fmt.Errorf("%w: %s: got %d of %d bytes", ErrShortRead, key, n, len(b))
	}
	return b, nil
}

func (f *Faulty) List(ctx context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.roll(f.P.ReadFail) {
		return nil, ErrInjected
	}
	keys, err := f.Inner.List(ctx, prefix)
	if err == nil && strings.HasPrefix(prefix, "log/") && len(keys) > 0 && f.roll(f.P.StaleList) {
		n := len(keys) - 1 - f.rng.Intn(min(2, len(keys))) // omit the newest 1 or 2
		f.Trace = append(f.Trace, fmt.Sprintf("list-stale %s %d/%d", prefix, n, len(keys)))
		keys = keys[:n]
	}
	return keys, err
}

// readFaultable: the objects read-path faults hit. LIST staleness is only
// injected under log/: fencing (putManifest) needs a LIST of manifest/
// that reflects every completed create, which S3, the journal and the
// filesystem all provide.
func readFaultable(key string) bool {
	return strings.HasPrefix(key, "log/") || strings.HasPrefix(key, "manifest/")
}

func (f *Faulty) Delete(ctx context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.roll(f.P.FailBefore) {
		return ErrInjected
	}
	return f.Inner.Delete(ctx, key)
}

// tearable: the objects Torn may cut. Torn models a store without atomic
// puts (none is supported) to exercise --repair-tail on segments; small
// metadata objects (repair notes, manifests, keys) are not torn. Snapshots
// and segment-index checkpoints are torn too: they are caches that open
// must skip when damaged.
func tearable(key string) bool {
	return strings.HasPrefix(key, "snap/") ||
		strings.HasPrefix(key, "log/") && (strings.HasSuffix(key, ".seg") || strings.HasSuffix(key, ".idx"))
}
