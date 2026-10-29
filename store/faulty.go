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
	return f.Inner.Get(ctx, key)
}

func (f *Faulty) List(ctx context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.roll(f.P.ReadFail) {
		return nil, ErrInjected
	}
	return f.Inner.List(ctx, prefix)
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
// metadata objects (repair notes, manifests, keys) are not torn.
func tearable(key string) bool {
	return strings.HasPrefix(key, "snap/") || strings.HasPrefix(key, "log/") && strings.HasSuffix(key, ".seg")
}
