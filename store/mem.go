package store

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// Mem is an in-memory Store. Safe for concurrent use.
type Mem struct {
	mu sync.Mutex
	m  map[string][]byte
}

func NewMem() *Mem { return &Mem{m: map[string][]byte{}} }

var (
	sharedMu sync.Mutex
	shared   = map[string]*Mem{}
)

// SharedMem returns a process-wide named in-memory store (for mem:// URLs).
func SharedMem(name string) *Mem {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if m, ok := shared[name]; ok {
		return m
	}
	m := NewMem()
	shared[name] = m
	return m
}

func (s *Mem) PutIfAbsent(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[key]; ok {
		return ErrExists
	}
	s.m[key] = append([]byte{}, data...)
	return nil
}

func (s *Mem) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte{}, v...), nil
}

func (s *Mem) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for k := range s.m {
		if strings.HasPrefix(k, prefix) && !strings.Contains(k[len(prefix):], "/") {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *Mem) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}

// Raw exposes the object map for fault-injection tests (overwrite/corrupt).
func (m *Mem) Raw(fn func(objects map[string][]byte)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fn(m.m)
}

// Keys returns every key, sorted.
func (m *Mem) Keys(context.Context) ([]string, error) {
	m.mu.Lock()
	out := make([]string, 0, len(m.m))
	for k := range m.m {
		out = append(out, k)
	}
	m.mu.Unlock()
	sort.Strings(out)
	return out, nil
}
