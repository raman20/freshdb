package store

import (
	"context"
	"fmt"
	"os"
	"sync"
)

// RangeReader is implemented by stores that can read part of an object.
type RangeReader interface {
	// GetRange returns n bytes of key starting at off. Reading past the end
	// of the object is an error.
	GetRange(ctx context.Context, key string, off, n int64) ([]byte, error)
}

// GetRange reads part of an object, using RangeReader when st has it and a
// whole-object Get otherwise.
func GetRange(ctx context.Context, st Store, key string, off, n int64) ([]byte, error) {
	if r, ok := st.(RangeReader); ok {
		return r.GetRange(ctx, key, off, n)
	}
	b, err := st.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return sliceRange(key, b, off, n)
}

func sliceRange(key string, b []byte, off, n int64) ([]byte, error) {
	if off < 0 || n < 0 || off+n > int64(len(b)) {
		return nil, fmt.Errorf("store: range %d+%d outside %s (%d bytes)", off, n, key, len(b))
	}
	return append([]byte{}, b[off:off+n]...), nil
}

// fdCache keeps a bounded set of read-only file handles (objects are
// immutable, so a handle stays valid until the object is deleted).
type fdCache struct {
	shards [16]fdShard
}

type fdShard struct {
	mu sync.Mutex
	m  map[string]*fdEnt
}

type fdEnt struct {
	f    *os.File
	refs int
	dead bool
}

const fdPerShard = 32

func (c *fdCache) shard(p string) *fdShard {
	h := uint32(2166136261) // FNV-1a
	for i := 0; i < len(p); i++ {
		h = (h ^ uint32(p[i])) * 16777619
	}
	return &c.shards[h%uint32(len(c.shards))]
}

func (c *fdCache) readAt(p string, off, n int64) ([]byte, error) {
	s := c.shard(p)
	s.mu.Lock()
	e := s.m[p]
	if e == nil {
		f, err := os.Open(p)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		if s.m == nil {
			s.m = map[string]*fdEnt{}
		}
		for k, v := range s.m { // evict an arbitrary handle when full
			if len(s.m) < fdPerShard {
				break
			}
			delete(s.m, k)
			v.dead = true
			if v.refs == 0 {
				v.f.Close()
			}
		}
		e = &fdEnt{f: f}
		s.m[p] = e
	}
	e.refs++
	s.mu.Unlock()
	b := make([]byte, n)
	m, err := e.f.ReadAt(b, off)
	s.mu.Lock()
	e.refs--
	if e.dead && e.refs == 0 {
		e.f.Close()
	}
	s.mu.Unlock()
	if err != nil || int64(m) != n {
		return nil, fmt.Errorf("store: range %d+%d of %s: read %d: %v", off, n, p, m, err)
	}
	return b, nil
}

func (c *fdCache) drop(p string) {
	s := c.shard(p)
	s.mu.Lock()
	if e := s.m[p]; e != nil {
		delete(s.m, p)
		e.dead = true
		if e.refs == 0 {
			e.f.Close()
		}
	}
	s.mu.Unlock()
}

// GetRange implements RangeReader with pread on a cached handle.
func (s *FS) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	if off < 0 || n < 0 {
		return nil, fmt.Errorf("store: bad range %d+%d", off, n)
	}
	b, err := s.fds.readAt(p, off, n)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	return b, err
}

// GetRange implements RangeReader.
func (s *Mem) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	if !ok {
		return nil, ErrNotFound
	}
	return sliceRange(key, v, off, n)
}

// GetRange implements RangeReader (reads can fail like Get).
func (f *Faulty) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.roll(f.P.ReadFail) {
		return nil, ErrInjected
	}
	return GetRange(ctx, f.Inner, key, off, n)
}
