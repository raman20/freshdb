package freshdb

import (
	"container/list"
	"context"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// valueCache is a bounded, sharded LRU of value bytes read from segments,
// keyed by position. Positions are immutable, so entries never go stale.
type valueCache struct {
	shards [16]vcShard
}

type vcKey struct {
	log string
	seq uint64
	off uint32
}

type vcShard struct {
	mu    sync.Mutex
	max   int
	bytes int
	m     map[vcKey]*list.Element
	lru   list.List // front = most recent; values are *vcEnt
}

type vcEnt struct {
	k vcKey
	v []byte
}

const vcOverhead = 96 // rough per-entry bookkeeping bytes

func newValueCache(maxBytes int) *valueCache {
	if maxBytes <= 0 {
		return nil
	}
	c := &valueCache{}
	for i := range c.shards {
		c.shards[i].max = maxBytes / len(c.shards)
		c.shards[i].m = map[vcKey]*list.Element{}
	}
	return c
}

func (c *valueCache) shard(k vcKey) *vcShard {
	h := k.seq*0x9E3779B97F4A7C15 ^ uint64(k.off)*0xC2B2AE3D27D4EB4F
	return &c.shards[(h>>32)%uint64(len(c.shards))]
}

func (c *valueCache) get(k vcKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	s := c.shard(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.m[k]; ok {
		s.lru.MoveToFront(e)
		return e.Value.(*vcEnt).v, true
	}
	return nil, false
}

func (c *valueCache) put(k vcKey, v []byte) {
	if c == nil {
		return
	}
	s := c.shard(k)
	sz := len(v) + vcOverhead
	if sz > s.max/4 {
		return // never let one value flush a shard
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[k]; ok {
		return
	}
	s.m[k] = s.lru.PushFront(&vcEnt{k, v})
	s.bytes += sz
	for s.bytes > s.max {
		b := s.lru.Back()
		e := b.Value.(*vcEnt)
		s.lru.Remove(b)
		delete(s.m, e.k)
		s.bytes -= len(e.v) + vcOverhead
	}
}

// readValue returns the stored bytes of a version kept as a position in
// segment seq of log x, via the cache, verifying the lens' checksum.
func (db *DB) readValue(x *logState, seq uint64, ver lens.Version) ([]byte, error) {
	k := vcKey{x.meta.ID, seq, ver.Off}
	if b, ok := db.vcache.get(k); ok {
		return b, nil
	}
	b, err := store.GetRange(context.Background(), db.st, segKey(x.meta.ID, seq), int64(ver.Off), int64(ver.Len))
	if err != nil {
		return nil, fmt.Errorf("freshdb: reading value at LSN %d: %w", ver.LSN, err)
	}
	if crc32.Checksum(b, lens.Castagnoli) != ver.Sum {
		return nil, fmt.Errorf("%w: value at LSN %d (segment %s/%d offset %d) fails its checksum", ErrCorrupt, ver.LSN, x.meta.ID, seq, ver.Off)
	}
	db.vcache.put(k, b)
	return b, nil
}
