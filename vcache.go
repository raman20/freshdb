package freshdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/maphash"
	"sync"

	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// valueCache is a bounded, sharded cache of value bytes read from
// segments, keyed by position (log, segment, offset). Positions are
// immutable, so entries never go stale.
//
// Each shard is a ring buffer of entries in one arena allocated outside the
// Go heap where the platform allows it (anonymous mmap on Unix), plus a
// map from a 64-bit key hash to the entry's position. So the cache costs
// about its configured size in RSS, adds no per-entry objects for the GC to
// track, and does not inflate the heap target. Writes append at the head;
// the oldest entries are overwritten (FIFO), except that a hit on an entry
// in the oldest quarter re-appends it (second chance), which keeps hot
// values resident. Reads copy the bytes out, so callers own the result.
type valueCache struct {
	seed   maphash.Seed
	free   func() // releases the one arena all shards slice
	once   sync.Once
	shards [vcShards]vcShard
}

const (
	vcShards = 64
	vcHdr    = 6 // klen u16, vlen u32
)

type vcKey struct {
	log string
	seq uint64
	off uint32
}

type vcShard struct {
	mu     sync.RWMutex
	ring   []byte
	head   uint64            // logical bytes written so far
	swept  uint64            // head at the last stale-index sweep
	m      map[uint64]uint64 // key hash -> logical position of the entry
	closed bool
	_      [32]byte // keep neighbouring shards' locks off one cache line
}

func newValueCache(maxBytes int) *valueCache {
	if maxBytes <= 0 {
		return nil
	}
	c := &valueCache{seed: maphash.MakeSeed()}
	per := max(maxBytes/len(c.shards), 4096)
	arena, free := allocArena(per * len(c.shards))
	c.free = free
	for i := range c.shards {
		c.shards[i].ring = arena[i*per : (i+1)*per : (i+1)*per]
		c.shards[i].m = map[uint64]uint64{}
	}
	return c
}

// keyBytes encodes k into buf (log ID, seq, offset).
func (k vcKey) bytes(buf []byte) []byte {
	buf = append(buf[:0], k.log...)
	buf = binary.LittleEndian.AppendUint64(buf, k.seq)
	return binary.LittleEndian.AppendUint32(buf, k.off)
}

func (c *valueCache) locate(k vcKey, buf []byte) (*vcShard, uint64, []byte) {
	kb := k.bytes(buf)
	h := maphash.Bytes(c.seed, kb)
	return &c.shards[h%uint64(len(c.shards))], h, kb
}

// live reports whether the entry written at logical position p has not
// been overwritten: the ring holds logical bytes [head-len, head), and an
// entry never wraps, so its start being in range means all of it is.
func (s *vcShard) live(p uint64) bool {
	return p+uint64(len(s.ring)) >= s.head
}

// get returns a copy of the cached bytes.
func (c *valueCache) get(k vcKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	var buf [64]byte
	s, h, kb := c.locate(k, buf[:0])
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, false
	}
	p, ok := s.m[h]
	var out []byte
	refresh := false
	if ok && s.live(p) {
		o := p % uint64(len(s.ring))
		kl := uint64(binary.LittleEndian.Uint16(s.ring[o:]))
		vl := uint64(binary.LittleEndian.Uint32(s.ring[o+2:]))
		size := vcHdr + kl + vl
		if bytes.Equal(s.ring[o+vcHdr:o+vcHdr+kl], kb) {
			out = bytes.Clone(s.ring[o+vcHdr+kl : o+size])
			if out == nil {
				out = []byte{}
			}
			refresh = s.head-p > uint64(len(s.ring))*3/4
		}
	}
	s.mu.RUnlock()
	if out == nil {
		return nil, false
	}
	if refresh { // second chance: copy it to the head before it is overwritten
		c.put(k, out)
	}
	return out, true
}

// put stores a copy of v (callers may keep using v).
func (c *valueCache) put(k vcKey, v []byte) {
	if c == nil {
		return
	}
	var buf [64]byte
	s, h, kb := c.locate(k, buf[:0])
	n := uint64(len(s.ring))
	size := uint64(vcHdr + len(kb) + len(v))
	if size > n/4 || len(kb) > 0xffff {
		return // never let one value flush a shard
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if p, ok := s.m[h]; ok && s.live(p) && s.head-p <= n*3/4 {
		return // present and not about to be overwritten
	}
	if o := s.head % n; o+size > n {
		s.head += n - o // never wrap an entry: skip to the start
	}
	o := s.head % n
	binary.LittleEndian.PutUint16(s.ring[o:], uint16(len(kb)))
	binary.LittleEndian.PutUint32(s.ring[o+2:], uint32(len(v)))
	copy(s.ring[o+vcHdr:], kb)
	copy(s.ring[o+vcHdr+uint64(len(kb)):], v)
	s.m[h] = s.head
	s.head += size
	if s.head-s.swept >= n/4 { // drop index entries whose bytes are gone
		for kh, p := range s.m {
			if p+n < s.head {
				delete(s.m, kh)
			}
		}
		s.swept = s.head
	}
}

// close releases the arenas; later gets miss and puts are ignored.
func (c *valueCache) close() {
	if c == nil {
		return
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.closed = true // ring itself is never reassigned: put reads its
		s.m = nil       // length unlocked; only closed guards the bytes
		s.mu.Unlock()
	}
	// Every shard is closed under its lock, so no reader can still be
	// inside the arena.
	c.once.Do(func() {
		if c.free != nil {
			c.free()
		}
	})
}

// readValue returns the stored bytes of a version kept as a position in
// segment seq of log x, via the cache, verifying the lens' checksum. The
// result is a private copy the caller may modify.
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
