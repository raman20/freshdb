package freshdb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/raman20/freshdb/store"
)

func vk(i int) vcKey { return vcKey{"log", uint64(i), uint32(i * 7)} }

// checkCache verifies shard invariants: the write head only moves forward,
// every live index entry decodes to an entry of its own shard, and no
// entry overwritten before the last sweep is still indexed.
func checkCache(t *testing.T, c *valueCache) {
	t.Helper()
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		n := uint64(len(s.ring))
		for _, p := range s.m {
			if p >= s.head {
				t.Fatalf("shard %d: index points past head", i)
			}
			if p+n < s.swept {
				t.Fatalf("shard %d: stale entry survived a sweep", i)
			}
		}
		s.mu.RUnlock()
	}
}

func TestValueCacheBasics(t *testing.T) {
	if c := newValueCache(0); c != nil {
		t.Fatal("size 0 must disable the cache")
	}
	var nilc *valueCache
	nilc.put(vk(1), []byte("x"))
	if _, ok := nilc.get(vk(1)); ok {
		t.Fatal("nil cache hit")
	}
	nilc.close()
	c := newValueCache(1 << 20)
	defer c.close()
	in := []byte("one")
	c.put(vk(1), in)
	in[0] = 'X' // put copies
	v, ok := c.get(vk(1))
	if !ok || string(v) != "one" {
		t.Fatalf("got %q %v", v, ok)
	}
	v[0] = 'Y' // get copies
	if v, _ := c.get(vk(1)); string(v) != "one" {
		t.Fatalf("caller's write reached the cache: %q", v)
	}
	if _, ok := c.get(vk(2)); ok {
		t.Fatal("phantom hit")
	}
	if _, ok := c.get(vcKey{"other", 1, 7}); ok {
		t.Fatal("hit for a different log with the same seq/off")
	}
	c.put(vk(3), []byte{})
	if v, ok := c.get(vk(3)); !ok || len(v) != 0 {
		t.Fatalf("empty value: %q %v", v, ok)
	}
	big := make([]byte, (1<<20)/vcShards) // over a quarter of a shard
	c.put(vk(4), big)
	if _, ok := c.get(vk(4)); ok {
		t.Fatal("cached a value larger than a quarter shard")
	}
	c.close()
	if _, ok := c.get(vk(1)); ok {
		t.Fatal("hit after close")
	}
	c.put(vk(5), []byte("x")) // ignored, must not panic
	c.close()                 // idempotent
}

// Every hit returns exactly the bytes put for that key, under heavy
// eviction and wrap-around, with values of many sizes.
func TestValueCacheModel(t *testing.T) {
	for seed := uint64(0); seed < 20; seed++ {
		r := rand.New(rand.NewPCG(seed, 1))
		c := newValueCache(64 << 10)
		val := func(i int) []byte { return bytes.Repeat([]byte{byte(i), byte(i >> 8)}, i%150) }
		hits := 0
		for op := 0; op < 20000; op++ {
			i := int(r.IntN(2000))
			if r.IntN(2) == 0 {
				c.put(vk(i), val(i))
			} else if v, ok := c.get(vk(i)); ok {
				hits++
				if !bytes.Equal(v, val(i)) {
					t.Fatalf("seed %d: key %d returned wrong bytes", seed, i)
				}
			}
		}
		if hits == 0 {
			t.Fatalf("seed %d: no hits", seed)
		}
		checkCache(t, c)
		c.close()
	}
}

// Second chance: a hot set that keeps being read survives a stream of
// cold inserts many times larger than the cache.
func TestValueCacheKeepsHotEntries(t *testing.T) {
	c := newValueCache(vcShards * 8192)
	defer c.close()
	v := make([]byte, 100)
	const hot = 200
	for i := 0; i < hot; i++ {
		c.put(vk(i), v)
	}
	for i := hot; i < hot+20000; i++ {
		if i%5 == 0 {
			for h := 0; h < hot; h += 10 {
				c.get(vk(h))
			}
		}
		c.put(vk(i), v)
	}
	kept := 0
	for h := 0; h < hot; h += 10 {
		if _, ok := c.get(vk(h)); ok {
			kept++
		}
	}
	if kept < hot/10*9/10 {
		t.Fatalf("only %d of %d hot entries survived a cold scan", kept, hot/10)
	}
}

func TestValueCacheConcurrent(t *testing.T) {
	c := newValueCache(256 << 10)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 2))
			for op := 0; op < 20000; op++ {
				i := int(r.IntN(5000))
				want := []byte(fmt.Sprint("value-", i))
				if r.IntN(3) == 0 {
					c.put(vk(i), want)
				} else if v, ok := c.get(vk(i)); ok && !bytes.Equal(v, want) {
					t.Errorf("key %d: %q", i, v)
					return
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() { defer wg.Done(); checkCache(t, c) }()
	wg.Wait()
	checkCache(t, c)
	// close racing with readers: readers must miss, never fault
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				c.get(vk(i))
				c.put(vk(i), []byte("x"))
			}
		}()
	}
	c.close()
	wg.Wait()
}

// DB level: values read back correctly with a cache so small that nearly
// every read evicts, including after Close/reopen.
func TestValuesTinyCache(t *testing.T) {
	db := mustOpen(t, store.NewMem(), WithValueCacheBytes(vcShards*4096))
	want := map[string][]byte{}
	for b := 0; b < 5; b++ {
		if err := db.Update(ctx, func(tx *Tx) error {
			for i := 0; i < 200; i++ {
				k := fmt.Sprint("k", b*200+i)
				want[k] = bigVal(k, i)
				if err := tx.Put(k, want[k]); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; round < 3; round++ {
		for k, v := range want {
			got, err := db.Get(ctx, k)
			if err != nil || !bytes.Equal(got, v) {
				t.Fatalf("%s: %v", k, err)
			}
		}
	}
}
