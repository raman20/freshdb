package lens

import (
	"bytes"
	"testing"
)

// kvSeed is a small KV with every kind of version: inline, empty and nil
// values, positions, deletes, subjects, base chunks (KV3).
func kvSeed(chunk bool) *KV {
	l := NewKVInline(4)
	l.put(ck("kv", "a"), Version{LSN: 1, Val: []byte("x")})
	l.put(ck("kv", "a"), Version{LSN: 3, Off: 9, Len: 100, Sum: 7, Op: 1})
	l.put(ck("kv", "b"), Version{LSN: 2, Val: []byte{}, Subject: "s"})
	l.put(ck("kv", "b"), Version{LSN: 4, Del: true})
	l.put(ck("doc:c", "k"), Version{LSN: 5})
	if chunk {
		l.put(ck("kv", "z"), Version{LSN: 6, Off: 3, Len: 9, Chunk: 2})
	}
	return l
}

// FuzzLoadKV: a snapshot or base either fails to load or loads into a lens
// that re-encodes to the same bytes and serves every read without a panic.
func FuzzLoadKV(f *testing.F) {
	f.Add(kvSeed(false).Snapshot())
	f.Add(kvSeed(true).Snapshot())
	f.Add(NewKVInline(4).Snapshot())
	f.Fuzz(func(t *testing.T, b []byte) {
		for _, strict := range []bool{true, false} {
			l, err := loadKV(b, 4, strict)
			if err != nil {
				continue
			}
			want := b
			if !strict { // a base keeps its own inline limit field
				want = append([]byte{}, b...)
				copy(want[11:19], l.Snapshot()[11:19])
			}
			if got := l.Snapshot(); !bytes.Equal(got, want) {
				t.Fatalf("loaded bytes do not re-encode identically (strict=%v)", strict)
			}
			l.Latest(func(space, key string, v Version) error {
				l.Get(space, key, v.LSN)
				l.Get(space, key, v.LSN-1)
				l.LastWrite(space, key)
				l.Scan(space, key[:len(key)/2], ^uint64(0))
				return nil
			})
			l.DropSubject("s")
			l.Digest()
		}
	})
}

// TestLoadKVRejectsMalformed: B-D1. Each case was accepted before the fix;
// the first one then panicked Latest ("slice bounds out of range").
func TestLoadKVRejectsMalformed(t *testing.T) {
	type kv struct {
		k string
		v []Version
	}
	build := func(f string, inline uint64, keys ...kv) []byte {
		e := &enc{}
		e.str(f)
		e.u64(inline)
		e.u64(uint64(len(keys)))
		for _, x := range keys {
			e.str(x.k)
			e.u64(uint64(len(x.v)))
			for _, v := range x.v {
				e.u64(v.LSN)
				e.u64(uint64(v.Off)<<32 | uint64(v.Len))
				e.u64(uint64(v.Op)<<32 | uint64(v.Sum))
				if f == "KV3" {
					e.u64(uint64(v.Chunk))
				}
				e.bytes(v.Val)
				e.str(v.Subject)
				e.bool(v.Del)
			}
		}
		return e.b
	}
	v1 := []Version{{LSN: 1, Val: []byte("x")}}
	good := build("KV2", 4, kv{"kv\x00a", v1}, kv{"kv\x00b", v1})
	if _, err := loadKV(build("KV2", 4, kv{"kv\x00a", []Version{{LSN: 2}, {LSN: 2, Op: 1}}}), 4, true); err != nil {
		t.Fatalf("one entry writing a key twice rejected: %v", err)
	}
	if _, err := loadKV(good, 4, true); err != nil {
		t.Fatalf("well-formed snapshot rejected: %v", err)
	}
	notCanon := func(b []byte, at int, c byte) []byte {
		b = append([]byte{}, b...)
		b[at] = c
		return b
	}
	for name, b := range map[string][]byte{
		"key without separator": build("KV2", 4, kv{"nokeysep", v1}),
		"duplicate key":         build("KV2", 4, kv{"kv\x00a", v1}, kv{"kv\x00a", v1}),
		"keys out of order":     build("KV2", 4, kv{"kv\x00b", v1}, kv{"kv\x00a", v1}),
		"versions out of order": build("KV2", 4, kv{"kv\x00a", []Version{{LSN: 2}, {LSN: 1}}}),
		"repeated version":      build("KV2", 4, kv{"kv\x00a", []Version{{LSN: 2, Op: 1}, {LSN: 2, Op: 1}}}),
		"ops out of order":      build("KV2", 4, kv{"kv\x00a", []Version{{LSN: 2, Op: 1}, {LSN: 2}}}),
		"no versions":           build("KV2", 4, kv{"kv\x00a", nil}),
		"KV3 without chunks":    build("KV3", 4, kv{"kv\x00a", v1}),
		"bool not 0 or 1":       notCanon(good, len(good)-1, 2),
		"inline mismatch":       build("KV2", 5, kv{"kv\x00a", v1}),
		"trailing bytes":        append(append([]byte{}, good...), 0),
		"truncated":             good[:len(good)-1],
		"bad format":            build("KV9", 4),
	} {
		if _, err := loadKV(b, 4, true); err != ErrBadSnapshot {
			t.Errorf("%s: err = %v, want ErrBadSnapshot", name, err)
		}
	}
	// A nil subject marker is not a string.
	e := &enc{}
	e.str("KV2")
	e.u64(4)
	e.u64(1)
	e.u64(^uint64(0))
	if _, err := loadKV(e.b, 4, true); err != ErrBadSnapshot {
		t.Errorf("nil key string: err = %v", err)
	}
	// A base may carry another inline limit.
	if _, err := loadKV(build("KV2", 5, kv{"kv\x00a", v1}), 4, false); err != nil {
		t.Errorf("base with another inline limit: %v", err)
	}
}
