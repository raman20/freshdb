package lens

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"testing"
)

// FuzzDecodeVec: DecodeVec never panics, accepts exactly the non-empty,
// 4-byte-aligned, finite encodings, and round-trips what it accepts.
func FuzzDecodeVec(f *testing.F) {
	f.Add([]byte{})
	f.Add(EncodeVec([]float32{1, -2.5, 0}))
	f.Add(EncodeVec([]float32{float32(math.NaN())}))
	f.Add(EncodeVec([]float32{float32(math.Inf(-1))}))
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, b []byte) {
		v, ok := DecodeVec(b)
		if !ok {
			if len(b) > 0 && len(b)%4 == 0 {
				finite := true
				for i := 0; i < len(b); i += 4 {
					x := float64(math.Float32frombits(uint32(b[i]) | uint32(b[i+1])<<8 | uint32(b[i+2])<<16 | uint32(b[i+3])<<24))
					finite = finite && !math.IsNaN(x) && !math.IsInf(x, 0)
				}
				if finite {
					t.Fatalf("rejected a finite encoding %x", b)
				}
			}
			return
		}
		if !bytes.Equal(EncodeVec(v), b) {
			t.Fatalf("round trip %x -> %v", b, v)
		}
		if u, ok := Normalize(v); ok {
			var s float64
			for _, x := range u {
				if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
					t.Fatalf("normalize %v -> %v", v, u)
				}
				s += float64(x) * float64(x)
			}
			if math.Abs(s-1) > 1e-5 {
				t.Fatalf("|normalize(%v)|^2 = %v", v, s)
			}
		}
	})
}

// FuzzDecodeANN: a snapshot blob from storage never panics the decoder,
// and whatever it accepts restores into a graph that passes CheckANN
// (restore re-checks every node against the lens's vectors).
func FuzzDecodeANN(f *testing.F) {
	l := NewVector()
	l.EnableANN("c", ANNParams{M: 4, EfConstruction: 8})
	for i, v := range [][]float32{{1, 0}, {0, 1}, {1, 1}, {-1, 0.5}, {0.2, -1}} {
		l.add("c", string(rune('a'+i)), "", mustUnit(v))
	}
	good := l.MarshalANN()
	f.Add(good)
	f.Add(good[:len(good)-5])
	f.Add([]byte(annMagic + "\x01\x00\x00\x00\x00\x00\x00\x00"))
	f.Fuzz(func(t *testing.T, b []byte) {
		// The blob is CRC-checked; fix the CRC up so the fuzzer reaches the
		// parser (a blob written by a buggy or hostile writer).
		if len(b) >= len(annMagic)+8 {
			b = append([]byte(annMagic), b[len(annMagic):]...)
			b = b[:len(b)-4]
			b = appendCRC(b)
		}
		snap, err := DecodeANN(b)
		if err != nil {
			return
		}
		m := NewVector()
		for i, v := range [][]float32{{1, 0}, {0, 1}, {1, 1}, {-1, 0.5}, {0.2, -1}} {
			m.add("c", string(rune('a'+i)), "", mustUnit(v))
		}
		m.EnableANNFrom("c", ANNParams{M: 4, EfConstruction: 8}, snap)
		if err := m.CheckANN(); err != nil {
			t.Fatalf("restored graph: %v", err)
		}
		if hs, ok := m.ANN("c", mustUnit([]float32{1, 0}), 3, 8, 0); !ok || len(hs) == 0 {
			t.Fatalf("restored graph finds nothing: %v %v", hs, ok)
		}
	})
}

func mustUnit(v []float32) []float32 {
	u, ok := normalize(v)
	if !ok {
		panic("zero vector")
	}
	return u
}

func appendCRC(b []byte) []byte {
	return binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
}

// Hand-made blobs with a valid CRC that used to panic DecodeANN: a node
// count or level count past MaxInt64 (negative after int()), and a link
// count whose 4*c overflows.
func TestDecodeANNHostile(t *testing.T) {
	uv := func(x uint64) []byte { return binary.AppendUvarint(nil, x) }
	head := func(fields ...uint64) []byte {
		b := append([]byte(annMagic), 1, 0, 0, 0)
		b = append(b, uv(1)...)
		b = append(b, 'c')
		for _, x := range fields {
			b = append(b, uv(x)...)
		}
		return b
	}
	cases := map[string][]byte{
		"nodes past MaxInt64": head(2, 4, 8, 8, 1<<63+5, 0, 0),
		"levels past MaxInt64": append(append(head(2, 4, 8, 8, 1, 0, 1), uv(1)...),
			append([]byte{'a', 0, 0, 0, 0, 0, 0, 0, 0}, uv(1<<63+1)...)...),
		"link count overflows 4*c": append(append(head(2, 4, 8, 8, 1, 0, 1), uv(1)...),
			append(append([]byte{'a', 0, 0, 0, 0, 0, 0, 0, 0}, uv(1)...), uv(1<<62+1)...)...),
		"entry past int32": head(2, 4, 8, 8, 0, 0, 1<<40),
	}
	for name, b := range cases {
		if _, err := DecodeANN(appendCRC(append(b, 0, 0, 0, 0, 0, 0, 0, 0))); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A deleted (or shredded) vector leaves no copy in the slab's spare
// capacity, with or without a graph.
func TestVectorDeleteLeavesNoResidue(t *testing.T) {
	for _, withANN := range []bool{false, true} {
		l := NewVector()
		if withANN {
			l.EnableANN("c", ANNParams{})
		}
		l.add("c", "a", "", mustUnit([]float32{3, 4}))
		l.add("c", "b", "s", mustUnit([]float32{4, 3}))
		l.add("c", "c", "", mustUnit([]float32{0, 1}))
		l.del("c", "c") // the last slot
		l.DropSubject("s")
		sl := l.m["c"].slabs[2]
		for i, x := range sl.data[len(sl.data):cap(sl.data)] {
			if x != 0 {
				t.Fatalf("ann=%v: spare slot %d holds %v", withANN, i, x)
			}
		}
		for _, s := range sl.subj[len(sl.subj):cap(sl.subj)] {
			if s != "" {
				t.Fatalf("ann=%v: spare subject %q", withANN, s)
			}
		}
	}
}

// ANN caps k and ef at the collection size (both may come from a request).
func TestANNHugeKEf(t *testing.T) {
	l := NewVector()
	l.EnableANN("c", ANNParams{M: 4})
	for i := 0; i < 50; i++ {
		l.add("c", itoa(i), "", mustUnit([]float32{float32(i%7) + 1, float32(i % 5)}))
	}
	hs, ok := l.ANN("c", mustUnit([]float32{1, 1}), 1<<40, 1<<40, 0)
	// not all 50: this data is mostly exact duplicates, and the neighbor
	// heuristic can leave a duplicate with no incoming link (vectors.md)
	if !ok || len(hs) == 0 || len(hs) > 50 {
		t.Fatalf("%d hits, %v", len(hs), ok)
	}
}
