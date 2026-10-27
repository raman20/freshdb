package format

import (
	"bytes"
	"testing"
)

func sample() []byte {
	b := EncodeHeader(SegHeader{Epoch: 3, Seq: 7})
	var prev [32]byte
	for i := uint64(1); i <= 3; i++ {
		f := &Frame{LSN: i, TS: int64(i * 10), Prev: prev, Ops: []Op{
			{Kind: OpPut, Space: "kv", Key: "k", Value: []byte("v")},
			{Kind: OpAppend, Subject: "u-7", Space: "topic:t", Value: []byte{1, 2, 3}},
			{Kind: OpDel, Space: "kv", Key: "x"},
		}}
		var p []byte
		b, p = AppendFrame(b, f)
		prev = ChainNext(prev, p)
	}
	return b
}

func TestRoundTrip(t *testing.T) {
	b := sample()
	h, fr, n, err := DecodeSegment(b)
	if err != nil || n != len(b) || len(fr) != 3 || h.Epoch != 3 || h.Seq != 7 {
		t.Fatalf("h=%+v n=%d/%d frames=%d err=%v", h, n, len(b), len(fr), err)
	}
	var prev [32]byte
	for i, d := range fr {
		if d.Frame.LSN != uint64(i+1) || d.Frame.Prev != prev || len(d.Frame.Ops) != 3 || d.Frame.Ops[1].Subject != "u-7" {
			t.Fatalf("frame %d mismatch: %+v", i, d.Frame)
		}
		prev = ChainNext(prev, d.Payload)
	}
}

// ValOff must locate every op's value inside the segment, both for the
// writer (AppendFrame) and for recovery (DecodeSegment).
func TestValueOffsets(t *testing.T) {
	b := EncodeHeader(SegHeader{Epoch: 1, Seq: 1})
	var written []Op
	for i := 0; i < 3; i++ {
		f := &Frame{LSN: uint64(i + 1), Ops: []Op{
			{Kind: OpPut, Space: "kv", Key: "k", Value: bytes.Repeat([]byte{byte('a' + i)}, 200*i+1)},
			{Kind: OpDel, Space: "kv", Key: "gone"},
			{Kind: OpAppend, Subject: "s", Space: "topic:t", Value: []byte("msg")},
		}}
		b, _ = AppendFrame(b, f)
		written = append(written, f.Ops...)
	}
	_, fr, _, err := DecodeSegment(b)
	if err != nil {
		t.Fatal(err)
	}
	var read []Op
	for _, d := range fr {
		read = append(read, d.Frame.Ops...)
	}
	for i, op := range written {
		if op.ValOff == 0 || op.ValOff != read[i].ValOff {
			t.Fatalf("op %d: writer offset %d, decoder offset %d", i, op.ValOff, read[i].ValOff)
		}
		if got := b[op.ValOff : int(op.ValOff)+len(op.Value)]; !bytes.Equal(got, op.Value) {
			t.Fatalf("op %d: offset %d does not point at its value", i, op.ValOff)
		}
	}
}

func TestTornTailEveryPrefix(t *testing.T) {
	b := sample()
	_, full, _, _ := DecodeSegment(b)
	for cut := SegHeaderSize; cut < len(b); cut++ {
		_, fr, n, err := DecodeSegment(b[:cut])
		// valid prefix must be a prefix of the full frames and never over-read
		if n > cut || len(fr) > len(full) {
			t.Fatalf("cut %d: n=%d frames=%d", cut, n, len(fr))
		}
		if n < cut && err != ErrTorn {
			t.Fatalf("cut %d: expected ErrTorn, got %v", cut, err)
		}
		for i := range fr {
			if !bytes.Equal(fr[i].Payload, full[i].Payload) {
				t.Fatalf("cut %d: frame %d differs", cut, i)
			}
		}
	}
}

func TestBitFlipDetected(t *testing.T) {
	b := sample()
	_, full, _, _ := DecodeSegment(b)
	for i := SegHeaderSize; i < len(b); i++ {
		c := append([]byte{}, b...)
		c[i] ^= 0x40
		_, fr, _, err := DecodeSegment(c)
		if err == nil && len(fr) == len(full) {
			same := true
			for j := range fr {
				if !bytes.Equal(fr[j].Payload, full[j].Payload) {
					same = false
				}
			}
			if !same {
				t.Fatalf("flip at %d silently changed data", i)
			}
			t.Fatalf("flip at %d undetected", i)
		}
	}
}

func FuzzDecodeSegment(f *testing.F) {
	f.Add(sample())
	f.Add([]byte(SegMagic))
	f.Add(EncodeHeader(SegHeader{1, 1}))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, fr, n, err := DecodeSegment(b)
		if n > len(b) {
			t.Fatal("over-read")
		}
		if err == nil {
			// re-encoding the decoded frames must reproduce the exact bytes
			out := EncodeHeader(h)
			for _, d := range fr {
				out, _ = AppendFrame(out, d.Frame)
			}
			if !bytes.Equal(out, b) {
				t.Fatalf("non-canonical decode accepted")
			}
		}
	})
}

func FuzzDecodePayload(f *testing.F) {
	f.Add(EncodePayload(&Frame{LSN: 1, Ops: []Op{{Kind: OpPut, Key: "a", Value: []byte("b")}}}))
	f.Fuzz(func(t *testing.T, p []byte) {
		fr, err := DecodePayload(p)
		// Canonical: anything that decodes re-encodes to exactly the same bytes,
		// so the hash chain over payload bytes is unambiguous.
		if err == nil && !bytes.Equal(EncodePayload(fr), p) {
			t.Fatalf("non-canonical payload accepted: %x", p)
		}
	})
}
