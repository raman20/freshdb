package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

// benchSegment: 64 frames of 16 ops with 100-byte values, about one
// group-commit batch under load.
func benchSegment() []byte {
	seg := EncodeHeader(SegHeader{Epoch: 1, Seq: 1})
	for i := 0; i < 64; i++ {
		f := &Frame{LSN: uint64(i + 1), TS: int64(i)}
		for j := 0; j < 16; j++ {
			f.Ops = append(f.Ops, Op{Kind: OpPut, Space: "kv", Key: fmt.Sprintf("key-%d-%d", i, j), Value: make([]byte, 100)})
		}
		seg, _ = AppendFrame(seg, f)
	}
	return seg
}

func BenchmarkDecodeSegment(b *testing.B) {
	seg := benchSegment()
	b.SetBytes(int64(len(seg)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, _, err := DecodeSegment(seg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendFrame(b *testing.B) {
	f := &Frame{LSN: 1}
	for j := 0; j < 16; j++ {
		f.Ops = append(f.Ops, Op{Kind: OpPut, Space: "kv", Key: fmt.Sprintf("key-%d", j), Value: make([]byte, 100)})
	}
	buf := make([]byte, 0, 1<<16)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, _ = AppendFrame(buf[:0], f)
	}
}

func BenchmarkChainNext(b *testing.B) {
	p := make([]byte, 2048)
	var h [32]byte
	b.SetBytes(int64(len(p)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h = ChainNext(h, p)
	}
}

// AppendFrame encodes in place; it must equal header + EncodePayload, and
// every ValOff must point at its value inside the returned buffer.
func TestAppendFrameMatchesEncodePayload(t *testing.T) {
	f := &Frame{LSN: 7, TS: 9, Ops: []Op{
		{Kind: OpPut, Subject: "s", Space: "kv", Key: "a", Value: []byte("hello")},
		{Kind: OpDel, Space: "kv", Key: "b"},
		{Kind: OpPut, Space: "docs", Key: "c", Value: make([]byte, 300)},
	}}
	pre := []byte("prefix--")
	out, p := AppendFrame(append([]byte{}, pre...), f)
	want := EncodePayload(&Frame{LSN: 7, TS: 9, Ops: append([]Op{}, f.Ops...)})
	if string(p) != string(want) || string(out[len(pre)+FrameHdrSize:]) != string(want) {
		t.Fatal("payload differs from EncodePayload")
	}
	if len(p) != payloadSize(f) {
		t.Fatalf("payloadSize %d, encoded %d", payloadSize(f), len(p))
	}
	for _, op := range f.Ops {
		if got := out[op.ValOff : int(op.ValOff)+len(op.Value)]; string(got) != string(op.Value) {
			t.Fatalf("ValOff %d of %q wrong", op.ValOff, op.Key)
		}
	}
	_, fr, _, err := DecodeSegment(append(EncodeHeader(SegHeader{1, 1}), out[len(pre):]...))
	if err != nil || len(fr) != 1 || fr[0].Frame.Ops[2].Space != "docs" {
		t.Fatalf("decode: %v", err)
	}
}

func TestHeaderReservedBytesRejected(t *testing.T) {
	b := EncodeHeader(SegHeader{Epoch: 3, Seq: 4})
	b[10] = 1
	binary.LittleEndian.PutUint32(b[28:], CRC(b[:28]))
	if _, err := DecodeHeader(b); !errors.Is(err, ErrHeader) {
		t.Fatalf("reserved byte set: %v, want ErrHeader", err)
	}
}
