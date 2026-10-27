// Package format defines freshdb's on-storage log format: immutable segments
// containing CRC32C-checksummed frames. One frame = one committed transaction
// (an atomic batch of ops) at one LSN. Frames carry the previous hash-chain
// head so the log is tamper-evident; the chain is computed over the stored
// (possibly encrypted) bytes, so crypto-shredding never breaks verification.
package format

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC returns the CRC32C of b.
func CRC(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// Op kinds. The log has exactly four verbs; every model is a lens over them.
type Kind uint8

const (
	OpPut    Kind = 1 // set (space,key) = value
	OpDel    Kind = 2 // delete (space,key)
	OpAppend Kind = 3 // append value to stream `space` (topics)
	OpShred  Kind = 4 // audit marker: subject (Key) was crypto-shredded
)

func (k Kind) String() string {
	switch k {
	case OpPut:
		return "PUT"
	case OpDel:
		return "DEL"
	case OpAppend:
		return "APPEND"
	case OpShred:
		return "SHRED"
	}
	return fmt.Sprintf("KIND(%d)", uint8(k))
}

// Op is one operation inside a transaction frame.
type Op struct {
	Kind    Kind
	Subject string // "" = not subject-scoped; else Value is AES-GCM ciphertext under the subject DEK
	Space   string
	Key     string
	Value   []byte
	// ValOff is not encoded: it is the byte offset of Value inside the
	// segment object that holds the frame (set by AppendFrame and
	// DecodeSegment; relative to the payload after EncodePayload or
	// DecodePayload alone; 0 = unknown). It lets lenses keep a position
	// instead of a copy of the value.
	ValOff uint32
}

// Frame is one committed transaction.
type Frame struct {
	LSN  uint64
	TS   int64 // commit timestamp (from the DB clock), part of the log => deterministic for lenses
	Prev [32]byte
	Ops  []Op
}

const (
	SegMagic      = "FRDBSEG1"
	SegVersion    = 1
	SegHeaderSize = 8 + 2 + 2 + 8 + 8 + 4
	FrameHdrSize  = 8
	MaxFrameSize  = 64 << 20
	maxOps        = 1 << 20
)

var (
	ErrTorn    = errors.New("format: torn or corrupt tail")
	ErrHeader  = errors.New("format: bad segment header")
	ErrCorrupt = errors.New("format: corrupt frame")
)

// SegHeader is the fixed header of a segment object.
type SegHeader struct {
	Epoch uint64 // writer epoch that created the segment (fencing)
	Seq   uint64 // dense sequence number within the log, starting at 1
}

func EncodeHeader(h SegHeader) []byte {
	b := make([]byte, SegHeaderSize)
	copy(b, SegMagic)
	binary.LittleEndian.PutUint16(b[8:], SegVersion)
	binary.LittleEndian.PutUint64(b[12:], h.Epoch)
	binary.LittleEndian.PutUint64(b[20:], h.Seq)
	binary.LittleEndian.PutUint32(b[28:], CRC(b[:28]))
	return b
}

func DecodeHeader(b []byte) (SegHeader, error) {
	if len(b) < SegHeaderSize || string(b[:8]) != SegMagic {
		return SegHeader{}, ErrHeader
	}
	if binary.LittleEndian.Uint32(b[28:]) != CRC(b[:28]) {
		return SegHeader{}, ErrHeader
	}
	if v := binary.LittleEndian.Uint16(b[8:]); v != SegVersion {
		return SegHeader{}, fmt.Errorf("%w: unsupported version %d (refusing to guess)", ErrHeader, v)
	}
	return SegHeader{Epoch: binary.LittleEndian.Uint64(b[12:]), Seq: binary.LittleEndian.Uint64(b[20:])}, nil
}

func putStr(b []byte, s []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// EncodePayload encodes a frame body (without the len/crc frame header).
// It records each op's value offset (relative to the payload) in f.Ops.
func EncodePayload(f *Frame) []byte {
	b := make([]byte, 0, 64)
	b = binary.LittleEndian.AppendUint64(b, f.LSN)
	b = binary.LittleEndian.AppendUint64(b, uint64(f.TS))
	b = append(b, f.Prev[:]...)
	b = binary.AppendUvarint(b, uint64(len(f.Ops)))
	for i, op := range f.Ops {
		b = append(b, byte(op.Kind))
		b = putStr(b, []byte(op.Subject))
		b = putStr(b, []byte(op.Space))
		b = putStr(b, []byte(op.Key))
		b = binary.AppendUvarint(b, uint64(len(op.Value)))
		f.Ops[i].ValOff = uint32(len(b))
		b = append(b, op.Value...)
	}
	return b
}

func rebase(ops []Op, base int) {
	for i := range ops {
		ops[i].ValOff += uint32(base)
	}
}

// AppendFrame appends a framed (len|crc|payload) encoding of f to dst and
// returns the new slice plus the payload (for hash chaining).
func AppendFrame(dst []byte, f *Frame) ([]byte, []byte) {
	p := EncodePayload(f)
	rebase(f.Ops, len(dst)+FrameHdrSize)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(p)))
	dst = binary.LittleEndian.AppendUint32(dst, CRC(p))
	dst = append(dst, p...)
	return dst, p
}

type reader struct {
	b   []byte
	err bool
}

func (r *reader) u64() uint64 {
	if len(r.b) < 8 {
		r.err = true
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

func (r *reader) uv() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 || n != uvarintLen(v) { // reject non-minimal encodings: one payload, one byte string
		r.err = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func (r *reader) bytes() []byte {
	n := r.uv()
	if r.err || n > uint64(len(r.b)) {
		r.err = true
		return nil
	}
	v := r.b[:n:n]
	r.b = r.b[n:]
	return v
}

// DecodePayload decodes a frame body. It never panics on arbitrary input.
func DecodePayload(p []byte) (*Frame, error) {
	r := &reader{b: p}
	f := &Frame{}
	f.LSN = r.u64()
	f.TS = int64(r.u64())
	if r.err || len(r.b) < 32 {
		return nil, ErrCorrupt
	}
	copy(f.Prev[:], r.b[:32])
	r.b = r.b[32:]
	n := r.uv()
	if r.err || n > maxOps || n > uint64(len(r.b)) {
		return nil, ErrCorrupt
	}
	f.Ops = make([]Op, 0, n)
	for i := uint64(0); i < n; i++ {
		if len(r.b) < 1 {
			return nil, ErrCorrupt
		}
		k := Kind(r.b[0])
		r.b = r.b[1:]
		if k < OpPut || k > OpShred {
			return nil, ErrCorrupt
		}
		subj, space, key := r.bytes(), r.bytes(), r.bytes()
		valOff := len(p) - len(r.b)
		val := r.bytes()
		if r.err {
			return nil, ErrCorrupt
		}
		var v []byte
		if val != nil {
			v = append([]byte{}, val...)
		}
		f.Ops = append(f.Ops, Op{Kind: k, Subject: string(subj), Space: string(space), Key: string(key), Value: v, ValOff: uint32(valOff + uvarintLen(uint64(len(val))))})
	}
	if len(r.b) != 0 {
		return nil, ErrCorrupt
	}
	return f, nil
}

// Decoded is one frame plus its raw payload (for chain verification).
type Decoded struct {
	Frame   *Frame
	Payload []byte
}

// DecodeSegment decodes a whole segment. It returns every frame up to the
// first damaged one, the number of valid bytes, and ErrTorn if anything after
// that is not a valid frame (a torn or corrupt tail). It never panics.
func DecodeSegment(b []byte) (SegHeader, []Decoded, int, error) {
	h, err := DecodeHeader(b)
	if err != nil {
		return h, nil, 0, err
	}
	off := SegHeaderSize
	var out []Decoded
	for off < len(b) {
		if len(b)-off < FrameHdrSize {
			return h, out, off, ErrTorn
		}
		n := int(binary.LittleEndian.Uint32(b[off:]))
		c := binary.LittleEndian.Uint32(b[off+4:])
		if n > MaxFrameSize || n > len(b)-off-FrameHdrSize {
			return h, out, off, ErrTorn
		}
		p := b[off+FrameHdrSize : off+FrameHdrSize+n]
		if CRC(p) != c {
			return h, out, off, ErrTorn
		}
		f, err := DecodePayload(p)
		if err != nil {
			return h, out, off, ErrTorn
		}
		rebase(f.Ops, off+FrameHdrSize)
		out = append(out, Decoded{Frame: f, Payload: p})
		off += FrameHdrSize + n
	}
	return h, out, off, nil
}

// ChainNext computes the next hash-chain head: H(prev || H(payload)).
// The payload contains the stored (possibly encrypted) bytes, so the chain
// is over ciphertext digests and survives crypto-shredding.
func ChainNext(prev [32]byte, payload []byte) [32]byte {
	d := sha256.Sum256(payload)
	h := sha256.New()
	h.Write(prev[:])
	h.Write(d[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
