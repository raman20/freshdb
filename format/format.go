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
	"slices"
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

// Segment header layout (little-endian, SegHeaderSize bytes):
//
//	0..8   magic "FRDBSEG1"
//	8..10  version (SegVersion)
//	10..12 reserved, zero (rejected otherwise, so a future use is never misread)
//	12..20 epoch
//	20..28 seq
//	28..32 CRC32C of bytes 0..28
//
// Frames follow: len uint32 | CRC32C(payload) uint32 | payload.
const (
	SegMagic      = "FRDBSEG1"
	SegVersion    = 1
	SegHeaderSize = 8 + 2 + 2 + 8 + 8 + 4
	FrameHdrSize  = 8
	MaxFrameSize  = 64 << 20
	MaxOps        = 1 << 20
)

var (
	ErrTorn    = errors.New("format: torn or corrupt tail")
	ErrHeader  = errors.New("format: bad segment header")
	ErrCorrupt = errors.New("format: corrupt frame")
	// ErrVersion: an intact header (CRC ok) with a version this build does
	// not read, e.g. written by a newer freshdb. Never repaired.
	ErrVersion = errors.New("format: unsupported segment version")
)

// SegHeader is the fixed header of a segment object.
type SegHeader struct {
	Epoch uint64 // writer epoch that created the segment (fencing)
	Seq   uint64 // dense sequence number within the log, starting at 1
}

// EncodeHeader encodes a segment header (layout above).
func EncodeHeader(h SegHeader) []byte {
	b := make([]byte, SegHeaderSize)
	copy(b, SegMagic)
	binary.LittleEndian.PutUint16(b[8:], SegVersion)
	binary.LittleEndian.PutUint64(b[12:], h.Epoch)
	binary.LittleEndian.PutUint64(b[20:], h.Seq)
	binary.LittleEndian.PutUint32(b[28:], CRC(b[:28]))
	return b
}

// DecodeHeader decodes and checks a segment header: ErrHeader if it is
// damaged or not canonical, ErrVersion if intact but of another version.
func DecodeHeader(b []byte) (SegHeader, error) {
	if len(b) < SegHeaderSize || string(b[:8]) != SegMagic {
		return SegHeader{}, ErrHeader
	}
	if binary.LittleEndian.Uint32(b[28:]) != CRC(b[:28]) {
		return SegHeader{}, ErrHeader
	}
	if v := binary.LittleEndian.Uint16(b[8:]); v != SegVersion {
		return SegHeader{}, fmt.Errorf("%w %d (this build reads %d; written by a newer freshdb? upgrade the binary)", ErrVersion, v, SegVersion)
	}
	if b[10]|b[11] != 0 {
		return SegHeader{}, ErrHeader
	}
	return SegHeader{Epoch: binary.LittleEndian.Uint64(b[12:]), Seq: binary.LittleEndian.Uint64(b[20:])}, nil
}

// EncodePayload encodes a frame body (without the len/crc frame header).
// It records each op's value offset (relative to the payload) in f.Ops.
func EncodePayload(f *Frame) []byte {
	return appendPayload(make([]byte, 0, payloadSize(f)), f)
}

// payloadSize is the exact encoded size of f's payload.
func payloadSize(f *Frame) int {
	n := 8 + 8 + 32 + uvarintLen(uint64(len(f.Ops)))
	for _, op := range f.Ops {
		n += 1 + strLen(len(op.Subject)) + strLen(len(op.Space)) + strLen(len(op.Key)) + strLen(len(op.Value))
	}
	return n
}

func strLen(n int) int { return uvarintLen(uint64(n)) + n }

// appendPayload appends f's payload to b. ValOff is relative to the start
// of the payload, which begins at len(b) on entry.
func appendPayload(b []byte, f *Frame) []byte {
	start := len(b)
	b = binary.LittleEndian.AppendUint64(b, f.LSN)
	b = binary.LittleEndian.AppendUint64(b, uint64(f.TS))
	b = append(b, f.Prev[:]...)
	b = binary.AppendUvarint(b, uint64(len(f.Ops)))
	for i, op := range f.Ops {
		b = append(b, byte(op.Kind))
		b = appendStr(b, op.Subject)
		b = appendStr(b, op.Space)
		b = appendStr(b, op.Key)
		b = binary.AppendUvarint(b, uint64(len(op.Value)))
		f.Ops[i].ValOff = uint32(len(b) - start)
		b = append(b, op.Value...)
	}
	return b
}

func appendStr[T string | []byte](b []byte, s T) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func rebase(ops []Op, base int) {
	for i := range ops {
		ops[i].ValOff += uint32(base)
	}
}

// AppendFrame appends a framed (len|crc|payload) encoding of f to dst and
// returns the new slice plus the payload (for hash chaining). The payload
// aliases the returned slice; ValOff in f.Ops becomes relative to dst[0].
func AppendFrame(dst []byte, f *Frame) ([]byte, []byte) {
	hdr := len(dst)
	dst = slices.Grow(dst, FrameHdrSize+payloadSize(f))
	dst = append(dst, make([]byte, FrameHdrSize)...)
	dst = appendPayload(dst, f)
	p := dst[hdr+FrameHdrSize:]
	binary.LittleEndian.PutUint32(dst[hdr:], uint32(len(p)))
	binary.LittleEndian.PutUint32(dst[hdr+4:], CRC(p))
	rebase(f.Ops, hdr+FrameHdrSize)
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
func DecodePayload(p []byte) (*Frame, error) { return decodePayload(p, nil) }

// names interns Subject and Space strings across the frames of one segment:
// a batch has few distinct spaces, and each conversion would allocate.
type names map[string]string

const maxNames = 256 // bound the table for segments with many subjects

func (m names) str(b []byte) string {
	if m == nil {
		return string(b)
	}
	if s, ok := m[string(b)]; ok { // no allocation for the lookup
		return s
	}
	s := string(b)
	if len(m) < maxNames {
		m[s] = s
	}
	return s
}

func decodePayload(p []byte, in names) (*Frame, error) {
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
	if r.err || n > MaxOps || n > uint64(len(r.b)) {
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
		f.Ops = append(f.Ops, Op{Kind: k, Subject: in.str(subj), Space: in.str(space), Key: string(key), Value: v, ValOff: uint32(valOff + uvarintLen(uint64(len(val))))})
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
	in := names{}
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
		f, err := decodePayload(p, in)
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
	var buf [64]byte
	copy(buf[:32], prev[:])
	d := sha256.Sum256(payload)
	copy(buf[32:], d[:])
	return sha256.Sum256(buf[:])
}
