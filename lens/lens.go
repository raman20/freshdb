// Package lens contains freshdb's views over the log.
//
// Central invariant (enforced by tests, see purity_test.go and the property
// tests): every lens is a pure, deterministic left fold over the redacted log:
//
//	state(lens) == fold(Apply, empty, redact(log[0..w], S))
//
// where w is the lens watermark and S is the set of crypto-shredded subjects.
// redact() turns every op of a shredded subject into an unreadable tombstone.
// Lenses must not read clocks, randomness, files or the network: everything
// they may depend on is in the Entry or the Env. This package is
// import-restricted by purity_test.go to keep it that way.
package lens

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sort"

	"github.com/raman20/freshdb/format"
)

// Entry is one committed transaction as seen by lenses.
type Entry struct {
	Log string // id of the log (branch lineage) the frame was written to
	LSN uint64
	TS  int64
	Ops []format.Op
}

// Env is the deterministic environment a lens may consult.
type Env interface {
	// Shredded reports whether a subject has been crypto-shredded.
	Shredded(subject string) bool
	// Open returns op i's plaintext value; ok=false if the subject is shredded.
	Open(e *Entry, i int) (plain []byte, ok bool)
}

// Lens is a view over the log.
type Lens interface {
	Name() string
	Apply(e *Entry, env Env)
	// DropSubject must leave the lens exactly as if every op of subject had
	// been redacted from the start (fold(redact(log, S ∪ {s}))).
	DropSubject(subject string)
	// Digest is a hash of the full lens state, for fold-equivalence checks.
	Digest() [32]byte
}

// enc is a tiny canonical encoder used for digests and snapshots.
type enc struct{ b []byte }

func (e *enc) u64(v uint64) { e.b = binary.LittleEndian.AppendUint64(e.b, v) }
func (e *enc) str(s string) { e.u64(uint64(len(s))); e.b = append(e.b, s...) }
func (e *enc) bytes(s []byte) {
	if s == nil {
		e.u64(math.MaxUint64)
		return
	}
	e.u64(uint64(len(s)))
	e.b = append(e.b, s...)
}
func (e *enc) bool(v bool) {
	if v {
		e.b = append(e.b, 1)
	} else {
		e.b = append(e.b, 0)
	}
}

type dec struct {
	b   []byte
	bad bool
}

func (d *dec) u64() uint64 {
	if len(d.b) < 8 {
		d.bad = true
		return 0
	}
	v := binary.LittleEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}
func (d *dec) bytes() []byte {
	n := d.u64()
	if n == math.MaxUint64 {
		return nil
	}
	if d.bad || n > uint64(len(d.b)) {
		d.bad = true
		return nil
	}
	v := append([]byte{}, d.b[:n]...)
	d.b = d.b[n:]
	return v
}

// str decodes a string. A string is never written as nil, so the nil
// marker is malformed input, not "".
func (d *dec) str() string {
	if len(d.b) >= 8 && binary.LittleEndian.Uint64(d.b) == math.MaxUint64 {
		d.bad = true
		return ""
	}
	return string(d.bytes())
}
func (d *dec) bool() bool {
	if len(d.b) < 1 {
		d.bad = true
		return false
	}
	if d.b[0] > 1 {
		d.bad = true // a bool is written as 0 or 1 only
	}
	v := d.b[0] == 1
	d.b = d.b[1:]
	return v
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func digest(b []byte) [32]byte { return sha256.Sum256(b) }
