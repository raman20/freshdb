package lens

import (
	"sort"
	"strings"

	"github.com/raman20/freshdb/format"
)

// Msg is one message in a topic. Offset = index in the topic (dense, 0-based).
type Msg struct {
	Offset   uint64
	LSN      uint64
	TS       int64
	Val      []byte // stored bytes (ciphertext if Subject != "")
	Subject  string
	Redacted bool
}

// Topic is the stream lens: every APPEND op, per space ("topic:<name>").
// Offsets are a pure function of the log: the n-th append is offset n.
type Topic struct {
	m map[string][]Msg
}

func NewTopic() *Topic { return &Topic{m: map[string][]Msg{}} }

func (*Topic) Name() string { return "topic" }

func (l *Topic) Apply(e *Entry, env Env) {
	for _, op := range e.Ops {
		switch op.Kind {
		case format.OpAppend:
			ms := l.m[op.Space]
			m := Msg{Offset: uint64(len(ms)), LSN: e.LSN, TS: e.TS, Val: op.Value, Subject: op.Subject}
			if op.Subject != "" && env.Shredded(op.Subject) {
				m.Val, m.Redacted = nil, true
			}
			l.m[op.Space] = append(ms, m)
		case format.OpShred:
			l.DropSubject(op.Key)
		}
	}
}

func (l *Topic) DropSubject(s string) {
	for _, ms := range l.m {
		for i := range ms {
			if ms[i].Subject == s {
				ms[i].Val, ms[i].Redacted = nil, true
			}
		}
	}
}

// Read returns up to max messages of space starting at offset from,
// visible at LSN at.
func (l *Topic) Read(space string, from uint64, max int, at uint64) []Msg {
	ms := l.m[space]
	end := sort.Search(len(ms), func(i int) bool { return ms[i].LSN > at })
	if from >= uint64(end) {
		return nil
	}
	out := ms[from:end]
	if max > 0 && len(out) > max {
		out = out[:max]
	}
	return append([]Msg{}, out...)
}

// Len returns the number of messages visible at LSN at.
func (l *Topic) Len(space string, at uint64) uint64 {
	ms := l.m[space]
	return uint64(sort.Search(len(ms), func(i int) bool { return ms[i].LSN > at }))
}

// Topics lists topic spaces with the given prefix.
func (l *Topic) Topics(prefix string) []string {
	var out []string
	for _, k := range sortedKeys(l.m) {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

func (l *Topic) encode() []byte {
	e := &enc{}
	e.str("TOPIC1")
	e.u64(uint64(len(l.m)))
	for _, k := range sortedKeys(l.m) {
		e.str(k)
		ms := l.m[k]
		e.u64(uint64(len(ms)))
		for _, m := range ms {
			e.u64(m.LSN)
			e.u64(uint64(m.TS))
			e.bytes(m.Val)
			e.str(m.Subject)
			e.bool(m.Redacted)
		}
	}
	return e.b
}

func (l *Topic) Digest() [32]byte { return digest(l.encode()) }
func (l *Topic) Snapshot() []byte { return l.encode() }

func LoadTopic(b []byte) (*Topic, error) {
	d := &dec{b: b}
	if d.str() != "TOPIC1" {
		return nil, ErrBadSnapshot
	}
	n := d.u64()
	l := NewTopic()
	for i := uint64(0); i < n && !d.bad; i++ {
		k := d.str()
		c := d.u64()
		if c > uint64(len(d.b)) {
			return nil, ErrBadSnapshot
		}
		ms := make([]Msg, 0, c)
		for j := uint64(0); j < c && !d.bad; j++ {
			ms = append(ms, Msg{Offset: j, LSN: d.u64(), TS: int64(d.u64()), Val: d.bytes(), Subject: d.str(), Redacted: d.bool()})
		}
		l.m[k] = ms
	}
	if d.bad || len(d.b) != 0 {
		return nil, ErrBadSnapshot
	}
	return l, nil
}
