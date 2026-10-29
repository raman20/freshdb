package lens

import (
	"sort"

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
// Retention (a compaction base that starts a topic at offset s > 0) drops
// the messages below s; offsets never change.
type Topic struct {
	m     map[string][]Msg
	start map[string]uint64 // first kept offset; absent = 0
	// subj indexes the unredacted messages that have a subject, so a shred
	// touches only them: subject -> (space, offset).
	subj map[string][]msgRef
}

type msgRef struct {
	space string
	off   uint64
}

func NewTopic() *Topic {
	return &Topic{m: map[string][]Msg{}, start: map[string]uint64{}, subj: map[string][]msgRef{}}
}

func (*Topic) Name() string { return "topic" }

func (l *Topic) Apply(e *Entry, env Env) {
	for _, op := range e.Ops {
		switch op.Kind {
		case format.OpAppend:
			ms := l.m[op.Space]
			m := Msg{Offset: l.start[op.Space] + uint64(len(ms)), LSN: e.LSN, TS: e.TS, Val: op.Value, Subject: op.Subject}
			if op.Subject != "" && env.Shredded(op.Subject) {
				m.Val, m.Redacted = nil, true
			}
			l.add(op.Space, m)
		case format.OpShred:
			l.DropSubject(op.Key)
		}
	}
}

func (l *Topic) add(space string, m Msg) {
	l.m[space] = append(l.m[space], m)
	if m.Subject != "" && !m.Redacted {
		l.subj[m.Subject] = append(l.subj[m.Subject], msgRef{space, m.Offset})
	}
}

// DropSubject redacts every message of subject s. It costs the number of
// such messages, not the size of the topics.
func (l *Topic) DropSubject(s string) {
	for _, r := range l.subj[s] {
		st := l.start[r.space]
		if ms := l.m[r.space]; r.off >= st && r.off-st < uint64(len(ms)) {
			ms[r.off-st].Val, ms[r.off-st].Redacted = nil, true
		}
	}
	delete(l.subj, s)
}

// visible returns the messages of space with LSN <= at.
func (l *Topic) visible(space string, at uint64) []Msg {
	ms := l.m[space]
	return ms[:sort.Search(len(ms), func(i int) bool { return ms[i].LSN > at })]
}

// Read returns up to max messages of space starting at offset from,
// visible at LSN at (max <= 0: all of them). A from below the topic's
// start reads from the start: those messages were removed by retention.
func (l *Topic) Read(space string, from uint64, max int, at uint64) []Msg {
	ms, st := l.visible(space, at), l.start[space]
	if from < st {
		from = st
	}
	if from-st >= uint64(len(ms)) {
		return nil
	}
	out := ms[from-st:]
	if max > 0 && len(out) > max {
		out = out[:max]
	}
	return append([]Msg{}, out...)
}

// Len returns the offset after the last message visible at LSN at, i.e.
// the number of messages ever appended up to at (retained or not).
func (l *Topic) Len(space string, at uint64) uint64 {
	return l.start[space] + uint64(len(l.visible(space, at)))
}

// Start returns the first offset still kept: 0 unless retention removed
// older messages.
func (l *Topic) Start(space string) uint64 { return l.start[space] }

// encode writes "TOPIC1" when every topic starts at 0 (the format before
// retention, so digests do not change) and "TOPIC2", which records each
// topic's start, otherwise.
func (l *Topic) encode() []byte {
	e := &enc{}
	v2 := false
	for _, s := range l.start {
		v2 = v2 || s != 0
	}
	if v2 {
		e.str("TOPIC2")
	} else {
		e.str("TOPIC1")
	}
	e.u64(uint64(len(l.m)))
	for _, k := range sortedKeys(l.m) {
		e.str(k)
		if v2 {
			e.u64(l.start[k])
		}
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

// LoadTopic decodes a Snapshot. It accepts only bytes Snapshot could have
// written: spaces in strictly ascending order, LSNs that never go back
// within a topic, a redacted message only with a subject and no value,
// and TOPIC2 only when some topic starts above 0. Anything else is
// ErrBadSnapshot.
func LoadTopic(b []byte) (*Topic, error) {
	d := &dec{b: b}
	f := d.str()
	if f != "TOPIC1" && f != "TOPIC2" {
		return nil, ErrBadSnapshot
	}
	n := d.u64()
	l := NewTopic()
	prev, started := "", false
	for i := uint64(0); i < n && !d.bad; i++ {
		k := d.str()
		if i > 0 && k <= prev {
			return nil, ErrBadSnapshot
		}
		prev = k
		var st uint64
		if f == "TOPIC2" {
			if st = d.u64(); st != 0 {
				l.start[k], started = st, true
			}
		}
		c := d.u64()
		if c > uint64(len(d.b))/33 || st+c < st { // a message takes at least 33 bytes
			return nil, ErrBadSnapshot
		}
		ms := make([]Msg, 0, c)
		l.m[k] = ms
		for j := uint64(0); j < c && !d.bad; j++ {
			m := Msg{Offset: st + j, LSN: d.u64(), TS: int64(d.u64()), Val: d.bytes(), Subject: d.str(), Redacted: d.bool()}
			if (j > 0 && m.LSN < l.m[k][j-1].LSN) || (m.Redacted && (m.Val != nil || m.Subject == "")) {
				return nil, ErrBadSnapshot
			}
			l.add(k, m)
		}
	}
	if d.bad || len(d.b) != 0 || (f == "TOPIC2") != started {
		return nil, ErrBadSnapshot
	}
	return l, nil
}

// Spaces returns the topic spaces in order.
func (l *Topic) Spaces() []string { return sortedKeys(l.m) }

// All returns every kept message of space (shared; do not modify).
func (l *Topic) All(space string) []Msg { return l.m[space] }

// SetAll replaces the messages of space with ms, which start at offset
// start (their Offset fields must be start, start+1, ...). For building
// compaction bases.
func (l *Topic) SetAll(space string, start uint64, ms []Msg) {
	old := l.m[space]
	if start == 0 {
		delete(l.start, space)
	} else {
		l.start[space] = start
	}
	for s, rs := range l.subj { // drop the space's old refs, if it had any
		if len(old) == 0 {
			break
		}
		k := rs[:0]
		for _, r := range rs {
			if r.space != space {
				k = append(k, r)
			}
		}
		if len(k) == 0 {
			delete(l.subj, s)
		} else {
			l.subj[s] = k
		}
	}
	l.m[space] = make([]Msg, 0, len(ms))
	for _, m := range ms {
		l.add(space, m)
	}
}
