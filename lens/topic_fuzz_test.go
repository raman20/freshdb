package lens

import (
	"bytes"
	"testing"

	"github.com/raman20/freshdb/format"
)

// topicSeed is a small Topic with every kind of message: plain, empty and
// nil values, subjects, a redacted message, two topics.
func topicSeed() *Topic {
	l := NewTopic()
	e := &Entry{Log: "L1", LSN: 1, TS: 10, Ops: []format.Op{
		{Kind: format.OpAppend, Space: "topic:a", Value: []byte("x")},
		{Kind: format.OpAppend, Space: "topic:a", Value: []byte{}, Subject: "s"},
		{Kind: format.OpAppend, Space: "topic:b"},
	}}
	l.Apply(e, noEnv{})
	e = &Entry{Log: "L1", LSN: 3, TS: 9, Ops: []format.Op{
		{Kind: format.OpAppend, Space: "topic:a", Value: []byte("y"), Subject: "r"},
	}}
	l.Apply(e, noEnv{})
	l.DropSubject("r")
	return l
}

// FuzzLoadTopic: a topic snapshot or base either fails to load or loads
// into a lens that re-encodes to the same bytes and serves every read.
func FuzzLoadTopic(f *testing.F) {
	f.Add(topicSeed().Snapshot())
	f.Add(NewTopic().Snapshot())
	trimmed := topicSeed()
	trimmed.SetAll("topic:a", 1, trimmed.All("topic:a")[1:])
	f.Add(trimmed.Snapshot())
	f.Fuzz(func(t *testing.T, b []byte) {
		l, err := LoadTopic(b)
		if err != nil {
			return
		}
		if got := l.Snapshot(); !bytes.Equal(got, b) {
			t.Fatalf("loaded bytes do not re-encode identically")
		}
		for _, s := range l.Spaces() {
			n := l.Len(s, ^uint64(0))
			all := l.Read(s, 0, 0, ^uint64(0))
			if st := l.Start(s); uint64(len(all)) != n-st {
				t.Fatalf("%s: Read all = %d messages, Len = %d, Start = %d", s, len(all), n, st)
			}
			for i, m := range all {
				if m.Offset != l.Start(s)+uint64(i) {
					t.Fatalf("%s: message %d has offset %d", s, i, m.Offset)
				}
				if i > 0 && m.LSN < all[i-1].LSN {
					t.Fatalf("%s: LSNs go back at offset %d", s, m.Offset)
				}
				if m.Redacted && m.Val != nil {
					t.Fatalf("%s: redacted message %d keeps its value", s, m.Offset)
				}
				if got := l.Len(s, m.LSN); got <= m.Offset {
					t.Fatalf("%s: Len at the LSN of offset %d = %d", s, m.Offset, got)
				}
			}
		}
		l.DropSubject("s")
		l.Digest()
	})
}

// TestLoadTopicRejectsMalformed: C-D1. Each case loaded before the fix.
func TestLoadTopicRejectsMalformed(t *testing.T) {
	type sp struct {
		k     string
		ms    []Msg
		start uint64
	}
	v2 := false
	build := func(spaces ...sp) []byte {
		e := &enc{}
		if v2 {
			e.str("TOPIC2")
		} else {
			e.str("TOPIC1")
		}
		e.u64(uint64(len(spaces)))
		for _, s := range spaces {
			e.str(s.k)
			if v2 {
				e.u64(s.start)
			}
			e.u64(uint64(len(s.ms)))
			for _, m := range s.ms {
				e.u64(m.LSN)
				e.u64(uint64(m.TS))
				e.bytes(m.Val)
				e.str(m.Subject)
				e.bool(m.Redacted)
			}
		}
		return e.b
	}
	m1 := []Msg{{LSN: 1, Val: []byte("x")}}
	if _, err := LoadTopic(build(sp{k: "topic:a", ms: m1}, sp{k: "topic:b"}, sp{k: "topic:c", ms: []Msg{{LSN: 2}, {LSN: 2, Subject: "s", Redacted: true}}})); err != nil {
		t.Fatalf("well-formed snapshot rejected: %v", err)
	}
	for name, b := range map[string][]byte{
		"duplicate topic":          build(sp{k: "topic:a", ms: m1}, sp{k: "topic:a", ms: m1}),
		"topics out of order":      build(sp{k: "topic:b", ms: m1}, sp{k: "topic:a", ms: m1}),
		"LSNs go back":             build(sp{k: "topic:a", ms: []Msg{{LSN: 2}, {LSN: 1}}}),
		"redacted with a value":    build(sp{k: "topic:a", ms: []Msg{{LSN: 1, Subject: "s", Val: []byte("x"), Redacted: true}}}),
		"redacted with no subject": build(sp{k: "topic:a", ms: []Msg{{LSN: 1, Redacted: true}}}),
		"count past the end":       append(build(sp{k: "topic:a"})[:len(build(sp{k: "topic:a"}))-8], 0xff, 0xff, 0, 0, 0, 0, 0, 0),
	} {
		if _, err := LoadTopic(b); err != ErrBadSnapshot {
			t.Errorf("%s: err = %v, want ErrBadSnapshot", name, err)
		}
	}
	v2 = true
	if _, err := LoadTopic(build(sp{k: "topic:a", ms: m1}, sp{k: "topic:b", start: 7})); err != nil {
		t.Fatalf("well-formed TOPIC2 rejected: %v", err)
	}
	for name, b := range map[string][]byte{
		"TOPIC2 with every start 0": build(sp{k: "topic:a", ms: m1}),
		"start + count overflows":   build(sp{k: "topic:a", ms: m1, start: ^uint64(0)}),
		"unknown format":            append([]byte{6, 0, 0, 0, 0, 0, 0, 0}, "TOPIC3"...),
	} {
		if _, err := LoadTopic(b); err != ErrBadSnapshot {
			t.Errorf("%s: err = %v, want ErrBadSnapshot", name, err)
		}
	}
}
