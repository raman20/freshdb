package lens

import (
	"bytes"
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"github.com/raman20/freshdb/format"
)

// topicModel: every message ever appended per space, with a trimmed flag
// per space prefix; reads are linear scans.
type topicModel struct {
	ms    map[string][]Msg
	start map[string]uint64
	shred map[string]bool
}

func (m *topicModel) read(space string, from uint64, max int, at uint64) []Msg {
	var out []Msg
	for _, x := range m.ms[space] {
		if x.Offset >= from && x.Offset >= m.start[space] && x.LSN <= at && (max <= 0 || len(out) < max) {
			out = append(out, x)
		}
	}
	return out
}

func (m *topicModel) len(space string, at uint64) uint64 {
	n := uint64(0)
	for _, x := range m.ms[space] {
		if x.LSN <= at {
			n = x.Offset + 1
		}
	}
	return max(n, m.start[space])
}

func sameMsgs(a, b []Msg) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Offset != y.Offset || x.LSN != y.LSN || x.TS != y.TS || x.Subject != y.Subject || x.Redacted != y.Redacted || !bytes.Equal(x.Val, y.Val) || (x.Val == nil) != (y.Val == nil) {
			return false
		}
	}
	return true
}

type shredEnv map[string]bool

func (s shredEnv) Shredded(x string) bool            { return s[x] }
func (shredEnv) Open(e *Entry, i int) ([]byte, bool) { return e.Ops[i].Value, true }

// TestTopicRefinesModel: appends, shreds, retention trims (as a base
// does: SetAll with a start), snapshot reloads; every Read and Len
// matches the model at every LSN.
func TestTopicRefinesModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		l := NewTopic()
		m := &topicModel{ms: map[string][]Msg{}, start: map[string]uint64{}, shred: map[string]bool{}}
		spaces := []string{"topic:a", "topic:b", "topic:c"}
		subjects := []string{"", "", "s1", "s2"}
		lsn := uint64(0)
		steps := rapid.IntRange(1, 60).Draw(t, "steps")
		for i := 0; i < steps; i++ {
			switch rapid.IntRange(0, 9).Draw(t, "op") {
			default: // an entry of 1-3 appends
				lsn++
				e := &Entry{Log: "L", LSN: lsn, TS: int64(lsn * 10)}
				for j := rapid.IntRange(1, 3).Draw(t, "n"); j > 0; j-- {
					sp := rapid.SampledFrom(spaces).Draw(t, "space")
					sub := rapid.SampledFrom(subjects).Draw(t, "subject")
					var v []byte
					if k := rapid.IntRange(0, 2).Draw(t, "val"); k > 0 {
						v = []byte(fmt.Sprint(lsn, j))[:k-1]
					}
					e.Ops = append(e.Ops, format.Op{Kind: format.OpAppend, Space: sp, Value: v, Subject: sub})
					x := Msg{Offset: uint64(len(m.ms[sp])), LSN: lsn, TS: e.TS, Val: v, Subject: sub}
					if sub != "" && m.shred[sub] {
						x.Val, x.Redacted = nil, true
					}
					m.ms[sp] = append(m.ms[sp], x)
				}
				l.Apply(e, shredEnv(m.shred))
			case 7: // shred
				s := rapid.SampledFrom(subjects[2:]).Draw(t, "shred")
				m.shred[s] = true
				for _, ms := range m.ms {
					for k := range ms {
						if ms[k].Subject == s {
							ms[k].Val, ms[k].Redacted = nil, true
						}
					}
				}
				lsn++
				l.Apply(&Entry{Log: "L", LSN: lsn, Ops: []format.Op{{Kind: format.OpShred, Key: s}}}, shredEnv(m.shred))
			case 8: // retention trim of one space
				sp := rapid.SampledFrom(spaces).Draw(t, "trim")
				end := uint64(len(m.ms[sp]))
				cut := rapid.Uint64Range(l.Start(sp), max(end, l.Start(sp))).Draw(t, "cut")
				if cut > m.start[sp] {
					m.start[sp] = cut
				}
				var keep []Msg
				for _, x := range l.All(sp) {
					if x.Offset >= cut {
						keep = append(keep, x)
					}
				}
				if _, ok := l.m[sp]; ok || cut > 0 {
					l.SetAll(sp, cut, keep)
				}
			case 9: // snapshot reload
				l2, err := LoadTopic(l.Snapshot())
				if err != nil {
					t.Fatalf("reload: %v", err)
				}
				if l2.Digest() != l.Digest() {
					t.Fatal("reload changed the digest")
				}
				l = l2
			}
			for _, sp := range spaces {
				for at := uint64(0); at <= lsn; at++ {
					if got, want := l.Len(sp, at), m.len(sp, at); got != want {
						t.Fatalf("Len(%s, %d) = %d, want %d", sp, at, got, want)
					}
					for from := uint64(0); from <= uint64(len(m.ms[sp]))+1; from++ {
						for _, mx := range []int{0, 1, 2} {
							if got, want := l.Read(sp, from, mx, at), m.read(sp, from, mx, at); !sameMsgs(got, want) {
								t.Fatalf("Read(%s, %d, %d, %d) = %v, want %v", sp, from, mx, at, got, want)
							}
						}
					}
				}
			}
		}
	})
}
