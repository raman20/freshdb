package lens

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/raman20/freshdb/format"
	"pgregory.net/rapid"
)

// testEnv: identity "encryption" + a shredded set.
type testEnv map[string]bool

func (t testEnv) Shredded(s string) bool { return t[s] }
func (t testEnv) Open(e *Entry, i int) ([]byte, bool) {
	op := e.Ops[i]
	if op.Subject != "" && t[op.Subject] {
		return nil, false
	}
	return op.Value, true
}

var spaces = []string{"kv", "doc:c", "vec:c", "topic:t"}

func genLog(t *rapid.T) []*Entry {
	n := rapid.IntRange(0, 40).Draw(t, "entries")
	var log []*Entry
	for lsn := uint64(1); lsn <= uint64(n); lsn++ {
		e := &Entry{Log: "L1", LSN: lsn, TS: int64(lsn)}
		nops := rapid.IntRange(1, 4).Draw(t, "nops")
		for j := 0; j < nops; j++ {
			sp := rapid.SampledFrom(spaces).Draw(t, "space")
			op := format.Op{Space: sp, Key: fmt.Sprint("k", rapid.IntRange(0, 4).Draw(t, "key")),
				Subject: rapid.SampledFrom([]string{"", "", "a", "b"}).Draw(t, "subj")}
			switch sp {
			case "topic:t":
				op.Kind = format.OpAppend
			default:
				op.Kind = rapid.SampledFrom([]format.Kind{format.OpPut, format.OpPut, format.OpDel}).Draw(t, "kind")
			}
			if op.Kind == format.OpDel {
				op.Subject = ""
			}
			switch sp {
			case "vec:c":
				op.Value = EncodeVec([]float32{float32(rapid.IntRange(-3, 3).Draw(t, "x")), float32(rapid.IntRange(-3, 3).Draw(t, "y"))})
			case "doc:c":
				op.Value, _ = json.Marshal(map[string]any{"f": rapid.IntRange(0, 3).Draw(t, "f")})
			default:
				op.Value = []byte(fmt.Sprint("v", lsn, ".", j))
			}
			if op.Kind == format.OpDel {
				op.Value = nil
			}
			e.Ops = append(e.Ops, op)
		}
		log = append(log, e)
	}
	return log
}

func lenses() []Lens { return []Lens{NewKV(), NewTopic(), NewVector(), NewDocIndex("c", "f")} }

func fold(log []*Entry, env Env) []Lens {
	ls := lenses()
	for _, e := range log {
		for _, l := range ls {
			l.Apply(e, env)
		}
	}
	return ls
}

func digests(ls []Lens) string {
	s := ""
	for _, l := range ls {
		d := l.Digest()
		s += fmt.Sprintf("%s=%x ", l.Name(), d[:6])
	}
	return s
}

// Replay determinism: the same log folds to bit-identical state.
func TestPropFoldDeterministic(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		log := genLog(t)
		env := testEnv{"b": rapid.Bool().Draw(t, "shredB")}
		if a, b := digests(fold(log, env)), digests(fold(log, env)); a != b {
			t.Fatalf("non-deterministic fold: %s vs %s", a, b)
		}
	})
}

// Checkpoint = fold prefix: Load(Snapshot(fold(log[:k]))) then fold(log[k:])
// is identical to fold(log).
func TestPropSnapshotPlusTailEqualsFold(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		log := genLog(t)
		k := rapid.IntRange(0, len(log)).Draw(t, "k")
		env := testEnv{}
		kv, tp := NewKV(), NewTopic()
		for _, e := range log[:k] {
			kv.Apply(e, env)
			tp.Apply(e, env)
		}
		kv2, err := LoadKV(kv.Snapshot())
		if err != nil {
			t.Fatal(err)
		}
		tp2, err := LoadTopic(tp.Snapshot())
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range log[k:] {
			kv2.Apply(e, env)
			tp2.Apply(e, env)
		}
		full := fold(log, env)
		if kv2.Digest() != full[0].Digest() || tp2.Digest() != full[1].Digest() {
			t.Fatal("snapshot+tail != fold")
		}
	})
}

// Crypto-shred: DropSubject(s) on a live lens == folding the log with s
// redacted from the very beginning, for every lens.
func TestPropDropSubjectEqualsRedactedFold(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		log := genLog(t)
		live := fold(log, testEnv{})
		for _, l := range live {
			l.DropSubject("a")
		}
		redacted := fold(log, testEnv{"a": true})
		if a, b := digests(live), digests(redacted); a != b {
			t.Fatalf("DropSubject != fold(redact):\n%s\n%s", a, b)
		}
	})
}

// Refinement of each lens against a naive reference model built from the
// log prefix ≤ at (MVCC reads / time travel).
func TestPropLensesRefineModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		log := genLog(t)
		env := testEnv{"b": rapid.Bool().Draw(t, "shredB")}
		ls := fold(log, env)
		kv, tp, vec, idx := ls[0].(*KV), ls[1].(*Topic), ls[2].(*Vector), ls[3].(*DocIndex)
		at := uint64(rapid.IntRange(0, len(log)).Draw(t, "at"))
		// reference model
		mkv := map[string]string{}
		var mtopic []string
		for _, e := range log {
			if e.LSN > at {
				break
			}
			for _, op := range e.Ops {
				red := op.Subject != "" && env[op.Subject]
				switch op.Kind {
				case format.OpPut:
					if red {
						delete(mkv, op.Space+"/"+op.Key)
					} else {
						mkv[op.Space+"/"+op.Key] = string(op.Value)
					}
				case format.OpDel:
					delete(mkv, op.Space+"/"+op.Key)
				case format.OpAppend:
					if red {
						mtopic = append(mtopic, "<r>")
					} else {
						mtopic = append(mtopic, string(op.Value))
					}
				}
			}
		}
		for _, sp := range spaces[:3] {
			for i := 0; i < 5; i++ {
				k := fmt.Sprint("k", i)
				v, ok := kv.Get(sp, k, at)
				want, wok := mkv[sp+"/"+k]
				if ok != wok || (ok && string(v.Val) != want) {
					t.Fatalf("KV %s/%s@%d: got %q,%v want %q,%v", sp, k, at, v.Val, ok, want, wok)
				}
			}
		}
		var gt []string
		for _, m := range tp.Read("topic:t", 0, 0, at) {
			if m.Redacted {
				gt = append(gt, "<r>")
			} else {
				gt = append(gt, string(m.Val))
			}
		}
		if fmt.Sprint(gt) != fmt.Sprint(mtopic) {
			t.Fatalf("topic: %v vs %v", gt, mtopic)
		}
		if at != uint64(len(log)) {
			return // vector/index lenses are head-only
		}
		// vector: brute force over the model
		q := []float32{float32(rapid.IntRange(-3, 3).Draw(t, "qx")), float32(rapid.IntRange(-3, 3).Draw(t, "qy"))}
		type h struct {
			id string
			s  float64
		}
		var want []h
		qn := math.Hypot(float64(q[0]), float64(q[1]))
		var wantIdx []string
		for i := 0; i < 5; i++ {
			k := fmt.Sprint("k", i)
			if raw, ok := mkv["vec:c/"+k]; ok && qn > 0 {
				v, _ := DecodeVec([]byte(raw))
				n := math.Hypot(float64(v[0]), float64(v[1]))
				if n > 0 {
					want = append(want, h{k, (float64(q[0])*float64(v[0]) + float64(q[1])*float64(v[1])) / (qn * n)})
				}
			}
			if raw, ok := mkv["doc:c/"+k]; ok {
				var d map[string]any
				json.Unmarshal([]byte(raw), &d)
				if fmt.Sprint(d["f"]) == "2" {
					wantIdx = append(wantIdx, k)
				}
			}
		}
		sort.Slice(want, func(i, j int) bool {
			if math.Abs(want[i].s-want[j].s) > 1e-6 {
				return want[i].s > want[j].s
			}
			return want[i].id < want[j].id
		})
		got := vec.Search("c", q, 10)
		if len(got) != len(want) {
			t.Fatalf("vector: %v vs %v", got, want)
		}
		for i := range got {
			if math.Abs(float64(got[i].Score)-want[i].s) > 1e-5 {
				t.Fatalf("vector scores: %v vs %v", got, want)
			}
		}
		if fmt.Sprint(idx.Lookup("2")) != fmt.Sprint(wantIdx) && !(len(wantIdx) == 0 && len(idx.Lookup("2")) == 0) {
			t.Fatalf("index: %v vs %v", idx.Lookup("2"), wantIdx)
		}
	})
}
