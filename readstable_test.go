package freshdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/store"
)

// scripted answers Get from a per-key script (then the real object).
type scripted struct {
	store.Store
	gets   map[string][][]byte
	nGet   map[string]int
	listFn func(prefix string, keys []string) []string
	onList func()
}

func newScripted(inner store.Store) *scripted {
	return &scripted{Store: inner, gets: map[string][][]byte{}, nGet: map[string]int{}}
}

func (s *scripted) Get(ctx context.Context, k string) ([]byte, error) {
	s.nGet[k]++
	if q := s.gets[k]; len(q) > 0 {
		s.gets[k] = q[1:]
		return q[0], nil
	}
	return s.Store.Get(ctx, k)
}

func (s *scripted) List(ctx context.Context, p string) ([]string, error) {
	if s.onList != nil {
		s.onList()
	}
	keys, err := s.Store.List(ctx, p)
	if s.listFn != nil && err == nil {
		keys = s.listFn(p, keys)
	}
	return keys, err
}

func TestGetStable(t *testing.T) {
	good, bad, other := []byte("good"), []byte("bad!"), []byte("othr")
	judge := func(b []byte) readVerdict {
		switch string(b) {
		case "good":
			return readOK
		case "othr":
			return readMisread
		}
		return readDamaged
	}
	cases := []struct {
		name   string
		script [][]byte
		stored []byte
		want   []byte
		reads  int
		err    error
	}{
		{"clean", nil, good, good, 1, nil},
		{"one flip then good", [][]byte{bad}, good, good, 2, nil},
		{"stable damage", nil, bad, bad, stableAgree, nil},
		{"damage must repeat consecutively", [][]byte{bad, bad, []byte("bad?"), bad, bad}, bad, bad, 6, nil},
		{"misreads then good", [][]byte{other, other, other}, good, good, 4, nil},
		{"a stored wrong object is believed only after every read", nil, other, other, stableReads, nil},
		{"never agreeing", [][]byte{[]byte("b1"), []byte("b2"), []byte("b3"), []byte("b4"), []byte("b5"), []byte("b6")}, bad, nil, stableReads, errUnstable},
	}
	for _, c := range cases {
		st := newScripted(store.NewMem())
		st.Store.PutIfAbsent(ctx, "k", c.stored)
		st.gets["k"] = c.script
		got, err := getStable(ctx, st, "k", judge)
		if !errors.Is(err, c.err) || string(got) != string(c.want) || st.nGet["k"] != c.reads {
			t.Errorf("%s: got %q %v after %d reads; want %q %v after %d", c.name, got, err, st.nGet["k"], c.want, c.err, c.reads)
		}
	}
	if _, err := getStable(ctx, store.NewMem(), "missing", judge); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

// A stale LIST that omits the newest segments must not shorten the log.
func TestRecoverIgnoresStaleList(t *testing.T) {
	mem := store.NewMem()
	db := mustOpen(t, mem)
	for i := 0; i < 5; i++ {
		db.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"))
	}
	db.Close()
	st := newScripted(mem)
	st.listFn = func(p string, keys []string) []string {
		if strings.HasPrefix(p, "log/") && len(keys) > 2 {
			return keys[:len(keys)-2]
		}
		return keys
	}
	db = mustOpen(t, st)
	defer db.Close()
	if v, err := db.Get(ctx, "k4"); err != nil || string(v) != "v" {
		t.Fatalf("newest write after a stale LIST: %q %v", v, err)
	}
}

// A segment missing in the middle is a hole: ErrCorrupt, never a shorter log.
func TestRecoverDetectsHole(t *testing.T) {
	mem := store.NewMem()
	db := mustOpen(t, mem)
	for i := 0; i < 5; i++ {
		db.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"))
	}
	db.Close()
	mem.Raw(func(m map[string][]byte) { delete(m, segKey("L1", 3)) })
	if _, err := OpenStore(ctx, mem, quiet()); !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "missing segment 3") {
		t.Fatalf("hole: %v", err)
	}
}

// A segment that appears between the scan and the LIST (another writer)
// is read on, not reported as a hole.
func TestRecoverReadsOnWhenLogGrows(t *testing.T) {
	mem := store.NewMem()
	db := mustOpen(t, mem)
	db.Put(ctx, "k", []byte("v"))
	db.Close()
	keys, _ := mem.List(ctx, "log/L1/")
	last, _ := parseNum(keys[len(keys)-1], "log/L1/", ".seg")
	st := newScripted(mem)
	once := false
	st.onList = func() {
		if !once {
			once = true
			mem.PutIfAbsent(ctx, segKey("L1", last+1), format.EncodeHeader(format.SegHeader{Epoch: 1, Seq: last + 1}))
		}
	}
	l := &logState{meta: LogMeta{ID: "L1"}, st: st}
	d := &DB{st: st, epoch: 99, opts: Options{Logf: func(string, ...any) {}}}
	if err := d.recoverLog(ctx, l, false); err != nil {
		t.Fatal(err)
	}
	if l.nextSeq != last+2 {
		t.Fatalf("nextSeq %d, want %d (the segment that appeared is read)", l.nextSeq, last+2)
	}
}

// A misdirected read (another segment's bytes) is retried, not taken for
// damage; one that is really stored under the key is ErrCorrupt.
func TestRecoverMisreadSegment(t *testing.T) {
	mem := store.NewMem()
	db := mustOpen(t, mem)
	for i := 0; i < 4; i++ {
		db.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"))
	}
	db.Close()
	seg2, _ := mem.Get(ctx, segKey("L1", 2))
	st := newScripted(mem)
	st.gets[segKey("L1", 3)] = [][]byte{seg2, seg2, seg2} // three identical misreads
	db = mustOpen(t, st)
	db.Close()
	mem.Raw(func(m map[string][]byte) { m[segKey("L1", 3)] = seg2 })
	if _, err := OpenStore(ctx, mem, quiet()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("stored wrong segment: %v", err)
	}
}

// One flipped read must not make a tail look damaged (and get repaired).
func TestRecoverTransientFlipNoRepair(t *testing.T) {
	mem := store.NewMem()
	db := mustOpen(t, mem)
	db.Put(ctx, "k", []byte("v"))
	db.Close()
	keys, _ := mem.List(ctx, "log/L1/")
	tail := keys[len(keys)-1]
	b, _ := mem.Get(ctx, tail)
	flipped := append([]byte{}, b...)
	flipped[len(flipped)-3] ^= 1
	st := newScripted(mem)
	st.gets[tail] = [][]byte{flipped, flipped}
	db = mustOpen(t, st, WithRepairTail(true))
	db.Close()
	if notes, _ := mem.List(ctx, "log/L1/"); strings.Contains(strings.Join(notes, " "), ".repair") {
		t.Fatalf("a transient flip was repaired: %v", notes)
	}
}
