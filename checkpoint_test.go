package freshdb

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/raman20/freshdb/store"
)

func TestCheckpointRoundTrip(t *testing.T) {
	c := &checkpoint{log: "L3", startSeq: 4, start: [32]byte{9}, lastEp: 7, lastTS: -5, tip: [32]byte{1, 2},
		segs: []segIdx{{seq: 5, first: 11, n: 2}, {seq: 7, first: 13, n: 1}}}
	b := encodeCheckpoint(c)
	got, err := decodeCheckpoint(b)
	if err != nil || string(encodeCheckpoint(got)) != string(b) {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	for i := range b {
		x := append([]byte{}, b...)
		x[i] ^= 0x10
		if _, err := decodeCheckpoint(x); err == nil {
			t.Fatalf("flip at %d accepted", i)
		}
	}
	for n := 0; n < len(b); n++ {
		if _, err := decodeCheckpoint(b[:n]); err == nil {
			t.Fatalf("truncation to %d accepted", n)
		}
	}
	// internally inconsistent indexes are rejected even with a valid CRC
	for name, segs := range map[string][]segIdx{
		"empty":         nil,
		"no frames":     {{seq: 5, first: 11, n: 0}},
		"before start":  {{seq: 3, first: 11, n: 1}},
		"not ascending": {{seq: 5, first: 11, n: 1}, {seq: 5, first: 12, n: 1}},
		"gap in LSNs":   {{seq: 5, first: 11, n: 1}, {seq: 6, first: 13, n: 1}},
		"overflow":      {{seq: 5, first: 1<<64 - 1, n: 2}},
	} {
		bad := *c
		bad.segs = segs
		if _, err := decodeCheckpoint(encodeCheckpoint(&bad)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func FuzzDecodeCheckpoint(f *testing.F) {
	f.Add(encodeCheckpoint(&checkpoint{log: "L1", startSeq: 1, segs: []segIdx{{seq: 2, first: 1, n: 3}}}))
	f.Add([]byte("FRDBIDX1"))
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := decodeCheckpoint(b)
		if err != nil {
			return
		}
		if string(encodeCheckpoint(c)) != string(b) {
			t.Fatalf("accepted bytes that do not re-encode identically")
		}
	})
}

// segGets counts segment GETs.
type segGets struct {
	store.Store
	mu sync.Mutex
	n  int
}

func (s *segGets) Get(ctx context.Context, k string) ([]byte, error) {
	if strings.HasSuffix(k, ".seg") {
		s.mu.Lock()
		s.n++
		s.mu.Unlock()
	}
	return s.Store.Get(ctx, k)
}

// recoverWith loads log L1 of db's store through a fresh DB (as open
// does) and returns it and the number of segment GETs recovery made.
func recoverWith(t *testing.T, db *DB, st store.Store, fullScan bool, logf ...func(string, ...any)) (*logState, int) {
	t.Helper()
	cs := &segGets{Store: st}
	m, err := loadLatestManifest(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	opts := db.opts
	if logf != nil {
		opts.Logf = logf[0]
	}
	fresh := &DB{st: cs, opts: opts, epoch: db.epoch, keys: db.keys, env: db.env, man: m, logs: map[string]*logState{}, fullScan: fullScan}
	l, err := fresh.loadLog(ctx, "L1", false)
	if err != nil {
		t.Fatal(err)
	}
	return l, cs.n
}

func sameIndex(t *testing.T, a, b *logState) {
	t.Helper()
	if a.n != b.n || a.tipCh != b.tipCh || a.lastTS != b.lastTS || a.lastEp != b.lastEp || a.nextSeq != b.nextSeq || len(a.segs) != len(b.segs) {
		t.Fatalf("checkpointed recovery differs from a full scan:\n%+v\n%+v", a, b)
	}
	for i := range a.segs {
		if a.segs[i] != b.segs[i] {
			t.Fatalf("segment %d: %+v vs %+v", i, a.segs[i], b.segs[i])
		}
	}
}

// With a checkpoint, recovery reads the checkpointed segment and the tail
// only, and ends with exactly the index a full scan builds.
func TestCheckpointRecoversTailOnly(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithSnapshotEvery(50))
	for i := 0; i < 103; i++ {
		if err := db.Put(ctx, "k", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	full, nFull := recoverWith(t, db, st, true)
	fast, nFast := recoverWith(t, db, st, false)
	sameIndex(t, fast, full)
	if nFull < 100 || nFast > 5 {
		t.Fatalf("segment GETs: full scan %d, with checkpoint %d", nFull, nFast)
	}
	db.Close()
	db = mustOpen(t, st)
	defer db.Close()
	if v, err := db.Get(ctx, "k"); err != nil || v[0] != 102 {
		t.Fatalf("k = %v %v", v, err)
	}
	if err := db.Put(ctx, "k", []byte("after")); err != nil {
		t.Fatal(err)
	}
	r, err := db.Verify(ctx)
	if err != nil || !r.OK || len(r.Warnings) != 0 {
		t.Fatalf("verify: %+v %v", r, err)
	}
}

// A checkpoint that is damaged, misplaced or does not match the log is
// skipped (with a log line): recovery falls back to an older checkpoint
// or a full scan, and Verify warns about it.
func TestCheckpointRejected(t *testing.T) {
	for name, mutate := range map[string]func(m map[string][]byte, keys []string){
		"flipped": func(m map[string][]byte, keys []string) {
			b := m[keys[2]]
			b[len(b)/2] ^= 1
		},
		"transplanted": func(m map[string][]byte, keys []string) { // an older one under the newest key
			m[keys[2]] = m[keys[0]]
		},
		"forged tip": func(m map[string][]byte, keys []string) { // valid CRC, wrong chain head
			c, _ := decodeCheckpoint(m[keys[2]])
			c.tip[0] ^= 1
			m[keys[2]] = encodeCheckpoint(c)
		},
		"forged epoch": func(m map[string][]byte, keys []string) {
			c, _ := decodeCheckpoint(m[keys[2]])
			c.lastEp++
			m[keys[2]] = encodeCheckpoint(c)
		},
		"forged start": func(m map[string][]byte, keys []string) { // e.g. another branch's history
			c, _ := decodeCheckpoint(m[keys[2]])
			c.start[0] ^= 1
			m[keys[2]] = encodeCheckpoint(c)
		},
		"missing first segment": func(m map[string][]byte, keys []string) {
			c, _ := decodeCheckpoint(m[keys[2]])
			c.segs = c.segs[1:]
			m[keys[2]] = encodeCheckpoint(c)
		},
	} {
		t.Run(name, func(t *testing.T) {
			st := store.NewMem()
			db := mustOpen(t, st, WithSnapshotEvery(10))
			for i := 0; i < 35; i++ {
				db.Put(ctx, "k", []byte{byte(i)})
			}
			want, _ := recoverWith(t, db, st, true)
			keys, _ := st.List(ctx, "log/L1/")
			var idx []string
			for _, k := range keys {
				if strings.HasSuffix(k, ".idx") {
					idx = append(idx, k)
				}
			}
			if len(idx) != 3 {
				t.Fatalf("checkpoints: %v", idx)
			}
			st.Raw(func(m map[string][]byte) { mutate(m, idx) })
			var logs []string
			got, n := recoverWith(t, db, st, false, func(f string, a ...any) { logs = append(logs, f) })
			sameIndex(t, got, want)
			if !strings.Contains(strings.Join(logs, ";"), "ignoring checkpoint") || n > 20 {
				t.Fatalf("logs %q, %d segment GETs (older checkpoint should be used)", logs, n)
			}
			r, err := db.Verify(ctx)
			if err != nil || !r.OK || len(r.Warnings) != 1 || !strings.Contains(r.Warnings[0], idx[2]) {
				t.Fatalf("verify: %+v %v", r, err)
			}
			db.Close()
		})
	}
}

// After compaction the log starts at its base: checkpoints from before it
// (kept for the grace period) are not used, new ones are, and GC deletes
// the old ones with the segments they cover.
func TestCheckpointAcrossCompaction(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithSnapshotEvery(10))
	for i := 0; i < 25; i++ {
		db.Put(ctx, "k", []byte{byte(i)})
	}
	if _, err := db.Compact(ctx, CompactOptions{Grace: 1 << 40}); err != nil {
		t.Fatal(err)
	}
	for i := 25; i < 47; i++ {
		db.Put(ctx, "k", []byte{byte(i)})
	}
	want, _ := recoverWith(t, db, st, true)
	got, n := recoverWith(t, db, st, false)
	sameIndex(t, got, want)
	if n > 10 {
		t.Fatalf("%d segment GETs after compaction", n)
	}
	r, err := db.Verify(ctx)
	if err != nil || !r.OK || len(r.Warnings) != 0 {
		t.Fatalf("verify: %+v %v", r, err)
	}
	db.Close()
	db = mustOpen(t, st)
	defer db.Close()
	if v, err := db.Get(ctx, "k"); err != nil || v[0] != 46 {
		t.Fatalf("k = %v %v", v, err)
	}
}
