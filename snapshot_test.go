package freshdb

import (
	"errors"
	"strings"
	"testing"

	"github.com/raman20/freshdb/store"
)

func TestSnapshotRoundTrip(t *testing.T) {
	h := snapHeader{log: "L7", lsn: 42, chain: [32]byte{1, 2, 3}}
	b := encodeSnapshot(h, []byte("kv-bytes"), []byte("topic"))
	got, kv, tp, err := decodeSnapshot(b)
	if err != nil || got != h || string(kv) != "kv-bytes" || string(tp) != "topic" {
		t.Fatalf("round trip: %+v %q %q %v", got, kv, tp, err)
	}
	for i := range b { // every single-byte change is caught
		c := append([]byte{}, b...)
		c[i] ^= 0x40
		if _, _, _, err := decodeSnapshot(c); err == nil {
			t.Fatalf("flip at %d accepted", i)
		}
	}
	for n := 0; n < len(b); n++ {
		if _, _, _, err := decodeSnapshot(b[:n]); err == nil {
			t.Fatalf("truncation to %d accepted", n)
		}
	}
}

func FuzzDecodeSnapshot(f *testing.F) {
	f.Add(encodeSnapshot(snapHeader{log: "L1", lsn: 9}, []byte("kv"), nil))
	f.Add([]byte("FRSNAP2\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, kv, tp, err := decodeSnapshot(b)
		if err != nil {
			return
		}
		if again := encodeSnapshot(h, kv, tp); string(again) != string(b) {
			t.Fatalf("accepted bytes that do not re-encode identically")
		}
	})
}

// A snapshot is used only if it is bound to this log's history: another
// log's snapshot, or one whose hash chain differs, is ignored (replay).
func TestSnapshotBinding(t *testing.T) {
	st := store.NewMem()
	var logs []string
	db := mustOpen(t, st, WithSnapshotEvery(2), WithLogf(func(f string, a ...any) {
		if strings.Contains(f, "ignoring snapshot") {
			logs = append(logs, f)
		}
	}))
	for i := 0; i < 4; i++ {
		db.Put(ctx, "k", []byte{byte('a' + i)})
	}
	key := db.Manifest().Logs["L1"].SnapKey
	db.Close()
	if key == "" {
		t.Fatal("no snapshot")
	}
	open := func() string {
		logs = nil
		db, err := OpenStore(ctx, st, WithLogf(func(f string, a ...any) {
			if strings.Contains(f, "ignoring snapshot") {
				logs = append(logs, f)
			}
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if v, err := db.Get(ctx, "k"); err != nil || string(v) != "d" {
			t.Fatalf("k = %q %v", v, err)
		}
		return strings.Join(logs, ";")
	}
	if got := open(); got != "" {
		t.Fatalf("good snapshot ignored: %s", got)
	}
	orig, _ := st.Get(ctx, key)
	h, kv, tp, _ := decodeSnapshot(orig)
	for name, bad := range map[string]snapHeader{
		"other chain": {log: h.log, lsn: h.lsn, chain: [32]byte{9}},
		"other log":   {log: "L9", lsn: h.lsn, chain: h.chain},
		"other lsn":   {log: h.log, lsn: h.lsn - 1, chain: h.chain},
	} {
		st.Raw(func(m map[string][]byte) { m[key] = encodeSnapshot(bad, kv, tp) })
		if got := open(); !strings.Contains(got, "ignoring snapshot") {
			t.Errorf("%s: snapshot used", name)
		}
	}
	// v1 (unbound) snapshots are ignored
	st.Raw(func(m map[string][]byte) { m[key] = append([]byte{8, 0, 0, 0, 0, 0, 0, 0}, orig[8:]...) })
	if got := open(); !strings.Contains(got, "ignoring snapshot") {
		t.Error("v1 snapshot used")
	}
	if _, _, _, err := decodeSnapshot(nil); !errors.Is(err, errSnapshot) {
		t.Fatal(err)
	}
}
