package freshdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/store"
)

// A transaction over the frame limit used to be written anyway; every
// later open then failed on it (decoded as corrupt). Now it is refused
// before anything is written.
func TestTooLargeTxRefused(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	big := make([]byte, MaxTxBytes+1)
	if err := db.Put(ctx, "big", big); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("put %d bytes: %v, want ErrTooLarge", len(big), err)
	}
	if err := db.Put(ctx, "small", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db = mustOpen(t, st)
	defer db.Close()
	if v, err := db.Get(ctx, "small"); err != nil || string(v) != "ok" {
		t.Fatalf("after reopen: %q %v", v, err)
	}
	if _, err := db.Get(ctx, "big"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("refused tx visible: %v", err)
	}
}

// A group commit stops adding transactions once its segment is full; the
// rest go to the next segment.
func TestSegmentSizeCap(t *testing.T) {
	old := maxSegBytes
	maxSegBytes = 1 << 20
	defer func() { maxSegBytes = old }()
	st := store.NewMem()
	db := mustOpen(t, st)
	val := bytes.Repeat([]byte{1}, 300<<10)
	done := make(chan error)
	for i := 0; i < 12; i++ {
		go func(i int) { done <- db.Put(ctx, string(rune('a'+i)), val) }(i)
	}
	for i := 0; i < 12; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	st.Raw(func(o map[string][]byte) {
		for k, v := range o {
			if strings.HasPrefix(k, "log/") && len(v) > 1<<20+400<<10 {
				t.Errorf("%s: %d bytes, cap 1 MiB + one tx", k, len(v))
			}
		}
	})
	db = mustOpen(t, st)
	defer db.Close()
	for i := 0; i < 12; i++ {
		if v, err := db.Get(ctx, string(rune('a'+i))); err != nil || len(v) != len(val) {
			t.Fatalf("%d: %d %v", i, len(v), err)
		}
	}
}

func segKeys(st *store.Mem) []string {
	var ks []string
	st.Raw(func(o map[string][]byte) {
		for k := range o {
			if strings.HasPrefix(k, "log/L1/") {
				ks = append(ks, k)
			}
		}
	})
	sort.Strings(ks)
	return ks
}

// A segment with an intact header of an unknown (newer) version is
// refused with ErrVersion, never "repaired" as a torn tail, even with
// WithRepairTail.
func TestNewerSegmentVersionRefused(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	db.Put(ctx, "a", []byte("1"))
	db.Close()
	ks := segKeys(st)
	last := ks[len(ks)-1]
	var orig []byte
	st.Raw(func(o map[string][]byte) {
		orig = append([]byte{}, o[last]...)
		b := o[last]
		binary.LittleEndian.PutUint16(b[8:], format.SegVersion+1)
		binary.LittleEndian.PutUint32(b[28:], format.CRC(b[:28]))
	})
	for _, repair := range []bool{false, true} {
		if _, err := OpenStore(ctx, st, quiet(), WithRepairTail(repair)); !errors.Is(err, format.ErrVersion) {
			t.Fatalf("repair=%v: %v, want ErrVersion", repair, err)
		}
	}
	st.Raw(func(o map[string][]byte) {
		if bytes.Equal(o[last], orig) {
			t.Fatal("test bug: segment not modified")
		}
		if binary.LittleEndian.Uint16(o[last][8:]) != format.SegVersion+1 {
			t.Fatal("newer segment was rewritten")
		}
	})
}

// Losing the newest segments (e.g. a restore from an older copy of the
// log directory) is detected when the manifest points past the end.
func TestMissingNewestSegmentsDetected(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithSnapshotEvery(1))
	for i := 0; i < 5; i++ {
		db.Put(ctx, string(rune('a'+i)), []byte("v"))
	}
	db.Close()
	ks := segKeys(st)
	// drop every segment from the one holding the newest frame on
	cut := -1
	st.Raw(func(o map[string][]byte) {
		for i, k := range ks {
			if _, fr, _, err := format.DecodeSegment(o[k]); err == nil && len(fr) > 0 {
				cut = i
			}
		}
		for _, k := range ks[cut:] {
			delete(o, k)
		}
	})
	if _, err := OpenStore(ctx, st, quiet()); !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("open: %v, want ErrCorrupt (newest segments missing)", err)
	}
}

// A journal-backed DB with a flipped bit inside a segment that is not the
// last record refuses to open (it used to silently drop every later
// record, and the next commit overwrote them).
func TestJournalBitFlipRefusesOpen(t *testing.T) {
	d := t.TempDir()
	st, err := store.NewJournal(d)
	if err != nil {
		t.Skip(err) // no journal store on this OS
	}
	db := mustOpen(t, st)
	for i := 0; i < 5; i++ {
		db.Put(ctx, string(rune('a'+i)), bytes.Repeat([]byte("payload-"), 8))
	}
	db.Close()
	st.Close()
	p := filepath.Join(d, "journal.fj")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	k := []byte("log/L1/00000000000000000002.seg")
	i := bytes.Index(raw, k)
	if i < 0 {
		t.Fatal("segment 2 not found")
	}
	raw[i+len(k)+40] ^= 1 // inside its frames
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.NewJournal(d); !errors.Is(err, store.ErrCorrupt) {
		t.Fatalf("open journal: %v, want store.ErrCorrupt", err)
	}
}

// verify is a scrub: it reads every object back, so damage in an object
// the open path never reads (here: a snapshot) is reported.
func TestVerifyScrub(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st, WithSnapshotEvery(2))
	for i := 0; i < 6; i++ {
		db.Put(ctx, string(rune('a'+i)), []byte("v"))
	}
	r, err := db.Verify(ctx)
	if err != nil || !r.OK || !r.Scrubbed || r.Objects == 0 || r.Snapshots == 0 {
		t.Fatalf("clean verify: %+v %v", r, err)
	}
	sk := db.Manifest().Logs["L1"].SnapKey
	st.Raw(func(o map[string][]byte) { o[sk][len(o[sk])/2] ^= 1 })
	r, err = db.Verify(ctx)
	if err != nil || !strings.Contains(strings.Join(r.Warnings, ";"), sk) {
		t.Fatalf("damaged snapshot not reported: %+v %v", r, err)
	}
	db.Close()
}
