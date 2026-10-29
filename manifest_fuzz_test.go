package freshdb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/store"
)

// fuzzBase is a small database (puts, a branch, a snapshot, a compaction
// base) whose objects every FuzzOpenManifest run starts from.
func fuzzBase(f *testing.F) (map[string][]byte, uint64, []byte) {
	ctx := context.Background()
	st := store.NewMem()
	db, err := OpenStore(ctx, st, WithSnapshotEvery(4), WithReadAhead(1))
	if err != nil {
		f.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := db.Put(ctx, "k"+string(rune('a'+i)), []byte("v")); err != nil {
			f.Fatal(err)
		}
	}
	if err := db.CreateBranch(ctx, "dev", "main", 0); err != nil {
		f.Fatal(err)
	}
	if _, err := db.Compact(ctx, CompactOptions{}); err != nil {
		f.Fatal(err)
	}

	if err := db.Close(); err != nil {
		f.Fatal(err)
	}
	objs := map[string][]byte{}
	st.Raw(func(o map[string][]byte) {
		for k, v := range o {
			objs[k] = append([]byte(nil), v...)
		}
	})
	last, err := loadLatestManifest(ctx, st)
	if err != nil {
		f.Fatal(err)
	}
	j, _ := json.Marshal(last)
	return objs, last.ID, j
}

// FuzzOpenManifest: whatever a checksummed manifest says, opening the
// database returns an error or a working DB, never a panic.
func FuzzOpenManifest(f *testing.F) {
	objs, id, seed := fuzzBase(f)
	f.Add(seed)
	var so map[string]any
	json.Unmarshal(seed, &so)
	so["id"] = id + 1
	seedPinned, _ := json.Marshal(so)
	f.Add([]byte(`{"format":1,"id":1,"logs":{"L1":null},"refs":{"main":"L1"}}`))
	f.Add([]byte(`{"format":2,"logs":{"L1":{"id":"L1","parent":"L1"}},"refs":{"main":"L1"}}`))
	f.Add([]byte(`{"format":2,"logs":{"L1":{"id":"L1","base":{"seq":0,"chunks":-1}}},"refs":{"main":"L1"}}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		ctx := context.Background()
		st := store.NewMem()
		st.Raw(func(o map[string][]byte) {
			for k, v := range objs {
				o[k] = v
			}
		})
		// Pin the id (a mismatch is refused before anything else runs).
		var obj map[string]any
		if json.Unmarshal(b, &obj) == nil && obj != nil {
			obj["id"] = id + 1
			b, _ = json.Marshal(obj)
		}
		raw := binary.LittleEndian.AppendUint32(append([]byte(nil), b...), format.CRC(b))
		if err := st.PutIfAbsent(ctx, manifestKey(id+1), raw); err != nil {
			t.Fatal(err)
		}
		db, err := OpenStore(ctx, st, WithReadAhead(1), WithLogf(func(string, ...any) {}))
		if err != nil {
			if string(b) == string(seedPinned) {
				t.Fatalf("the unmodified manifest does not open: %v", err)
			}
			return
		}
		for br := range db.Branches() {
			db.Branch(br).View(ctx, func(tx *Tx) error { _, err := tx.Get("ka"); return err })
		}
		db.Put(ctx, "kz", []byte("w"))
		db.Close()
	})
}
