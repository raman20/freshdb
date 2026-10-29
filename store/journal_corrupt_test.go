//go:build unix

package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// journalWith writes n puts k0..k(n-1) and returns the dir and the offset
// of each record (closing the journal).
func journalWith(t *testing.T, n int) (string, []int64) {
	t.Helper()
	ctx := context.Background()
	d := t.TempDir()
	j := mustJournal(t, d)
	var offs []int64
	for i := 0; i < n; i++ {
		offs = append(offs, j.end)
		if err := j.PutIfAbsent(ctx, "k"+string(rune('0'+i)), bytes.Repeat([]byte{byte('a' + i)}, 100)); err != nil {
			t.Fatal(err)
		}
	}
	j.Close()
	return d, offs
}

func flip(t *testing.T, d string, off int64) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(d, jrnFile), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var b [1]byte
	f.ReadAt(b[:], off)
	b[0] ^= 0x10
	f.WriteAt(b[:], off)
}

// A damaged record with valid records after it must not be treated as a
// torn tail (that hid every later record, and the next append overwrote
// them): open refuses with ErrCorrupt, whether the data, the key or the
// header is hit.
func TestJournalMidFileCorruptionRefusesOpen(t *testing.T) {
	for name, at := range map[string]int64{"data": jrnHdr + 2 + 50, "key": jrnHdr, "crc": 0, "kind": 4, "len": 9} {
		t.Run(name, func(t *testing.T) {
			d, offs := journalWith(t, 4)
			flip(t, d, offs[1]+at)
			before, _ := os.ReadFile(filepath.Join(d, jrnFile))
			if _, err := NewJournal(d); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("open: %v, want ErrCorrupt", err)
			}
			after, _ := os.ReadFile(filepath.Join(d, jrnFile))
			if !bytes.Equal(before, after) {
				t.Fatal("refused open modified the file")
			}
		})
	}
}

// The last record damaged with nothing valid after it is a torn append:
// open drops it, zeroes its bytes and later appends survive reopen.
func TestJournalTornTailZeroed(t *testing.T) {
	ctx := context.Background()
	d, offs := journalWith(t, 3)
	flip(t, d, offs[2]+jrnHdr+20)
	j := mustJournal(t, d)
	if _, err := j.Get(ctx, "k2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("torn record: %v", err)
	}
	if j.end != offs[2] {
		t.Fatalf("end %d, want %d", j.end, offs[2])
	}
	raw, _ := os.ReadFile(filepath.Join(d, jrnFile))
	if bytes.Count(raw[offs[2]:], []byte{0}) != len(raw)-int(offs[2]) {
		t.Fatal("torn bytes not zeroed")
	}
	if err := j.PutIfAbsent(ctx, "z", []byte("short")); err != nil {
		t.Fatal(err)
	}
	j.Close()
	j2 := mustJournal(t, d)
	if v, err := j2.Get(ctx, "z"); err != nil || string(v) != "short" {
		t.Fatalf("after torn tail: %q %v", v, err)
	}
	j2.Close()
}

// A crash inside erase (the dead-marking header write torn, or the hole
// punched before the header reached the disk) leaves a put that fails its
// CRC with its delete record after it: open finishes the delete.
func TestJournalTornErase(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"crc", "punched"} {
		t.Run(mode, func(t *testing.T) {
			d, offs := journalWith(t, 3)
			raw, _ := os.ReadFile(filepath.Join(d, jrnFile))
			orig := append([]byte{}, raw[offs[1]:offs[2]]...)
			j := mustJournal(t, d)
			if err := j.Delete(ctx, "k1"); err != nil {
				t.Fatal(err)
			}
			j.Close()
			f, _ := os.OpenFile(filepath.Join(d, jrnFile), os.O_RDWR, 0)
			if mode == "crc" { // new kind byte, old CRC
				f.WriteAt(orig[:4], offs[1])
			} else { // old header (kind put, old CRC), data already punched
				f.WriteAt(orig[:jrnHdr], offs[1])
			}
			f.Close()
			j2 := mustJournal(t, d)
			if _, err := j2.Get(ctx, "k1"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted object: %v", err)
			}
			for _, k := range []string{"k0", "k2"} {
				if _, err := j2.Get(ctx, k); err != nil {
					t.Fatalf("%s: %v", k, err)
				}
			}
			j2.Close()
			j3 := mustJournal(t, d) // the re-erase made the record valid (dead)
			j3.Close()
		})
	}
}

// A live object whose bytes rot after open reads as ErrCorrupt, not as
// "not found".
func TestJournalGetCorrupt(t *testing.T) {
	ctx := context.Background()
	d, offs := journalWith(t, 2)
	j := mustJournal(t, d)
	defer j.Close()
	flip(t, d, offs[0]+jrnHdr+2+10)
	if _, err := j.Get(ctx, "k0"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
	if _, err := j.Get(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
}

// Truncating the file (losing records) is detected by the engine, not
// here; but truncation must not make open fail or invent records.
func TestJournalTruncated(t *testing.T) {
	ctx := context.Background()
	d, offs := journalWith(t, 3)
	if err := os.Truncate(filepath.Join(d, jrnFile), offs[2]+jrnHdr+5); err != nil {
		t.Fatal(err)
	}
	j := mustJournal(t, d)
	defer j.Close()
	if _, err := j.Get(ctx, "k2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("truncated record: %v", err)
	}
	if _, err := j.Get(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
}
