//go:build unix

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

// Power-loss model for the journal (dm-log-writes style, at the file
// level): a workload runs on a real journal while jtrace records every
// write and sync in order. Then, for every interval between two syncs, we
// build the file a power cut inside that interval can leave behind: every
// write before the interval's opening sync is on disk; of the writes
// inside it, none, all, or a random subset of 512-byte sectors (torn and
// reordered writes). Each image must open without error, contain every
// object whose PutIfAbsent returned (and none whose Delete returned),
// never return bytes that were not put, and survive a further append and
// reopen.
func TestJournalPowerLoss(t *testing.T) {
	seeds := 3
	if testing.Short() || raceEnabled {
		seeds = 1
	}
	if n, err := strconv.Atoi(os.Getenv("FRESHDB_POWERLOSS_SEEDS")); err == nil {
		seeds = n // long runs: scripts/soak
	}
	// small preallocation steps: more grow/reserve events, smaller images
	c, a, b := jrnChunk, jrnGrowAt, jrnGrowBit
	jrnChunk, jrnGrowAt, jrnGrowBit = 64<<10, 32<<10, 16<<10
	defer func() { jrnChunk, jrnGrowAt, jrnGrowBit = c, a, b }()
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) { powerLoss(t, seed) })
	}
}

type plAck struct {
	from int // event index when the call began
	at   int // event index of the ack
	del  bool
	key  string
	data []byte
}

func powerLoss(t *testing.T, seed int64) {
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(seed))
	d := t.TempDir()
	var mu sync.Mutex
	var evs []jev
	jtrace = func(ev jev) { mu.Lock(); evs = append(evs, ev); mu.Unlock() }
	ack := func() int { mu.Lock(); defer mu.Unlock(); return len(evs) }
	var acks []plAck
	puts := map[string][]byte{} // every put attempted: the only bytes that may appear
	j := mustJournal(t, d)
	var live []string
	for i := 0; i < 150; i++ {
		switch r := rnd.Intn(20); {
		case r == 0: // reopen: open's recovery writes are traced too
			j.Close()
			j = mustJournal(t, d)
		case r < 6 && len(live) > 0:
			x := rnd.Intn(len(live))
			k := live[x]
			live = append(live[:x], live[x+1:]...)
			from := ack()
			if err := j.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
			acks = append(acks, plAck{from: from, at: ack(), del: true, key: k})
		default:
			k := fmt.Sprintf("log/L1/%04d.seg", i)
			v := make([]byte, []int{1, 100, 511, 513, 4000, 70000}[rnd.Intn(6)])
			rnd.Read(v)
			puts[k] = v
			from := ack()
			if err := j.PutIfAbsent(ctx, k, v); err != nil {
				t.Fatal(err)
			}
			acks = append(acks, plAck{from: from, at: ack(), key: k, data: v})
			live = append(live, k)
		}
	}
	j.Close()
	jtrace = nil

	var syncs []int
	for i, e := range evs {
		if e.kind == 's' {
			syncs = append(syncs, i)
		}
	}
	syncs = append(syncs, len(evs))
	apply := func(img []byte, e jev, keep func(int) bool) []byte {
		for s := 0; s*512 < len(e.data); s++ {
			if keep != nil && !keep(s) {
				continue
			}
			lo, hi := s*512, min((s+1)*512, len(e.data))
			end := e.off + int64(hi)
			if int64(len(img)) < end {
				img = append(img, make([]byte, end-int64(len(img)))...)
			}
			copy(img[e.off+int64(lo):], e.data[lo:hi])
		}
		return img
	}
	var durable []byte
	next, images := 0, 0
	for i, s := range syncs {
		for ; next < s; next++ { // writes before this interval's opening sync
			if evs[next].kind == 'w' {
				durable = apply(durable, evs[next], nil)
			}
		}
		end := len(evs)
		if i+1 < len(syncs) {
			end = syncs[i+1]
		}
		var pending []jev
		for _, e := range evs[s:min(end, len(evs))] {
			if e.kind == 'w' {
				pending = append(pending, e)
			}
		}
		variants := 1
		if len(pending) > 0 {
			variants = 4
		}
		for v := 0; v < variants; v++ {
			img := append([]byte(nil), durable...)
			for _, e := range pending {
				switch v {
				case 0: // none
				case 1:
					img = apply(img, e, nil)
				default:
					img = apply(img, e, func(int) bool { return rnd.Intn(2) == 0 })
				}
			}
			images++
			checkImage(t, img, end, acks, puts, fmt.Sprintf("interval %d/%d variant %d", i, len(syncs), v))
			if t.Failed() {
				return
			}
		}
	}
	t.Logf("seed %d: %d events, %d syncs, %d crash images checked", seed, len(evs), len(syncs)-1, images)
}

func checkImage(t *testing.T, img []byte, crashAt int, acks []plAck, puts map[string][]byte, what string) {
	t.Helper()
	ctx := context.Background()
	d := t.TempDir()
	if err := writeSparse(filepath.Join(d, jrnFile), img); err != nil {
		t.Fatal(err)
	}
	j, err := NewJournal(d)
	if err != nil {
		t.Errorf("%s: open: %v", what, err)
		return
	}
	defer j.Close()
	j.SetUnsafeNoSync(true) // durability of the checks themselves is not under test
	want := map[string][]byte{}
	gone := map[string]bool{}
	for _, a := range acks {
		if a.at > crashAt {
			if a.del && a.from < crashAt { // in flight: either outcome
				delete(want, a.key)
			}
			continue
		}
		if a.del {
			delete(want, a.key)
			gone[a.key] = true
		} else {
			want[a.key] = a.data
		}
	}
	for k, v := range want {
		got, err := j.Get(ctx, k)
		if err != nil || !bytes.Equal(got, v) {
			t.Errorf("%s: acknowledged %s: %d bytes, %v (end %d, len %d, key at %d)", what, k, len(got), err, j.end, len(img), bytes.Index(img, []byte(k)))
			return
		}
	}
	keys, _ := j.Keys(ctx)
	for _, k := range keys {
		if gone[k] {
			t.Errorf("%s: deleted %s is back", what, k)
			return
		}
		got, err := j.Get(ctx, k)
		if p, ok := puts[k]; !ok || err != nil || !bytes.Equal(got, p) {
			t.Errorf("%s: object %s has bytes that were never put (%v)", what, k, err)
			return
		}
	}
	if err := j.PutIfAbsent(ctx, "after/crash", []byte("x")); err != nil {
		t.Errorf("%s: append after recovery: %v", what, err)
		return
	}
	j.Close()
	j2, err := NewJournal(d)
	if err != nil {
		t.Errorf("%s: second open: %v", what, err)
		return
	}
	defer j2.Close()
	if _, err := j2.Get(ctx, "after/crash"); err != nil {
		t.Errorf("%s: append after recovery lost: %v", what, err)
	}
	for k := range want {
		if _, err := j2.Get(ctx, k); err != nil {
			t.Errorf("%s: %s lost on second open: %v", what, k, errors.Unwrap(err))
			return
		}
	}
}

// writeSparse writes only the non-zero 4 KiB blocks (the images are
// mostly preallocated zeros).
func writeSparse(p string, img []byte) error {
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()
	zero := make([]byte, 4096)
	for off := 0; off < len(img); off += 4096 {
		b := img[off:min(off+4096, len(img))]
		if !bytes.Equal(b, zero[:len(b)]) {
			if _, err := f.WriteAt(b, int64(off)); err != nil {
				return err
			}
		}
	}
	return f.Truncate(int64(len(img)))
}
