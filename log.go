package freshdb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// logState is the in-memory image of one log (one branch lineage segment).
// Segments are immutable objects log/<id>/<seq>.seg, seq dense from 1.
type logState struct {
	meta    LogMeta
	parent  *logState
	mu      sync.RWMutex
	entries []*lens.Entry // own frames only, LSN = meta.ParentLSN+1 ...
	chains  [][32]byte    // chain head after each entry
	base    [32]byte      // chain head at ParentLSN (from parent)
	nextSeq uint64
	lastEp  uint64
	repairs int
}

func segKey(log string, seq uint64) string { return fmt.Sprintf("log/%s/%020d.seg", log, seq) }

func (l *logState) head() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.meta.ParentLSN + uint64(len(l.entries))
}

// chainAt returns the hash-chain head after LSN lsn in this lineage.
func (l *logState) chainAt(lsn uint64) [32]byte {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if lsn <= l.meta.ParentLSN {
		if l.parent == nil {
			return [32]byte{}
		}
		return l.parent.chainAt(lsn)
	}
	return l.chains[lsn-l.meta.ParentLSN-1]
}

// lineage returns every entry visible on this log up to LSN `upto`, oldest
// first (parent's entries up to ParentLSN, then our own).
func (l *logState) lineage(upto uint64) []*lens.Entry {
	var out []*lens.Entry
	if l.parent != nil {
		out = l.parent.lineage(min(upto, l.meta.ParentLSN))
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, e := range l.entries {
		if e.LSN > upto {
			break
		}
		out = append(out, e)
	}
	return out
}

// recoverLog reads a log's segments. For the writable (lease-holding) log
// it also repairs a torn final segment: the damaged object is copied to
// quarantine/ and replaced by its valid prefix. Damage anywhere else is an
// error: we never silently drop acknowledged data.
func (db *DB) recoverLog(ctx context.Context, l *logState, writable bool) error {
	st := db.st
	l.mu.Lock()
	defer l.mu.Unlock()
	keys, err := st.List(ctx, "log/"+l.meta.ID+"/")
	if err != nil {
		return err
	}
	// resume after what we already have (re-recovery after a fencing race)
	expectSeq := l.nextSeq
	if expectSeq == 0 {
		expectSeq = 1
	}
	for _, k := range keys {
		seq, ok := parseNum(k, "log/"+l.meta.ID+"/", ".seg")
		if !ok || seq < expectSeq {
			continue
		}
		if seq != expectSeq {
			return fmt.Errorf("%w: log %s: missing segment %d (found %d)", ErrCorrupt, l.meta.ID, expectSeq, seq)
		}
		data, err := st.Get(ctx, k)
		if err != nil {
			return err
		}
		h, frames, valid, derr := format.DecodeSegment(data)
		last := k == keys[len(keys)-1]
		if derr != nil && !(errors.Is(derr, format.ErrTorn) || errors.Is(derr, format.ErrHeader)) {
			return derr
		}
		if derr == nil || errors.Is(derr, format.ErrTorn) {
			if h.Seq != seq {
				return fmt.Errorf("%w: segment %s header seq %d", ErrCorrupt, k, h.Seq)
			}
			if h.Epoch < l.lastEp {
				return fmt.Errorf("%w: segment %s epoch %d < %d (fencing violated)", ErrCorrupt, k, h.Epoch, l.lastEp)
			}
		}
		if derr != nil {
			if !last {
				return fmt.Errorf("%w: damaged segment %s is not the tail: %v", ErrCorrupt, k, derr)
			}
			if !writable {
				// read-only view: use the valid prefix, ignore the tail
				if errors.Is(derr, format.ErrHeader) {
					break
				}
			} else {
				if l.lastEp > db.epoch || h.Epoch > db.epoch {
					return fmt.Errorf("%w: log %s superseded (epoch %d)", ErrFenced, l.meta.ID, max(l.lastEp, h.Epoch))
				}
				if err := db.repairTail(ctx, l, k, seq, data, h, frames, valid, derr); err != nil {
					return err
				}
				if errors.Is(derr, format.ErrHeader) {
					frames = nil
				}
				h.Epoch = max(h.Epoch, db.epoch)
				l.repairs++
			}
		}
		for _, d := range frames {
			f := d.Frame
			want := l.meta.ParentLSN + uint64(len(l.entries)) + 1
			if f.LSN != want {
				return fmt.Errorf("%w: log %s: LSN %d, expected %d (duplicate or gap)", ErrCorrupt, l.meta.ID, f.LSN, want)
			}
			prev := l.base
			if n := len(l.chains); n > 0 {
				prev = l.chains[n-1]
			}
			if f.Prev != prev {
				return fmt.Errorf("%w: log %s: hash chain broken at LSN %d", ErrCorrupt, l.meta.ID, f.LSN)
			}
			l.entries = append(l.entries, &lens.Entry{Log: l.meta.ID, LSN: f.LSN, TS: f.TS, Ops: f.Ops})
			l.chains = append(l.chains, format.ChainNext(prev, d.Payload))
		}
		if h.Epoch > l.lastEp {
			l.lastEp = h.Epoch
		}
		expectSeq = seq + 1
	}
	l.nextSeq = expectSeq
	if writable && l.lastEp > db.epoch {
		// A newer writer already fenced this log (found by TLC, FreshLog.tla:
		// a slow opener must not append an older-epoch fence after a newer
		// one, which would make the log unrecoverable).
		return fmt.Errorf("%w: log %s has epoch %d > ours %d", ErrFenced, l.meta.ID, l.lastEp, db.epoch)
	}
	return nil
}

func (db *DB) repairTail(ctx context.Context, l *logState, k string, seq uint64, data []byte, h format.SegHeader, frames []format.Decoded, valid int, derr error) error {
	q := fmt.Sprintf("quarantine/%s/%020d-%d.seg", l.meta.ID, seq, db.epoch)
	if err := db.st.PutIfAbsent(ctx, q, data); err != nil && !errors.Is(err, store.ErrExists) {
		return err
	}
	if err := db.st.Delete(ctx, k); err != nil {
		return err
	}
	var fixed []byte
	if errors.Is(derr, format.ErrHeader) {
		fixed = format.EncodeHeader(format.SegHeader{Epoch: db.epoch, Seq: seq})
	} else {
		fixed = append([]byte{}, data[:valid]...)
	}
	db.logf("freshdb: repaired torn tail of %s (%d valid frames kept, damaged copy at %s)", k, len(frames), q)
	return putExactlyOnce(ctx, db.st, k, fixed)
}

// fence writes an empty segment with our epoch at the next sequence number.
// Any older writer (zombie) will fail its next create-only segment write.
// If a zombie got there first, absorb its segment and retry.
func (db *DB) fence(ctx context.Context, l *logState) error {
	for i := 0; i < 64; i++ {
		l.mu.RLock()
		seq := l.nextSeq
		l.mu.RUnlock()
		hdr := format.EncodeHeader(format.SegHeader{Epoch: db.epoch, Seq: seq})
		err := putExactlyOnce(ctx, db.st, segKey(l.meta.ID, seq), hdr)
		if err == nil {
			l.mu.Lock()
			l.nextSeq = seq + 1
			l.lastEp = db.epoch
			l.mu.Unlock()
			return nil
		}
		if !errors.Is(err, store.ErrExists) {
			return err
		}
		if err := db.recoverLog(ctx, l, true); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: could not fence log %s", ErrFenced, l.meta.ID)
}

// entryAt returns the entry with the given LSN in this lineage.
func (l *logState) entryAt(lsn uint64) *lens.Entry {
	l.mu.RLock()
	if lsn > l.meta.ParentLSN {
		i := lsn - l.meta.ParentLSN - 1
		var e *lens.Entry
		if i < uint64(len(l.entries)) {
			e = l.entries[i]
		}
		l.mu.RUnlock()
		return e
	}
	l.mu.RUnlock()
	if l.parent == nil || lsn == 0 {
		return nil
	}
	return l.parent.entryAt(lsn)
}
