package freshdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// Snapshots checkpoint a log's KV and topic state so open need not replay
// the whole log. They are a cache: a missing, damaged or mismatched
// snapshot is ignored (with a log line) and the log is replayed instead.
//
// Layout (v2; formats.md):
//
//	magic "FRSNAP2\n" | lsn u64 | chain [32]byte | idLen u8 | log id
//	| kvLen u64 | kv | topic | crc32c u32 (over everything before it)
//
// The log id and the hash-chain head at lsn bind a snapshot to the exact
// history it summarises: bytes that belong to another log (or another
// branch of the same history) are rejected, not loaded. v1 snapshots
// (no magic, no binding) are ignored.

var snapMagic = [8]byte{'F', 'R', 'S', 'N', 'A', 'P', '2', '\n'}

type snapshot struct {
	lsn   uint64
	kv    *lens.KV
	topic *lens.Topic
	drop  []string
}

// snapHeader is what binds a snapshot to its history.
type snapHeader struct {
	log   string
	lsn   uint64
	chain [32]byte
}

func snapKey(log string, lsn uint64) string { return fmt.Sprintf("snap/%s/%020d.snap", log, lsn) }

func encodeSnapshot(h snapHeader, kv, topic []byte) []byte {
	b := make([]byte, 0, len(snapMagic)+8+32+1+len(h.log)+8+len(kv)+len(topic)+4)
	b = append(b, snapMagic[:]...)
	b = binary.LittleEndian.AppendUint64(b, h.lsn)
	b = append(b, h.chain[:]...)
	b = append(b, byte(len(h.log)))
	b = append(b, h.log...)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(kv)))
	b = append(b, kv...)
	b = append(b, topic...)
	return binary.LittleEndian.AppendUint32(b, format.CRC(b))
}

var errSnapshot = errors.New("not a valid v2 snapshot")

// decodeSnapshot checks a snapshot's framing and checksum and splits it.
// It never panics on any input (FuzzDecodeSnapshot).
func decodeSnapshot(b []byte) (h snapHeader, kv, topic []byte, err error) {
	const fixed = len(snapMagic) + 8 + 32 + 1
	if len(b) < fixed+8+4 || !bytes.Equal(b[:8], snapMagic[:]) {
		return h, nil, nil, errSnapshot
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if format.CRC(body) != sum {
		return h, nil, nil, fmt.Errorf("%w: checksum mismatch", errSnapshot)
	}
	h.lsn = binary.LittleEndian.Uint64(body[8:])
	copy(h.chain[:], body[16:48])
	idLen := int(body[48])
	rest := body[fixed:]
	if len(rest) < idLen+8 {
		return h, nil, nil, fmt.Errorf("%w: truncated header", errSnapshot)
	}
	h.log, rest = string(rest[:idLen]), rest[idLen:]
	n := binary.LittleEndian.Uint64(rest)
	rest = rest[8:]
	if n > uint64(len(rest)) {
		return h, nil, nil, fmt.Errorf("%w: kv length %d > %d", errSnapshot, n, len(rest))
	}
	return h, rest[:n], rest[n:], nil
}

func (db *DB) writeSnapshot(ctx context.Context, l *logState, v *view) error {
	v.mu.RLock()
	lsn, kv, topic := v.head, v.kv.Snapshot(), v.topic.Snapshot()
	v.mu.RUnlock()
	chain, err := l.chainAt(ctx, lsn)
	if err != nil {
		return err
	}
	key := snapKey(l.meta.ID, lsn)
	b := encodeSnapshot(snapHeader{log: l.meta.ID, lsn: lsn, chain: chain}, kv, topic)
	if err := putExactlyOnce(ctx, db.st, key, b); err != nil && !errors.Is(err, store.ErrExists) {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.commitManifest(ctx, func(m *Manifest) error {
		if lm := m.Logs[l.meta.ID]; lm != nil && lm.SnapLSN < lsn && (lm.Base == nil || lm.Base.LSN < lsn) {
			lm.SnapLSN, lm.SnapKey = lsn, key
		}
		return nil
	})
}

// loadSnapshot returns l's newest snapshot, or nil to replay the log.
func (db *DB) loadSnapshot(ctx context.Context, l *logState) *snapshot {
	lm := db.man.Logs[l.meta.ID]
	if lm == nil || lm.SnapKey == "" {
		return nil
	}
	snap, err := db.readSnapshot(ctx, l, lm.SnapKey)
	if err != nil {
		db.logf("freshdb: ignoring snapshot %s (the log is replayed instead): %v", lm.SnapKey, err)
		return nil
	}
	return snap
}

func (db *DB) readSnapshot(ctx context.Context, l *logState, key string) (*snapshot, error) {
	b, err := db.st.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	h, kvb, tpb, err := decodeSnapshot(b)
	if err != nil {
		return nil, err
	}
	if err := l.checkSnapshot(ctx, key, h); err != nil {
		return nil, err
	}
	kv, err := lens.LoadKV(kvb, db.opts.InlineMax)
	if err != nil {
		return nil, err
	}
	tp, err := lens.LoadTopic(tpb)
	if err != nil {
		return nil, err
	}
	return &snapshot{lsn: h.lsn, kv: kv, topic: tp, drop: db.keys.shreddedList()}, nil
}

// checkSnapshot verifies that a snapshot header belongs at key in l's
// history: same log and LSN as its key, inside the live range, and the
// same hash-chain head at that LSN.
func (l *logState) checkSnapshot(ctx context.Context, key string, h snapHeader) error {
	if key != snapKey(h.log, h.lsn) || h.log != l.meta.ID {
		return fmt.Errorf("%w: it holds log %s LSN %d", errSnapshot, h.log, h.lsn)
	}
	if h.lsn > l.head() || h.lsn < l.floor() {
		return fmt.Errorf("%w: LSN %d outside the log (%d..%d)", errSnapshot, h.lsn, l.floor(), l.head())
	}
	chain, err := l.chainAt(ctx, h.lsn)
	if err != nil {
		return err
	}
	if chain != h.chain {
		return fmt.Errorf("%w: hash chain at LSN %d differs from the log's", errSnapshot, h.lsn)
	}
	return nil
}
