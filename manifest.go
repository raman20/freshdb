package freshdb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/lens"
	"github.com/raman20/freshdb/store"
)

// Manifest is a full snapshot of database metadata. Manifests are numbered
// densely (manifest/<id>.mf) and created with PutIfAbsent, so exactly one
// writer can create each id: that is the fencing token for metadata.
type Manifest struct {
	Format  int                 `json:"format"`
	ID      uint64              `json:"id"`
	Epoch   uint64              `json:"epoch"` // writer epoch; bumped on every open-for-write
	DBID    string              `json:"db_id"`
	NextLog uint64              `json:"next_log"`
	Logs    map[string]*LogMeta `json:"logs"`
	Refs    map[string]string   `json:"refs"` // branch name -> log id
	Reflog  []RefChange         `json:"reflog,omitempty"`
	Indexes []IndexDecl         `json:"indexes,omitempty"`
	ANN     []ANNDecl           `json:"ann,omitempty"`     // vector collections with an HNSW graph
	Garbage []Garbage           `json:"garbage,omitempty"` // objects to delete once due (compaction)
}

// Garbage is storage that no manifest from this one on refers to. It is
// deleted by a sweep once the clock passes After (the grace period that
// lets readers of older manifests finish).
type Garbage struct {
	Kind   string `json:"kind"` // "segs": log/<Log>/ seq < Below; "snaps": snap/<Log>/ lsn < Below; "base": Key + Chunks
	Log    string `json:"log,omitempty"`
	Below  uint64 `json:"below,omitempty"`
	Key    string `json:"key,omitempty"`
	Chunks int    `json:"chunks,omitempty"`
	After  int64  `json:"after"`
}

// BaseMeta locates a log's compaction base: the full state at LSN, in
// object Key plus Chunks value chunks. Segments before Seq are covered.
type BaseMeta struct {
	LSN    uint64 `json:"lsn"`
	Seq    uint64 `json:"seq"`
	Key    string `json:"key"`
	Chunks int    `json:"chunks"`
	Chain  string `json:"chain"` // hex hash-chain head at LSN
	TS     int64  `json:"ts"`
}

// LogMeta describes one log. A branch is a log whose history is its
// parent's log up to ParentLSN: branching copies no data.
type LogMeta struct {
	ID        string `json:"id"`
	Parent    string `json:"parent,omitempty"`
	ParentLSN uint64 `json:"parent_lsn,omitempty"`
	SnapLSN   uint64 `json:"snap_lsn,omitempty"`
	SnapKey   string `json:"snap_key,omitempty"`
	// Base, if set, holds the state at Base.LSN: the log no longer needs
	// its parent or its segments before Base.Seq (see compact.go).
	Base *BaseMeta `json:"base,omitempty"`
}

type RefChange struct {
	Name string `json:"name"`
	From string `json:"from,omitempty"`
	To   string `json:"to"`
	At   uint64 `json:"at"`
	Why  string `json:"why"`
	TS   int64  `json:"ts,omitempty"` // unix nanoseconds (0 in manifests written before GC existed)
}

// ANNDecl opts a vector collection into an HNSW graph (EnableANN).
type ANNDecl struct {
	Coll string `json:"coll"`
	ANNOptions
}

type IndexDecl struct {
	Coll  string `json:"coll"`
	Field string `json:"field"`
}

// manifestFormat is what a new database starts with; compaction writes
// manifestFormatBase (bases and garbage), which older binaries refuse
// instead of misreading a log whose first segments are gone.
const (
	manifestFormat     = 1
	manifestFormatBase = 2
)

// Formats describes the on-storage format versions this build reads and
// writes (`freshdb version`; docs/compatibility.md).
func Formats() string {
	return fmt.Sprintf("manifest %d-%d, segment %d, base %s, journal %d, snapshot 2 (KV2), checkpoint 1, ann %s",
		manifestFormat, manifestFormatBase, format.SegVersion, baseMagic[7:], store.JournalVersion, lens.ANNVersion)
}

func manifestKey(id uint64) string { return fmt.Sprintf("manifest/%020d.mf", id) }

func (m *Manifest) clone() *Manifest {
	b, _ := json.Marshal(m)
	var c Manifest
	json.Unmarshal(b, &c)
	if c.Logs == nil {
		c.Logs = map[string]*LogMeta{}
	}
	if c.Refs == nil {
		c.Refs = map[string]string{}
	}
	return &c
}

func encodeManifest(m *Manifest) []byte {
	b, _ := json.Marshal(m)
	return binary.LittleEndian.AppendUint32(b, format.CRC(b))
}

func decodeManifest(b []byte) (*Manifest, error) {
	if len(b) < 4 || binary.LittleEndian.Uint32(b[len(b)-4:]) != format.CRC(b[:len(b)-4]) {
		return nil, fmt.Errorf("%w: manifest checksum", ErrCorrupt)
	}
	var m Manifest
	if err := json.Unmarshal(b[:len(b)-4], &m); err != nil {
		return nil, fmt.Errorf("%w: manifest: %v", ErrCorrupt, err)
	}
	if m.Format != manifestFormat && m.Format != manifestFormatBase {
		return nil, fmt.Errorf("%w: manifest format %d (this build reads %d and %d; written by a newer freshdb? upgrade the binary)",
			ErrVersion, m.Format, manifestFormat, manifestFormatBase)
	}
	c := m.clone2()
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("%w: manifest %d: %v", ErrCorrupt, c.ID, err)
	}
	return &c, nil
}

// validate checks the structure the rest of the code relies on, so a
// checksummed but inconsistent manifest (a bug, or a hand edit) is
// refused instead of panicking or recursing forever. FuzzOpenManifest.
func (m *Manifest) validate() error {
	for id, lm := range m.Logs {
		switch {
		case lm == nil:
			return fmt.Errorf("log %q: null", id)
		case id == "" || lm.ID != id:
			return fmt.Errorf("log %q: id %q", id, lm.ID)
		case lm.Base != nil && lm.Base.Chunks < 0:
			return fmt.Errorf("log %q: base chunks %d", id, lm.Base.Chunks)
		}
		// The parent chain must end (loadLog walks it recursively, stopping
		// at a compaction base: a based log's parent may be gone).
		p, hops := lm, 0
		for p.Base == nil && p.Parent != "" {
			if hops++; hops > len(m.Logs) {
				return fmt.Errorf("log %q: parent cycle", id)
			}
			p = m.Logs[p.Parent]
			if p == nil {
				return fmt.Errorf("log %q: unknown ancestor", id)
			}
		}
	}
	for name, id := range m.Refs {
		if name == "" || m.Logs[id] == nil {
			return fmt.Errorf("ref %q: unknown log %q", name, id)
		}
	}
	return nil
}

func (m Manifest) clone2() Manifest {
	if m.Logs == nil {
		m.Logs = map[string]*LogMeta{}
	}
	if m.Refs == nil {
		m.Refs = map[string]string{}
	}
	return m
}

func parseNum(key, prefix, suffix string) (uint64, bool) {
	s := strings.TrimSuffix(strings.TrimPrefix(key, prefix), suffix)
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil
}

// loadLatestManifest returns the highest-numbered manifest, or nil.
func loadLatestManifest(ctx context.Context, st store.Store) (*Manifest, error) {
	keys, err := st.List(ctx, "manifest/")
	if err != nil {
		return nil, err
	}
	var best uint64
	for _, k := range keys {
		if n, ok := parseNum(k, "manifest/", ".mf"); ok && n > best {
			best = n
		}
	}
	if best == 0 {
		return nil, nil
	}
	var m *Manifest
	var derr error
	if _, err := getStable(ctx, st, manifestKey(best), func(b []byte) readVerdict {
		if m, derr = decodeManifest(b); derr != nil {
			return readDamaged
		}
		if m.ID != best {
			return readMisread
		}
		return readOK
	}); err != nil {
		return nil, err
	}
	if derr != nil {
		return nil, derr
	}
	if m.ID != best { // every read returned the same other manifest
		return nil, fmt.Errorf("%w: manifest %d holds id %d", ErrCorrupt, best, m.ID)
	}
	return m, nil
}

// putManifest creates manifest m.ID. ErrExists means we lost the race.
//
// The create-only put alone fences a stale writer only while every older
// manifest slot stays occupied. GC deletes old manifests, so a writer
// holding manifest k could create a deleted slot k+1 below the latest.
// So after the put we list manifest/: a higher id means we lost (our
// manifest is a stray no reader loads, since readers take the highest
// id; we delete it). GC never deletes the newest manifests, so a free
// slot below the latest always comes with a higher id
// (spec/FreshGC.tla, NoZombie; the mutant without this check loses data).
//
// That higher id may also be a newer opener that read OUR manifest and
// carried its change forward, so a loss found after our put landed wraps
// both store.ErrExists (we are fenced) and ErrUnknownOutcome (the change
// may be committed); so does a failed List after the put.
func putManifest(ctx context.Context, st store.Store, m *Manifest) error {
	if err := putExactlyOnce(ctx, st, manifestKey(m.ID), encodeManifest(m)); err != nil {
		return err
	}
	keys, err := st.List(ctx, "manifest/")
	if err != nil {
		return fmt.Errorf("%w: manifest %d created, check for a newer one failed: %w", ErrUnknownOutcome, m.ID, err)
	}
	for _, k := range keys {
		if n, ok := parseNum(k, "manifest/", ".mf"); ok && n > m.ID {
			_ = st.Delete(ctx, manifestKey(m.ID))
			return fmt.Errorf("%w: manifest %d created, then found below %d: %w", store.ErrExists, m.ID, n, ErrUnknownOutcome)
		}
	}
	return nil
}

// putExactlyOnce creates key with data, retrying transient errors. Because
// the write is create-only and the bytes are fixed, retries are idempotent:
// ErrExists followed by a byte-identical read-back means an earlier attempt
// (whose response was lost) succeeded.
//
// Result: nil = key holds data; store.ErrExists = key holds other bytes
// (proven by a read, so "not ours" is certain); anything else wraps
// ErrUnknownOutcome and never store.ErrExists, because callers read
// ErrExists as "definitely not committed".
func putExactlyOnce(ctx context.Context, st store.Store, key string, data []byte) error {
	var err error
	for attempt := 0; attempt < 4 && ctx.Err() == nil; attempt++ {
		if err = st.PutIfAbsent(ctx, key, data); err == nil {
			return nil
		}
		if !errors.Is(err, store.ErrExists) {
			continue
		}
		var mine bool
		if mine, err = readBackIs(ctx, st, key, data); err == nil {
			if mine {
				return nil
			}
			return store.ErrExists
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	return fmt.Errorf("%w: %w", ErrUnknownOutcome, err)
}

// readBackIs reports whether key holds exactly data, retrying failed reads.
// "Other bytes" must be read alike several times (getStable): one faulty read must
// not turn our own landed write into "fenced". A NotFound after the store
// said "exists" cannot happen on a correct store; it is returned as an
// error so the caller retries the put.
func readBackIs(ctx context.Context, st store.Store, key string, data []byte) (bool, error) {
	mine := func(b []byte) readVerdict { return okIf(string(b) == string(data)) }
	var err error
	for r := 0; r < 4 && ctx.Err() == nil; r++ {
		var b []byte
		if b, err = getStable(ctx, st, key, mine); err == nil {
			return string(b) == string(data), nil
		}
		if errors.Is(err, store.ErrNotFound) {
			return false, err
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	return false, err
}
