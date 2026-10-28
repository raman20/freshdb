// Package sim is a deterministic simulation harness for freshdb.
//
// One seed fully determines a run: the workload, the fake clock, every
// random byte the engine consumes (keys, nonces), and every injected fault
// (failed / lost / delayed+reordered / duplicated responses, torn writes,
// crashes, a second writer taking over). The engine is driven by a single
// client goroutine and the writer acks only after all of its store calls,
// so the sequence of store calls — and therefore the run — is a pure
// function of the seed. Run reports a trace hash so determinism itself is
// checked (same seed twice ⇒ identical trace).
//
// The reference model is a plain sequence of transaction ids. After every
// recovery the harness checks refinement against it:
//
//	acked ⊆ recovered ⊆ acked ∪ ambiguous      (durability, no phantoms)
//	recovered is strictly increasing           (no reordering, no double-apply)
//	lenses == fold(model ops of recovered)     (KV + topic state, shred-redacted)
//	lens digests == full replay digests        (lens = pure fold over log)
//	a fenced (zombie) writer never commits     (no split brain)
//	shredded subjects are never readable       (crypto-shred)
//	Verify(): hash chains + manifest history OK
package sim

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/raman20/freshdb"
	"github.com/raman20/freshdb/format"
	"github.com/raman20/freshdb/store"
)

type Config struct {
	Steps int
	Probs store.FaultProbs
}

type Result struct {
	Seed      int64
	Steps     int
	Acked     int
	Ambiguous int
	Crashes   int
	Takeovers int
	Shreds    int
	Rewinds   int
	Compacts  int
	GCs       int
	Faults    int
	TraceHash [32]byte
}

type mop struct {
	kind    byte // 'p' put, 'd' del, 't' publish
	key     string
	val     string
	subject string
}

type sim struct {
	ctx     context.Context
	rng     *rand.Rand
	seed    int64
	mem     *store.Mem
	st      *store.Faulty
	db      *freshdb.DB
	clock   atomic.Int64
	rnd     *lockedRand
	txs     map[int][]mop  // all issued txns
	acked   map[int]uint64 // tx id -> lsn
	amb     map[int]bool
	shred   map[string]bool // subject -> definitely shredded
	shredQ  map[string]bool // shred outcome unknown
	rewindQ uint64          // ambiguous rewind target (+1), 0 = none
	// prefix: tx ids (and LSNs) in each compaction base, by "log@LSN";
	// the log only has entries above the base
	prefix map[string]txPrefix
	nextID int
	lsns   []uint64 // LSNs of the ids last returned by recovered
	res    Result
	trace  []string
}

type txPrefix struct {
	ids  []int
	lsns []uint64
}

type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func (l *lockedRand) Read(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Read(p)
}

func (s *sim) fail(f string, a ...any) error {
	tail := s.trace
	if len(tail) > 25 {
		tail = tail[len(tail)-25:]
	}
	return fmt.Errorf("seed %d step %d: %s\n  recent: %s", s.seed, s.res.Steps, fmt.Sprintf(f, a...), strings.Join(tail, " | "))
}

func (s *sim) log(f string, a ...any) { s.trace = append(s.trace, fmt.Sprintf(f, a...)) }

func (s *sim) open() (*freshdb.DB, error) {
	var last error
	for i := 0; i < 200; i++ {
		db, err := freshdb.OpenStore(s.ctx, s.st,
			freshdb.WithClock(func() int64 { return s.clock.Add(1) }),
			freshdb.WithRand(s.rnd), freshdb.WithSnapshotEvery(1+int(s.seed%7)),
			freshdb.WithLogf(func(string, ...any) {}),
			// even seeds keep every value as a log position (none inline),
			// and every third seed reads them without the value cache
			freshdb.WithInlineMax(int(s.seed%2)*64-1), freshdb.WithValueCacheBytes(map[bool]int{true: -1}[s.seed%3 == 0]))
		if err == nil {
			return db, nil
		}
		if errors.Is(err, freshdb.ErrCorrupt) {
			return nil, err
		}
		last = err
	}
	return nil, fmt.Errorf("could not open after 200 attempts: %v", last)
}

// Run executes one seeded simulation.
func Run(seed int64, cfg Config) (Result, error) {
	s := &sim{ctx: context.Background(), rng: rand.New(rand.NewSource(seed)), seed: seed,
		txs: map[int][]mop{}, acked: map[int]uint64{}, amb: map[int]bool{},
		shred: map[string]bool{}, shredQ: map[string]bool{}, prefix: map[string]txPrefix{}}
	s.res.Seed = seed
	s.rnd = &lockedRand{r: rand.New(rand.NewSource(seed ^ 0x5eed))}
	s.mem = store.NewMem()
	s.st = store.NewFaulty(s.mem, seed*7919+1, cfg.Probs)
	var err error
	if s.db, err = s.open(); err != nil {
		return s.res, s.fail("initial open: %v", err)
	}
	for s.res.Steps = 0; s.res.Steps < cfg.Steps; s.res.Steps++ {
		var err error
		switch r := s.rng.Intn(100); {
		case r < 66:
			err = s.txn()
		case r < 70:
			err = s.compact()
		case r < 80:
			err = s.crash(true)
		case r < 85:
			err = s.takeover()
		case r < 90:
			err = s.doShred()
		case r < 95:
			err = s.rewind()
		default:
			err = s.check()
		}
		if err != nil {
			return s.res, err
		}
	}
	if err := s.crash(false); err != nil {
		return s.res, err
	}
	s.db.Close()
	h := sha256.New()
	for _, t := range s.trace {
		h.Write([]byte(t))
		h.Write([]byte{0})
	}
	for _, t := range s.st.Trace {
		h.Write([]byte(t))
		h.Write([]byte{0})
	}
	copy(s.res.TraceHash[:], h.Sum(nil))
	s.res.Acked, s.res.Ambiguous = len(s.acked), len(s.amb)
	for _, t := range s.st.Trace {
		if !strings.HasPrefix(t, "put ") {
			s.res.Faults++
		}
	}
	return s.res, nil
}

var subjects = []string{"", "", "alice", "bob"}

func (s *sim) txn() error {
	id := s.nextID
	s.nextID++
	n := 1 + s.rng.Intn(3)
	ops := []mop{}
	for i := 0; i < n; i++ {
		m := mop{key: "k" + strconv.Itoa(s.rng.Intn(6)), val: fmt.Sprintf("v%d.%d", id, i), subject: subjects[s.rng.Intn(4)]}
		switch r := s.rng.Intn(10); {
		case r < 6:
			m.kind = 'p'
		case r < 8:
			m.kind, m.val, m.subject = 'd', "", ""
		default:
			m.kind = 't'
		}
		ops = append(ops, m)
	}
	s.txs[id] = ops
	lsn, err := s.db.Branch("main").UpdateLSN(s.ctx, func(tx *freshdb.Tx) error {
		tx.Put("__tx", []byte(strconv.Itoa(id)))
		for _, m := range ops {
			var o []freshdb.WriteOpt
			if m.subject != "" {
				o = append(o, freshdb.Subject(m.subject))
			}
			var err error
			switch m.kind {
			case 'p':
				err = tx.Put(m.key, []byte(m.val), o...)
			case 'd':
				err = tx.Delete(m.key)
			case 't':
				err = tx.Topic("t").Publish([]byte(m.val), o...)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	s.log("tx%d%v=%v", id, ops, err)
	switch {
	case err == nil:
		s.acked[id] = lsn
		return nil
	case errors.Is(err, freshdb.ErrShredded):
		for _, m := range ops {
			if m.subject != "" && (s.shred[m.subject] || s.shredQ[m.subject]) {
				return nil
			}
		}
		return s.fail("tx%d: ErrShredded but no shredded subject", id)
	case errors.Is(err, freshdb.ErrConflict):
		return s.fail("tx%d: conflict with a single client", id)
	default:
		s.amb[id] = true
		if errors.Is(err, freshdb.ErrUnknownOutcome) || errors.Is(err, freshdb.ErrFenced) {
			return s.crash(true) // writer is poisoned; recover
		}
		return nil
	}
}

// crash kills the process (no clean close) and recovers.
func (s *sim) crash(faulty bool) error {
	s.res.Crashes++
	s.db.Crash()
	if s.rng.Intn(2) == 0 || !faulty {
		s.st.Flush(s.ctx) // in-flight requests may still land after a crash
	}
	s.st.On = faulty
	db, err := s.open()
	s.st.On = true
	if err != nil {
		return s.fail("recovery: %v", err)
	}
	s.db = db
	s.log("recovered epoch=%d", db.Epoch())
	return s.check()
}

// takeover: a second writer opens the same bucket while the first is alive.
func (s *sim) takeover() error {
	s.res.Takeovers++
	old := s.db
	nw, err := s.open()
	if err != nil {
		return s.fail("takeover open: %v", err)
	}
	s.log("takeover epoch %d->%d", old.Epoch(), nw.Epoch())
	id := s.nextID
	s.nextID++
	s.txs[id] = nil
	err = old.Branch("main").Update(s.ctx, func(tx *freshdb.Tx) error {
		return tx.Put("__tx", []byte(strconv.Itoa(id)))
	})
	if err == nil {
		return s.fail("SPLIT BRAIN: zombie epoch %d committed after epoch %d opened", old.Epoch(), nw.Epoch())
	}
	old.Crash()
	s.db = nw
	return s.check()
}

func (s *sim) doShred() error {
	subj := subjects[2+s.rng.Intn(2)]
	_, err := s.db.Shred(s.ctx, subj)
	s.log("shred %s=%v", subj, err)
	if err == nil {
		s.res.Shreds++
		s.shred[subj] = true
		return s.check()
	}
	s.shredQ[subj] = true
	return s.crash(true)
}

func (s *sim) rewind() error {
	es, err := s.db.Entries("main")
	if err != nil || len(es) == 0 {
		return nil
	}
	at := uint64(s.rng.Intn(len(es) + 1))
	if at > 0 {
		at = es[at-1].LSN
	}
	err = s.db.Rewind(s.ctx, "main", at)
	s.log("rewind %d=%v", at, err)
	if errors.Is(err, freshdb.ErrCompacted) {
		return nil // below the base: refused, nothing changed
	}
	if err == nil {
		s.res.Rewinds++
		s.truncate(at)
		return s.check()
	}
	s.rewindQ = at + 1
	return s.crash(true)
}

func (s *sim) truncate(at uint64) {
	for id, l := range s.acked {
		if l > at {
			delete(s.acked, id)
		}
	}
}

// recovered returns the tx ids present on main, in log order.
func (s *sim) recovered() ([]int, error) {
	es, err := s.db.Entries("main")
	if err != nil {
		return nil, err
	}
	var ids []int
	var lsns []uint64
	if key := s.floorKey(); key != "" {
		p, ok := s.prefix[key]
		if !ok {
			return nil, fmt.Errorf("main starts from base %s that no compaction made", key)
		}
		ids, lsns = append(ids, p.ids...), append(lsns, p.lsns...)
	}
	for _, e := range es {
		for _, op := range e.Ops {
			if op.Kind == format.OpPut && op.Space == "kv" && op.Key == "__tx" {
				id, _ := strconv.Atoi(string(op.Value))
				ids = append(ids, id)
				lsns = append(lsns, e.LSN)
			}
		}
	}
	// resolve an ambiguous rewind: all acked txns past the target vanish, or none
	if s.rewindQ > 0 {
		at := s.rewindQ - 1
		s.rewindQ = 0
		present := map[int]bool{}
		for _, id := range ids {
			present[id] = true
		}
		var past, gone int
		for id, l := range s.acked {
			if l > at {
				past++
				if !present[id] {
					gone++
				}
			}
		}
		if gone != 0 && gone != past {
			return nil, fmt.Errorf("ambiguous rewind to %d half-applied: %d of %d gone", at, gone, past)
		}
		if gone > 0 {
			s.truncate(at)
		}
	}
	s.lsns = lsns
	return ids, nil
}

// floorKey names main's compaction base ("" if none).
func (s *sim) floorKey() string {
	m := s.db.Manifest()
	for lm := m.Logs[m.Refs["main"]]; lm != nil; lm = m.Logs[lm.Parent] {
		if lm.Base != nil {
			return fmt.Sprintf("%s@%d", lm.ID, lm.Base.LSN)
		}
		if lm.Parent == "" {
			break
		}
	}
	return ""
}

// compact records which txns are at main's head, then compacts. A
// failed compaction may still have committed: recover and check.
func (s *sim) compact() error {
	if err := s.check(); err != nil {
		return err
	}
	s.st.On = false // bookkeeping read, not under test
	head, err := s.db.Branch("main").Head()
	if err != nil {
		return s.fail("%v", err)
	}
	ids, err := s.recovered()
	s.st.On = true
	if err != nil {
		return s.fail("%v", err)
	}
	m := s.db.Manifest()
	s.prefix[fmt.Sprintf("%s@%d", m.Refs["main"], head)] = txPrefix{ids, s.lsns}
	op := "compact"
	if s.rng.Intn(2) == 0 {
		// GC at the head: same floor, plus reflog, retired-log and
		// old-manifest deletion under crash injection.
		op = "gc"
		_, err = s.db.GC(s.ctx, freshdb.GCOptions{KeepReflog: 1, KeepManifests: 1 + s.rng.Intn(3)})
	} else {
		_, err = s.db.Compact(s.ctx, freshdb.CompactOptions{})
	}
	s.log("%s @%d=%v", op, head, err)
	if err == nil {
		s.res.Compacts++
		if op == "gc" {
			s.res.GCs++
		}
		return s.check()
	}
	return s.crash(true)
}

func (s *sim) check() error {
	s.st.On = false
	defer func() { s.st.On = true }()
	ids, err := s.recovered()
	if err != nil {
		return s.fail("%v", err)
	}
	present := map[int]bool{}
	for i, id := range ids {
		if i > 0 && id <= ids[i-1] {
			return s.fail("order/double-apply violation: tx%d after tx%d", id, ids[i-1])
		}
		present[id] = true
		if _, ok := s.acked[id]; !ok && !s.amb[id] {
			return s.fail("phantom tx%d (never acked nor in flight)", id)
		}
		if s.txs[id] == nil && s.amb[id] {
			// zombie probe transactions must never be present
			return s.fail("zombie write tx%d is durable", id)
		}
	}
	for id := range s.acked {
		if !present[id] {
			return s.fail("LOST acknowledged tx%d (lsn %d)", id, s.acked[id])
		}
	}
	// shred status: resolve ambiguous shreds from durable state
	for subj := range s.shredQ {
		if s.db.IsShredded(subj) {
			s.shred[subj] = true
		}
	}
	// expected state = fold of model ops over recovered ids
	kv := map[string]string{}
	var topic []string
	for _, id := range ids {
		kv["__tx"] = strconv.Itoa(id)
		for _, m := range s.txs[id] {
			gone := m.subject != "" && s.shred[m.subject]
			switch m.kind {
			case 'p':
				if gone {
					delete(kv, m.key)
				} else {
					kv[m.key] = m.val
				}
			case 'd':
				delete(kv, m.key)
			case 't':
				if gone {
					topic = append(topic, "<redacted>")
				} else {
					topic = append(topic, m.val)
				}
			}
		}
	}
	got := map[string]string{}
	var gotTopic []string
	err = s.db.View(s.ctx, func(tx *freshdb.Tx) error {
		kvs, err := tx.Scan("")
		if err != nil {
			return err
		}
		for _, p := range kvs {
			got[p.Key] = string(p.Value)
		}
		ms, err := tx.Topic("t").Read(0, 0)
		for _, m := range ms {
			if m.Redacted {
				gotTopic = append(gotTopic, "<redacted>")
			} else {
				gotTopic = append(gotTopic, string(m.Value))
			}
		}
		return err
	})
	if err != nil {
		return s.fail("view: %v", err)
	}
	if !mapEq(kv, got) {
		return s.fail("KV lens != model fold\n  want %v\n  got  %v", sorted(kv), sorted(got))
	}
	if strings.Join(topic, ",") != strings.Join(gotTopic, ",") {
		return s.fail("topic lens != model fold\n  want %v\n  got  %v", topic, gotTopic)
	}
	d1, _, err := s.db.LensDigests("main")
	if err != nil {
		return s.fail("%v", err)
	}
	d2, err := s.db.RebuildDigests("main")
	if err != nil {
		return s.fail("%v", err)
	}
	if fmt.Sprint(d1) != fmt.Sprint(d2) {
		return s.fail("lens digests != fold(log) digests")
	}
	r, err := s.db.Verify(s.ctx)
	if err != nil || !r.OK {
		return s.fail("verify: %+v %v", r, err)
	}
	// a durable shred leaves no key material behind (after the recovery pass)
	s.log("check ok ids=%d", len(ids))
	return nil
}

func mapEq(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func sorted(m map[string]string) []string {
	var out []string
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}
