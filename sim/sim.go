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
	"encoding/json"
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
	Seed             int64
	Steps            int
	Acked            int
	Ambiguous        int
	Crashes          int
	ANNRestores      int // opens that restored a saved HNSW graph
	ReadFaultCorrupt int // opens that said ErrCorrupt under read faults (then opened clean without them)
	CalmOpens        int // recoveries that needed the fault storm to pass (100 failed opens)
	Closes           int // clean closes (save the HNSW graph) instead of a crash
	Takeovers        int
	Shreds           int
	Rewinds          int
	Compacts         int
	GCs              int
	TopicTrims       int // checks that saw topic retention move the start
	Faults           int
	TraceHash        [32]byte
}

// document field literals and their numeric class (Find matches by value)
var simDocVals = []string{"1", "1.0", "10e-1", "2", "2e0", "3"}
var simDocClass = map[string]string{"1": "1", "1.0": "1", "10e-1": "1", "2": "2", "2e0": "2", "3": "3"}

type mop struct {
	kind    byte // 'p' put, 'd' del, 't' publish, 'v' vector put, 'x' vector del, 'D' doc put, 'Y' doc del
	key     string
	val     string
	subject string
}

type sim struct {
	ctx    context.Context
	rng    *rand.Rand
	seed   int64
	mem    *store.Mem
	st     *store.Faulty
	db     *freshdb.DB
	clock  atomic.Int64
	rnd    *lockedRand
	txs    map[int][]mop  // all issued txns
	acked  map[int]uint64 // tx id -> lsn
	amb    map[int]bool
	shred  map[string]bool // subject -> definitely shredded
	shredQ map[string]bool // shred outcome unknown
	// topicStart: the topic's retention start at the last check (it
	// never goes back); retained: a compaction with retention was tried.
	topicStart uint64
	retained   bool
	rewindQ    uint64 // ambiguous rewind target (+1), 0 = none
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
	st := s.st.Trace
	if len(st) > 15 {
		st = st[len(st)-15:]
	}
	return fmt.Errorf("seed %d step %d: %s\n  recent: %s\n  store: %s", s.seed, s.res.Steps, fmt.Sprintf(f, a...), strings.Join(tail, " | "), strings.Join(st, " | "))
}

func (s *sim) log(f string, a ...any) { s.trace = append(s.trace, fmt.Sprintf(f, a...)) }

func (s *sim) open() (*freshdb.DB, error) {
	var last error
	on, probs := s.st.On, s.st.P
	defer func() { s.st.On, s.st.P = on, probs }()
	for i := 0; i < 200; i++ {
		if i == 100 {
			// Open re-reads every object, so on a large store under a
			// read-fault storm it fails with probability ~1-(1-p)^objects:
			// a liveness limit of the harness, not a safety question. Let
			// the storm pass; the open itself is still checked.
			s.st.On = false
			s.log("storm passed after 100 failed opens")
			s.res.CalmOpens++
		}
		db, err := freshdb.OpenStore(s.ctx, s.st,
			freshdb.WithClock(func() int64 { return s.clock.Add(1) }),
			freshdb.WithRand(s.rnd), freshdb.WithRepairTail(true), freshdb.WithSnapshotEvery(1+int(s.seed%7)), freshdb.WithReadAhead(1),
			freshdb.WithLogf(func(f string, _ ...any) {
				if strings.Contains(f, "ANN graphs restored") {
					s.res.ANNRestores++
				}
			}),
			// even seeds keep every value as a log position (none inline),
			// and every third seed reads them without the value cache
			freshdb.WithInlineMax(int(s.seed%2)*64-1), freshdb.WithValueCacheBytes(map[bool]int{true: -1}[s.seed%3 == 0]))
		if err == nil {
			return db, nil
		}
		if errors.Is(err, freshdb.ErrCorrupt) {
			p := &s.st.P
			if !s.st.On || p.BitFlip+p.Misdirect+p.ShortRead+p.StaleList == 0 {
				return nil, err
			}
			// Oracle for read-path faults: ErrCorrupt or the right answer,
			// never a wrong one. The faults are transient, so with them off
			// the same store must open cleanly and pass every check.
			s.log("corrupt under read faults: %v", err)
			s.res.ReadFaultCorrupt++
			p.BitFlip, p.Misdirect, p.ShortRead, p.StaleList = 0, 0, 0, 0
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
	s.st.On = false
	if err := s.db.EnableANN(s.ctx, "c", freshdb.ANNOptions{M: 2 + int(seed%4), EfConstruction: 4}); err != nil {
		return s.res, s.fail("enable ann: %v", err)
	}
	// odd seeds index the documents' field, even seeds scan: Find must
	// match the model either way
	if seed%2 == 1 {
		if err := s.db.RegisterIndex(s.ctx, "docs", "f"); err != nil {
			return s.res, s.fail("register index: %v", err)
		}
	}
	s.st.On = true
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
		switch r := s.rng.Intn(17); {
		case r < 6:
			m.kind = 'p'
		case r < 8:
			m.kind, m.val, m.subject = 'd', "", ""
		case r < 10:
			m.kind = 't'
		case r < 13:
			m.kind, m.key = 'v', "e"+strconv.Itoa(s.rng.Intn(40))
		case r < 14:
			m.kind, m.key, m.val, m.subject = 'x', "e"+strconv.Itoa(s.rng.Intn(40)), "", ""
		case r < 16: // field f: equal numbers in different literals
			m.kind, m.key, m.val = 'D', "d"+strconv.Itoa(s.rng.Intn(6)), simDocVals[s.rng.Intn(len(simDocVals))]
		default:
			m.kind, m.key, m.val, m.subject = 'Y', "d"+strconv.Itoa(s.rng.Intn(6)), "", ""
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
			case 'v':
				err = tx.Vector("c").Put(m.key, simVec(m.val), o...)
			case 'x':
				err = tx.Vector("c").Delete(m.key)
			case 'D':
				err = tx.Doc("docs").Put(m.key, json.RawMessage(`{"f":`+m.val+`}`), o...)
			case 'Y':
				err = tx.Doc("docs").Delete(m.key)
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
	if s.rng.Intn(3) == 0 {
		s.db.Close() // clean: saves the HNSW graph (through the faulty store)
		s.res.Closes++
	} else {
		s.db.Crash()
	}
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
		var o freshdb.CompactOptions
		if s.rng.Intn(2) == 0 { // topic retention (no groups in the sim)
			o.Topics.MaxMessages = uint64(s.rng.Intn(8))
			s.retained = s.retained || o.Topics.MaxMessages > 0
		}
		_, err = s.db.Compact(s.ctx, o)
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
	vec := map[string]string{}
	docs := map[string]string{} // id -> canonical class of f
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
			case 'v':
				if gone {
					delete(vec, m.key)
				} else {
					vec[m.key] = m.val
				}
			case 'x':
				delete(vec, m.key)
			case 'D':
				if gone {
					delete(docs, m.key)
				} else {
					docs[m.key] = simDocClass[m.val]
				}
			case 'Y':
				delete(docs, m.key)
			}
		}
	}
	got := map[string]string{}
	gotVec := map[string]string{}
	gotDocs := map[string]string{}
	var gotTopic []string
	var start, n uint64
	err = s.db.View(s.ctx, func(tx *freshdb.Tx) error {
		kvs, err := tx.Scan("")
		if err != nil {
			return err
		}
		for _, p := range kvs {
			got[p.Key] = string(p.Value)
		}
		hs, err := tx.Vector("c").SearchExact([]float32{1, 0, 0}, 1000)
		if err != nil {
			return err
		}
		for _, h := range hs {
			gotVec[h.ID] = ""
		}
		for _, class := range []string{"1", "2", "3"} {
			ids, err := tx.Doc("docs").Find("f", json.Number(class))
			if err != nil {
				return err
			}
			for _, id := range ids {
				if prev, dup := gotDocs[id]; dup {
					return fmt.Errorf("doc %s found by f=%s and f=%s", id, prev, class)
				}
				gotDocs[id] = class
			}
		}
		start, n = tx.Topic("t").Start(), tx.Topic("t").Len()
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
	for k := range vec {
		vec[k] = ""
	}
	if !mapEq(vec, gotVec) {
		return s.fail("vector lens != model fold\n  want %v\n  got  %v", sorted(vec), sorted(gotVec))
	}
	if !mapEq(docs, gotDocs) {
		return s.fail("document Find != model fold (indexed %v)\n  want %v\n  got  %v", s.seed%2 == 1, sorted(docs), sorted(gotDocs))
	}
	if err := s.db.CheckANN("main"); err != nil {
		return s.fail("%v", err)
	}
	// retention may have removed a prefix: never past the end, never
	// back, never without a retention compaction; offsets unchanged.
	if n != uint64(len(topic)) || start > n || start < s.topicStart || (start > 0 && !s.retained) {
		return s.fail("topic start %d len %d: want len %d, start in [%d, len] (retention used: %v)", start, n, len(topic), s.topicStart, s.retained)
	}
	if start > s.topicStart {
		s.res.TopicTrims++
	}
	s.topicStart = start
	if strings.Join(topic[start:], ",") != strings.Join(gotTopic, ",") {
		return s.fail("topic lens != model fold from %d\n  want %v\n  got  %v", start, topic[start:], gotTopic)
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

// simVec derives a 3-d vector from a value string.
func simVec(v string) []float32 {
	h := sha256.Sum256([]byte(v))
	return []float32{float32(int8(h[0])) + 0.5, float32(int8(h[1])), float32(int8(h[2]))}
}
