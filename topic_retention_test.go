package freshdb

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/freshdb/store"
)

// topicState reads start, len and every kept message of topic.
func topicState(t *testing.T, db *DB, topic string) (start, n uint64, ms []Message) {
	t.Helper()
	err := db.View(ctx, func(tx *Tx) (err error) {
		tt := tx.Topic(topic)
		start, n = tt.Start(), tt.Len()
		ms, err = tt.Read(0, 0)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return
}

func wantTopic(t *testing.T, db *DB, topic string, start, n uint64) {
	t.Helper()
	s, l, ms := topicState(t, db, topic)
	if s != start || l != n || uint64(len(ms)) != n-start {
		t.Fatalf("start %d len %d kept %d, want %d %d %d", s, l, len(ms), start, n, n-start)
	}
	for i, m := range ms {
		if m.Offset != start+uint64(i) || (!m.Redacted && m.Value[0] != byte(m.Offset)) {
			t.Fatalf("message %d: %+v", i, m)
		}
	}
}

func TestTopicRetentionMaxMessages(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	pub(t, db, "t", 10)
	pub(t, db, "u", 2)
	r := CompactOptions{Topics: TopicRetention{MaxMessages: 3}}
	s, err := db.Compact(ctx, r)
	if err != nil || s.Trimmed != 7 || s.Messages != 5 {
		t.Fatalf("%+v %v", s, err)
	}
	wantTopic(t, db, "t", 7, 10)
	wantTopic(t, db, "u", 0, 2)
	// offsets continue; the trim survives reopening
	pub(t, db, "t", 1)
	db.Close()
	db = mustOpen(t, st)
	defer db.Close()
	wantTopic(t, db, "t", 7, 11)
	// 11 messages from 7: the cut moves to 8
	if s, err = db.Compact(ctx, r); err != nil || s.Trimmed != 1 || s.Messages != 5 {
		t.Fatalf("%+v %v", s, err)
	}
	wantTopic(t, db, "t", 8, 11)
	// nothing new and nothing to cut: no new base
	if s, err = db.Compact(ctx, r); err != nil || s.Logs != 0 || s.Trimmed != 0 {
		t.Fatalf("%+v %v", s, err)
	}
	// without retention the trim stays
	if s, err = db.Compact(ctx, CompactOptions{}); err != nil || s.Logs != 0 {
		t.Fatalf("%+v %v", s, err)
	}
	wantTopic(t, db, "t", 8, 11)
}

func ack(t *testing.T, db *DB, topic, group string, n int) {
	t.Helper()
	err := db.Update(ctx, func(tx *Tx) error {
		g := tx.Topic(topic).Group(group)
		ms, err := g.Fetch(n)
		if err != nil || len(ms) == 0 {
			return err
		}
		return g.Ack(ms[len(ms)-1])
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestTopicRetentionGroups: the cut stops at the slowest group's cursor
// unless IgnoreGroups; a group behind the cut then resumes at the cut.
func TestTopicRetentionGroups(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	pub(t, db, "t", 10)
	pub(t, db, "other", 10)
	ack(t, db, "t", "slow", 4)  // cursor 4
	ack(t, db, "t", "fast", 9)  // cursor 9
	ack(t, db, "other", "x", 1) // another topic: ignored
	r := TopicRetention{MaxMessages: 2}
	if _, err := db.Compact(ctx, CompactOptions{Topics: r}); err != nil {
		t.Fatal(err)
	}
	wantTopic(t, db, "t", 4, 10)
	wantTopic(t, db, "other", 1, 10)
	ack(t, db, "t", "slow", 2) // cursor 6
	if _, err := db.Compact(ctx, CompactOptions{Topics: r}); err != nil {
		t.Fatal(err)
	}
	wantTopic(t, db, "t", 6, 10)
	r.IgnoreGroups = true
	if _, err := db.Compact(ctx, CompactOptions{Topics: r}); err != nil {
		t.Fatal(err)
	}
	wantTopic(t, db, "t", 8, 10)
	var first uint64
	db.Update(ctx, func(tx *Tx) error {
		ms, err := tx.Topic("t").Group("slow").Fetch(1)
		if err == nil {
			first = ms[0].Offset
		}
		return err
	})
	if first != 8 {
		t.Fatalf("slow group resumed at %d, want 8", first)
	}
}

// TestTopicRetentionMaxAge: messages older than MaxAge (commit time) go;
// GC applies the same retention.
func TestTopicRetentionMaxAge(t *testing.T) {
	var now atomic.Int64
	db := mustOpen(t, store.NewMem(), WithClock(func() int64 { return now.Load() }))
	defer db.Close()
	for i := 0; i < 5; i++ {
		now.Store(int64(i+1) * int64(time.Second))
		pub(t, db, "t", 1)
	}
	wantTopic(t, db, "t", 0, 5)
	now.Store(int64(10 * time.Second))
	s, err := db.GC(ctx, GCOptions{Topics: TopicRetention{MaxAge: 7 * time.Second}}) // keep TS >= 3s
	if err != nil || s.Trimmed != 2 {
		t.Fatalf("%+v %v", s, err)
	}
	st, n, _ := topicState(t, db, "t")
	if st != 2 || n != 5 {
		t.Fatalf("start %d len %d", st, n)
	}
}

// TestTopicRetentionSubjects: re-sealed subject messages that survive the
// cut stay readable and shreddable, before and after reopening.
func TestTopicRetentionSubjects(t *testing.T) {
	st := store.NewMem()
	db := mustOpen(t, st)
	for i := 0; i < 4; i++ {
		db.Update(ctx, func(tx *Tx) error { return tx.Topic("t").Publish([]byte{byte(tx.Topic("t").Len())}, Subject("u")) })
	}
	if _, err := db.Compact(ctx, CompactOptions{Topics: TopicRetention{MaxMessages: 2}}); err != nil {
		t.Fatal(err)
	}
	wantTopic(t, db, "t", 2, 4)
	db.Close()
	db = mustOpen(t, st)
	defer db.Close()
	wantTopic(t, db, "t", 2, 4)
	if _, err := db.Shred(ctx, "u"); err != nil {
		t.Fatal(err)
	}
	_, _, ms := topicState(t, db, "t")
	if len(ms) != 2 || !ms[0].Redacted || !ms[1].Redacted {
		t.Fatalf("after shred: %+v", ms)
	}
}
