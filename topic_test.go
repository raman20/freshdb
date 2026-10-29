package freshdb

import (
	"errors"
	"testing"

	"github.com/raman20/freshdb/format"

	"github.com/raman20/freshdb/store"
)

// pub publishes n messages to topic, each holding its offset's low byte.
func pub(t *testing.T, db *DB, topic string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := db.Update(ctx, func(tx *Tx) error { tt := tx.Topic(topic); return tt.Publish([]byte{byte(tt.Len())}) }); err != nil {
			t.Fatal(err)
		}
	}
}

func cursor(t *testing.T, db *DB, topic, group string) uint64 {
	t.Helper()
	var c uint64
	if err := db.View(ctx, func(tx *Tx) (err error) { c, err = tx.Topic(topic).Group(group).Cursor(); return }); err != nil {
		t.Fatal(err)
	}
	return c
}

// TestGroupAckNeverMovesBack: C-D2. Acking an earlier message after a
// later one used to rewind the cursor, so the later messages were
// delivered again.
func TestGroupAckNeverMovesBack(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	pub(t, db, "t", 5)
	err := db.Update(ctx, func(tx *Tx) error {
		g := tx.Topic("t").Group("g")
		ms, err := g.Fetch(5)
		if err != nil {
			return err
		}
		if err := g.Ack(ms[3]); err != nil {
			return err
		}
		return g.Ack(ms[1])
	})
	if err != nil {
		t.Fatal(err)
	}
	if c := cursor(t, db, "t", "g"); c != 4 {
		t.Fatalf("cursor %d, want 4", c)
	}
	// Seek still moves either way.
	db.Update(ctx, func(tx *Tx) error { return tx.Topic("t").Group("g").Seek(1) })
	if c := cursor(t, db, "t", "g"); c != 1 {
		t.Fatalf("cursor after Seek %d, want 1", c)
	}
}

// TestGroupAckOtherTopic: C-D2. A message of topic a acked in a group of
// topic b used to set b's cursor from a's offset.
func TestGroupAckOtherTopic(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	pub(t, db, "a", 3)
	err := db.Update(ctx, func(tx *Tx) error {
		ms, err := tx.Topic("a").Read(2, 1)
		if err != nil {
			return err
		}
		return tx.Topic("b").Group("g").Ack(ms[0])
	})
	if !errors.Is(err, ErrBadName) {
		t.Fatalf("err = %v, want ErrBadName", err)
	}
	if c := cursor(t, db, "b", "g"); c != 0 {
		t.Fatalf("cursor of b moved to %d", c)
	}
}

// TestGroupNameNUL: C-D2. Group "x\x00y" on topic "z" and group "x" on
// topic "y\x00z" shared one cursor.
func TestGroupNameNUL(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	err := db.Update(ctx, func(tx *Tx) error { return tx.Topic("z").Group("x\x00y").Seek(7) })
	if !errors.Is(err, ErrBadName) {
		t.Fatalf("Seek: err = %v, want ErrBadName", err)
	}
	err = db.View(ctx, func(tx *Tx) error { _, err := tx.Topic("z").Group("x\x00y").Fetch(1); return err })
	if !errors.Is(err, ErrBadName) {
		t.Fatalf("Fetch: err = %v, want ErrBadName", err)
	}
	// a NUL in the topic name is fine: the group name ends the key's first part
	if err := db.Update(ctx, func(tx *Tx) error { return tx.Topic("y\x00z").Group("x").Seek(3) }); err != nil {
		t.Fatal(err)
	}
	if c := cursor(t, db, "y\x00z", "x"); c != 3 {
		t.Fatalf("cursor %d", c)
	}
}

// TestGroupCursorCorrupt: C-D2. A cursor value of the wrong size read as
// offset 0, so the group silently re-consumed the whole topic.
func TestGroupCursorCorrupt(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	pub(t, db, "t", 2)
	err := db.Update(ctx, func(tx *Tx) error {
		return tx.write(format.Op{Kind: format.OpPut, Space: spaceCursor, Key: cursorKey("g", "t"), Value: []byte{1, 2, 3}}, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.View(ctx, func(tx *Tx) error { _, err := tx.Topic("t").Group("g").Fetch(10); return err })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}
