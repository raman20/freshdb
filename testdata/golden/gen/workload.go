package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"

	"github.com/raman20/freshdb"
)

// --- workload begin ---
// (testdata/golden/gen/workload.go holds an identical copy; it builds the
// v0.1.0 golden database with the v0.1.0 code. Only API present in v0.1.0.)

func goldenOpts() []freshdb.Option {
	var now int64 = 1_800_000_000_000_000_000
	return []freshdb.Option{
		freshdb.WithClock(func() int64 { now += 1_000_000; return now }),
		freshdb.WithRand(rand.New(rand.NewSource(1))),
		freshdb.WithSnapshotEvery(10),
		freshdb.WithLogf(func(string, ...any) {}),
	}
}

func goldenWorkload(ctx context.Context, db *freshdb.DB) error {
	for i := 0; i < 40; i++ {
		err := db.Update(ctx, func(tx *freshdb.Tx) error {
			v := []byte(fmt.Sprint("v", i))
			if i%3 == 0 {
				v = bytes.Repeat(v, 40) // over 64 B: kept out of line
			}
			if err := tx.Put(fmt.Sprintf("k%02d", i%25), v); err != nil {
				return err
			}
			if i%7 == 6 {
				if err := tx.Delete(fmt.Sprintf("k%02d", i%25-1)); err != nil {
					return err
				}
			}
			if err := tx.Topic("events").Publish([]byte(fmt.Sprint("e", i))); err != nil {
				return err
			}
			doc := map[string]any{"team": fmt.Sprint("t", i%2), "n": i}
			if err := tx.Doc("users").Put(fmt.Sprint("u", i%5), doc); err != nil {
				return err
			}
			return tx.Vector("emb").Put(fmt.Sprint("x", i%10), []float32{float32(i), float32(i % 3), 1, -float32(i % 5)})
		})
		if err != nil {
			return err
		}
		if i == 10 {
			if err := db.RegisterIndex(ctx, "users", "team"); err != nil {
				return err
			}
		}
	}
	err := db.Update(ctx, func(tx *freshdb.Tx) error {
		tx.Put("alice/email", []byte("alice@example.com"), freshdb.Subject("alice"))
		tx.Put("bob/email", []byte("bob@example.com"), freshdb.Subject("bob"))
		return tx.Topic("events").Publish([]byte("alice-event"), freshdb.Subject("alice"))
	})
	if err != nil {
		return err
	}
	if _, err := db.Shred(ctx, "alice"); err != nil {
		return err
	}
	err = db.Update(ctx, func(tx *freshdb.Tx) error {
		g := tx.Topic("events").Group("g1")
		ms, err := g.Fetch(5)
		if err != nil || len(ms) == 0 {
			return fmt.Errorf("fetch: %d %v", len(ms), err)
		}
		return g.Ack(ms[len(ms)-1])
	})
	if err != nil {
		return err
	}
	if err := db.CreateBranch(ctx, "exp", "main", 20); err != nil {
		return err
	}
	return db.Branch("exp").Update(ctx, func(tx *freshdb.Tx) error {
		tx.Put("exp-only", []byte("x"))
		return tx.Delete("k01")
	})
}

// goldenDump prints everything a reader can see, in a fixed order.
func goldenDump(ctx context.Context, db *freshdb.DB) (string, error) {
	var b strings.Builder
	for _, br := range []string{"main", "exp"} {
		head, err := db.Branch(br).Head()
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "branch %s head %d\n", br, head)
		err = db.Branch(br).View(ctx, func(tx *freshdb.Tx) error {
			kvs, err := tx.Scan("")
			if err != nil {
				return err
			}
			for _, kv := range kvs {
				fmt.Fprintf(&b, "kv %s = %q\n", kv.Key, kv.Value)
			}
			for _, k := range []string{"alice/email", "bob/email"} {
				v, err := tx.Get(k)
				fmt.Fprintf(&b, "get %s = %q shredded=%v\n", k, v, errors.Is(err, freshdb.ErrShredded))
			}
			ms, err := tx.Topic("events").Read(0, 0)
			if err != nil {
				return err
			}
			for _, m := range ms {
				fmt.Fprintf(&b, "msg %d lsn %d ts %d %q redacted=%v\n", m.Offset, m.LSN, m.TS, m.Value, m.Redacted)
			}
			c, err := tx.Topic("events").Group("g1").Cursor()
			fmt.Fprintf(&b, "cursor g1 %d %v\n", c, err)
			for i := 0; i < 5; i++ {
				d, err := tx.Doc("users").Get(fmt.Sprint("u", i))
				fmt.Fprintf(&b, "doc u%d %s %v\n", i, d, err)
			}
			ids, err := tx.Doc("users").Find("team", "t1")
			fmt.Fprintf(&b, "find team=t1 %v %v\n", ids, err)
			hits, err := tx.Vector("emb").Search([]float32{3, 1, 1, -2}, 3)
			for _, h := range hits {
				fmt.Fprintf(&b, "hit %s %.4f\n", h.ID, h.Score)
			}
			return err
		})
		if err != nil {
			return "", err
		}
	}
	fmt.Fprintf(&b, "shredded alice=%v bob=%v\n", db.IsShredded("alice"), db.IsShredded("bob"))
	rep, err := db.Verify(ctx)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(&b, "verify ok=%v\n", rep.OK)
	return b.String(), nil
}

// --- workload end ---
