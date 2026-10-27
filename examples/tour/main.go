// Command tour is a runnable walk through the freshDB Go API. Every output
// shown in the README's "API tour" comes from running this program:
//
//	go run ./examples/tour
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raman20/freshdb"
)

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	ctx := context.Background()
	// One URL: a directory, s3://bucket/prefix?endpoint=..., or mem:// for tests.
	db, err := freshdb.Open(ctx, "mem://tour", freshdb.WithLogf(func(string, ...any) {}))
	must(err)
	defer db.Close()

	// KV
	must(db.Put(ctx, "config/model", []byte("small")))
	v, _ := db.Get(ctx, "config/model")
	fmt.Printf("kv: config/model = %s\n", v)

	// Topics + consumer groups: the result write and the ack are one transaction.
	must(db.Update(ctx, func(tx *freshdb.Tx) error {
		tx.Topic("tasks").Publish([]byte("summarise inbox"))
		return tx.Topic("tasks").Publish([]byte("draft reply"))
	}))
	must(db.Update(ctx, func(tx *freshdb.Tx) error {
		g := tx.Topic("tasks").Group("worker")
		msgs, err := g.Fetch(1)
		if err != nil || len(msgs) == 0 {
			return err
		}
		fmt.Printf("topic: worker got offset %d %q\n", msgs[0].Offset, msgs[0].Value)
		if err := tx.Put("result/0", []byte("3 unread, 1 urgent")); err != nil {
			return err
		}
		return g.Ack(msgs[0]) // commits atomically with result/0, or not at all
	}))

	// Documents + a secondary index added later (built by replaying the log).
	must(db.Update(ctx, func(tx *freshdb.Tx) error {
		tx.Doc("memories").Put("m1", map[string]any{"user": "alice", "text": "allergic to peanuts"}, freshdb.Subject("alice"))
		return tx.Doc("memories").Put("m2", map[string]any{"user": "bob", "text": "likes window seats"}, freshdb.Subject("bob"))
	}))
	must(db.RegisterIndex(ctx, "memories", "user"))
	must(db.View(ctx, func(tx *freshdb.Tx) error {
		ids, err := tx.Doc("memories").Find("user", "alice")
		fmt.Printf("docs: memories where user=alice -> %v\n", ids)
		return err
	}))

	// Vectors (cosine, brute force).
	must(db.Update(ctx, func(tx *freshdb.Tx) error {
		tx.Vector("mem").Put("m1", []float32{0.9, 0.1, 0}, freshdb.Subject("alice"))
		return tx.Vector("mem").Put("m2", []float32{0.1, 0.9, 0}, freshdb.Subject("bob"))
	}))
	must(db.View(ctx, func(tx *freshdb.Tx) error {
		hits, err := tx.Vector("mem").Search([]float32{1, 0, 0}, 1)
		fmt.Printf("vectors: nearest to [1 0 0] -> %s (%.3f)\n", hits[0].ID, hits[0].Score)
		return err
	}))

	// Time travel, branches, rewind.
	main := db.Branch("main")
	good, _ := main.Head()
	must(db.Put(ctx, "config/model", []byte("oops-huge")))
	must(main.ViewAt(ctx, good, func(tx *freshdb.Tx) error {
		old, err := tx.Get("config/model")
		fmt.Printf("time travel: at LSN %d config/model = %s\n", good, old)
		return err
	}))
	must(db.CreateBranch(ctx, "experiment", "main", good))
	must(db.Branch("experiment").Update(ctx, func(tx *freshdb.Tx) error { return tx.Put("config/model", []byte("medium")) }))
	must(db.Rewind(ctx, "main", good))
	v, _ = db.Get(ctx, "config/model")
	fmt.Printf("rewind: main config/model = %s (old head kept in reflog)\n", v)
	must(db.Branch("experiment").View(ctx, func(tx *freshdb.Tx) error {
		e, err := tx.Get("config/model")
		fmt.Printf("branch: experiment config/model = %s\n", e)
		return err
	}))

	// Shred: alice's data becomes unreadable everywhere, provably.
	cert, err := db.Shred(ctx, "alice")
	must(err)
	fmt.Printf("shred: certificate for LSN %d, DEK deleted=%v, signed=%v\n", cert.ShredLSN, cert.DEKDeleted, cert.Signature != "")
	must(db.View(ctx, func(tx *freshdb.Tx) error {
		ids, _ := tx.Doc("memories").Find("user", "alice")
		hits, _ := tx.Vector("mem").Search([]float32{1, 0, 0}, 1)
		fmt.Printf("after shred: docs(user=alice) -> %v, nearest -> %s\n", ids, hits[0].ID)
		return nil
	}))
	err = db.Update(ctx, func(tx *freshdb.Tx) error { return tx.Put("x", []byte("y"), freshdb.Subject("alice")) })
	fmt.Printf("after shred: new write for alice -> %v\n", err)
	rep, err := db.Verify(ctx)
	must(err)
	fmt.Printf("verify: ok=%v\n", rep.OK)
}
