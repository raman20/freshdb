package main

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/raman20/freshdb/store"
)

// s3check runs store.S3Check against URL (an empty scratch prefix is
// used and cleaned up) and prints one line per guarantee.
func s3check(u string, flags map[string]string) int {
	large := 64
	if v := flags["large-mib"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			fmt.Println("freshdb s3check: --large-mib wants a number of MiB (0 = skip)")
			return 2
		}
		large = n
	}
	st, err := store.Open(u)
	if err != nil {
		fmt.Printf("FAIL  open %s: %v\n", u, err)
		return 1
	}
	if c, ok := st.(io.Closer); ok {
		defer c.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	fmt.Printf("freshdb s3check %s (writes only under _s3check/, removed afterwards)\n", u)
	bad := 0
	for _, r := range store.S3Check(ctx, st, large<<20) {
		mark := "ok  "
		if !r.OK {
			mark, bad = "FAIL", bad+1
		}
		fmt.Printf("%s  %-44s %s\n", mark, r.Name, r.Detail)
	}
	if bad > 0 {
		fmt.Printf("\nverdict: NOT SAFE for freshDB writes (%d check(s) failed). Reading may work; do not open it for writing.\n", bad)
		return 1
	}
	fmt.Println("\nverdict: every guarantee freshDB relies on held in this run (docs/s3.md). This is a probe, not a proof: rerun after endpoint or proxy changes.")
	return 0
}
