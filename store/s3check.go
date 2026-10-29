package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// CheckResult is the outcome of one S3Check probe.
type CheckResult struct {
	Name   string
	OK     bool
	Detail string
}

// S3Check probes the guarantees freshDB relies on from an object store
// (docs/s3.md): atomic create-only writes (If-None-Match: *) including
// under concurrent creates, read-after-write for GET and LIST, delete
// visibility, ranged GETs, error mapping, and one large single-PUT object
// of large bytes (0 = skip). It writes only under _s3check/<random>/ and
// deletes what it wrote. Every result with OK false means: do not write
// to this store with freshDB.
func S3Check(ctx context.Context, st Store, large int) []CheckResult {
	nonce := make([]byte, 6)
	rand.Read(nonce)
	p := "_s3check/" + hex.EncodeToString(nonce) + "/"
	var out []CheckResult
	add := func(name string, err error, detail string) {
		r := CheckResult{Name: name, OK: err == nil, Detail: detail}
		if err != nil {
			r.Detail = err.Error()
		}
		out = append(out, r)
	}
	var written []string
	put := func(k string, v []byte) error {
		err := st.PutIfAbsent(ctx, p+k, v)
		if err == nil {
			written = append(written, p+k)
		}
		return err
	}
	defer func() {
		for _, k := range written {
			st.Delete(ctx, k)
		}
	}()

	// 1. read-after-write
	var lat []time.Duration
	err := func() error {
		for i := 0; i < 20; i++ {
			v := []byte(fmt.Sprintf("value-%d", i))
			t0 := time.Now()
			if err := put(fmt.Sprintf("raw/%02d", i), v); err != nil {
				return fmt.Errorf("put: %w", err)
			}
			lat = append(lat, time.Since(t0))
			got, err := st.Get(ctx, p+fmt.Sprintf("raw/%02d", i))
			if err != nil || !bytes.Equal(got, v) {
				return fmt.Errorf("GET right after PUT: %q, %v (stale or missing read)", got, err)
			}
		}
		return nil
	}()
	add("read-after-write (20 objects)", err, latency("PUT", lat))

	// 2. create-only
	err = func() error {
		if err := put("once", []byte("first")); err != nil {
			return err
		}
		if err := st.PutIfAbsent(ctx, p+"once", []byte("second")); !errors.Is(err, ErrExists) {
			return fmt.Errorf("second create of the same key: %v (want 412 Precondition Failed): the store ignores If-None-Match; writes would not be fenced", err)
		}
		got, err := st.Get(ctx, p+"once")
		if err != nil || string(got) != "first" {
			return fmt.Errorf("after a refused create the object reads %q, %v", got, err)
		}
		return nil
	}()
	add("create-only write (If-None-Match: *)", err, "second create refused, object unchanged")

	// 3. concurrent creates: exactly one winner, everyone reads its bytes
	err = func() error {
		for round := 0; round < 10; round++ {
			k := fmt.Sprintf("race/%d", round)
			const n = 8
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = st.PutIfAbsent(ctx, p+k, []byte(fmt.Sprint("writer-", i)))
				}()
			}
			wg.Wait()
			written = append(written, p+k)
			win := -1
			for i, e := range errs {
				switch {
				case e == nil && win >= 0:
					return fmt.Errorf("round %d: writers %d and %d both created the key (lost update: fencing unsafe)", round, win, i)
				case e == nil:
					win = i
				case !errors.Is(e, ErrExists):
					return fmt.Errorf("round %d: writer %d: %v", round, i, e)
				}
			}
			if win < 0 {
				return fmt.Errorf("round %d: no writer won", round)
			}
			got, err := st.Get(ctx, p+k)
			if err != nil || string(got) != fmt.Sprint("writer-", win) {
				return fmt.Errorf("round %d: winner %d but object reads %q, %v", round, win, got, err)
			}
		}
		return nil
	}()
	add("concurrent creates (10 rounds x 8 writers)", err, "exactly one winner per key")

	// 4. list-after-write
	err = func() error {
		got, err := st.List(ctx, p+"raw/")
		if err != nil {
			return err
		}
		sort.Strings(got)
		if len(got) != 20 {
			return fmt.Errorf("LIST right after 20 PUTs returned %d keys (stale listing: open could miss a manifest)", len(got))
		}
		return nil
	}()
	add("list-after-write", err, "LIST sees every new key at once")

	// 5. ranged GET
	err = func() error {
		rr, ok := st.(RangeReader)
		if !ok {
			return nil
		}
		b, err := rr.GetRange(ctx, p+"raw/07", 2, 3)
		if err != nil || string(b) != "lue" {
			return fmt.Errorf("range 2+3 of %q: %q, %v", "value-7", b, err)
		}
		return nil
	}()
	add("ranged GET", err, "Range: bytes=2-4")

	// 6. missing key, delete visibility
	err = func() error {
		if _, err := st.Get(ctx, p+"absent"); !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("GET of a missing key: %v (want not found)", err)
		}
		if err := st.Delete(ctx, p+"raw/00"); err != nil {
			return err
		}
		if _, err := st.Get(ctx, p+"raw/00"); !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("GET after DELETE: %v (want not found)", err)
		}
		if err := st.PutIfAbsent(ctx, p+"raw/00", []byte("again")); err != nil {
			return fmt.Errorf("re-create after DELETE: %v", err)
		}
		return nil
	}()
	add("not-found mapping, delete visibility", err, "GET after DELETE is not found; key can be re-created")

	// 7. large single-PUT object
	if large > 0 {
		detail := ""
		err = func() error {
			v := make([]byte, large)
			rand.Read(v)
			t0 := time.Now()
			if err := put("large", v); err != nil {
				return fmt.Errorf("PUT %d MiB: %w", large>>20, err)
			}
			up := time.Since(t0)
			t0 = time.Now()
			got, err := st.Get(ctx, p+"large")
			if err != nil || !bytes.Equal(got, v) {
				return fmt.Errorf("GET %d MiB: %d bytes, %v", large>>20, len(got), err)
			}
			detail = fmt.Sprintf("PUT %v, GET %v", up.Round(time.Millisecond), time.Since(t0).Round(time.Millisecond))
			return nil
		}()
		add(fmt.Sprintf("large object (%d MiB, single PUT)", large>>20), err, detail)
	}
	return out
}

func latency(op string, d []time.Duration) string {
	if len(d) == 0 {
		return ""
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return fmt.Sprintf("%s p50 %v, max %v", op, d[len(d)/2].Round(100*time.Microsecond), d[len(d)-1].Round(100*time.Microsecond))
}
