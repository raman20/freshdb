package lens

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/raman20/freshdb/format"
)

// kvLoad builds a KV of n keys ("user%010d", shuffled), one put each, as
// the engine folds them: 1 KiB values kept as log positions (inline 64),
// 8 puts per entry. The value buffer is shared: only the lens is measured.
func kvLoad(n int, seed int64) *KV {
	val := make([]byte, 1024)
	l := NewKVInline(64)
	perm := rand.New(rand.NewSource(seed)).Perm(n)
	e := &Entry{Log: "L1"}
	for i := 0; i < n; i += 8 {
		e.LSN++
		e.Ops = e.Ops[:0]
		for j := i; j < i+8 && j < n; j++ {
			e.Ops = append(e.Ops, format.Op{Kind: format.OpPut, Space: "kv", Key: fmt.Sprintf("user%010d", perm[j]), Value: val, ValOff: 100})
		}
		l.Apply(e, noEnv{})
	}
	return l
}

type noEnv struct{}

func (noEnv) Shredded(string) bool                { return false }
func (noEnv) Open(e *Entry, i int) ([]byte, bool) { return e.Ops[i].Value, true }

func heapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// benchKeys is the key count: 1M by default, FRESHDB_KV_KEYS overrides
// (the 10M-key acceptance run: FRESHDB_KV_KEYS=10000000).
func benchKeys() int {
	if s := os.Getenv("FRESHDB_KV_KEYS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return 1 << 20
}

// BenchmarkKVBuild: fold time per key and live heap per key after the
// fold (the KV lens only; values are positions).
func BenchmarkKVBuild(b *testing.B) {
	n := benchKeys()
	for i := 0; i < b.N; i++ {
		before := heapAlloc()
		t := time.Now()
		l := kvLoad(n, 1)
		el := time.Since(t)
		after := heapAlloc()
		b.ReportMetric(float64(el.Nanoseconds())/float64(n), "ns/key")
		b.ReportMetric(float64(after-before)/float64(n), "heapB/key")
		runtime.KeepAlive(l)
	}
}

var benchKV struct {
	n int
	l *KV
}

func loaded(b *testing.B) (*KV, int) {
	n := benchKeys()
	if benchKV.l == nil || benchKV.n != n {
		benchKV.l, benchKV.n = kvLoad(n, 1), n
	}
	b.ResetTimer()
	return benchKV.l, n
}

func BenchmarkKVGet(b *testing.B) {
	l, n := loaded(b)
	r := rand.New(rand.NewSource(2))
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("user%010d", r.Intn(n))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := l.Get("kv", keys[i%len(keys)], math64); !ok {
			b.Fatal("missing")
		}
	}
	b.ReportMetric(float64(len(l.runs)), "runs")
}

const math64 = ^uint64(0)

// BenchmarkKVScan100: a prefix that matches 100 keys.
func BenchmarkKVScan100(b *testing.B) {
	l, n := loaded(b)
	for i := 0; i < b.N; i++ {
		p := fmt.Sprintf("user%08d", (i*7919)%(n/100))
		if got := len(l.Scan("kv", p, math64)); got != 100 {
			b.Fatalf("%s: %d keys", p, got)
		}
	}
}

func BenchmarkKVSnapshotLoad(b *testing.B) {
	l, n := loaded(b)
	snap := l.Snapshot()
	b.SetBytes(int64(len(snap)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		before := heapAlloc()
		l2, err := LoadKV(snap, 64)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(heapAlloc()-before)/float64(n), "heapB/key")
		runtime.KeepAlive(l2)
	}
}
