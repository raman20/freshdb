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

// benchMsgs is the message count: 1M by default, FRESHDB_TOPIC_MSGS
// overrides.
func benchMsgs() int {
	if s := os.Getenv("FRESHDB_TOPIC_MSGS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return 1 << 20
}

// topicLoad folds n messages of 256 B into 16 topics, 8 appends per
// entry; every 16th message has a subject (s0..s63). Each value is its own
// allocation, as decoded entries are.
func topicLoad(n int) *Topic {
	l := NewTopic()
	e := &Entry{Log: "L1"}
	for i := 0; i < n; i += 8 {
		e.LSN++
		e.Ops = e.Ops[:0]
		for j := i; j < i+8 && j < n; j++ {
			op := format.Op{Kind: format.OpAppend, Space: fmt.Sprintf("topic:t%02d", j%16), Value: make([]byte, 256)}
			if j%16 == 0 {
				op.Subject = fmt.Sprintf("s%d", (j/16)%64)
			}
			e.Ops = append(e.Ops, op)
		}
		l.Apply(e, noEnv{})
	}
	return l
}

func BenchmarkTopicApply(b *testing.B) {
	n := benchMsgs()
	for i := 0; i < b.N; i++ {
		before := heapAlloc()
		t := time.Now()
		l := topicLoad(n)
		el := time.Since(t)
		b.ReportMetric(float64(el.Nanoseconds())/float64(n), "ns/msg")
		b.ReportMetric(float64(heapAlloc()-before)/float64(n), "heapB/msg")
		runtime.KeepAlive(l)
	}
}

var benchTopic struct {
	n int
	l *Topic
}

func loadedTopic(b *testing.B) (*Topic, int) {
	n := benchMsgs()
	if benchTopic.l == nil || benchTopic.n != n {
		benchTopic.l, benchTopic.n = topicLoad(n), n
	}
	b.ResetTimer()
	return benchTopic.l, n
}

// BenchmarkTopicRead100: 100 messages from a random offset.
func BenchmarkTopicRead100(b *testing.B) {
	l, n := loadedTopic(b)
	r := rand.New(rand.NewSource(2))
	for i := 0; i < b.N; i++ {
		if got := len(l.Read("topic:t03", uint64(r.Intn(n/16-100)), 100, math64)); got != 100 {
			b.Fatalf("read %d", got)
		}
	}
}

func BenchmarkTopicSnapshotLoad(b *testing.B) {
	l, n := loadedTopic(b)
	snap := l.Snapshot()
	b.SetBytes(int64(len(snap)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		before := heapAlloc()
		l2, err := LoadTopic(snap)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(heapAlloc()-before)/float64(n), "heapB/msg")
		runtime.KeepAlive(l2)
	}
}

// BenchmarkTopicDropSubject: shred one subject that still has messages
// (1/1024 of them; 1,024 messages at the default 1M). Every shred hits
// a live subject: a fresh topic is loaded, outside the timer, every 64
// shreds. Run with -benchtime 64x; with a time-based benchtime the
// reloads dominate the wall time.
func BenchmarkTopicDropSubject(b *testing.B) {
	n := benchMsgs()
	var l *Topic
	for i := 0; i < b.N; i++ {
		if i%64 == 0 {
			b.StopTimer()
			l = nil
			l = topicLoad(n)
			b.StartTimer()
		}
		l.DropSubject(fmt.Sprintf("s%d", i%64))
	}
}
