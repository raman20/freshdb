package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/raman20/freshdb"
)

// broker is the messaging surface both freshDB and NATS JetStream are driven through.
type broker interface {
	setup(topic string, groups []string) error
	publish(topic string, payload []byte) error
	// consume fetches up to max messages for group and durably acks them
	// before returning (freshDB: one transaction; NATS: AckSync on the last).
	consume(topic, group string, max int) ([][]byte, error)
	durability() string
	close()
}

// ---- freshDB ----

type freshBroker struct {
	db    *freshdb.DB
	fsync bool
}

func (b *freshBroker) setup(string, []string) error { return nil }
func (b *freshBroker) publish(topic string, p []byte) error {
	return b.db.Update(bg, func(tx *freshdb.Tx) error { return tx.Topic(topic).Publish(p) })
}
func (b *freshBroker) consume(topic, group string, max int) (out [][]byte, err error) {
	err = b.db.Update(bg, func(tx *freshdb.Tx) error {
		out = out[:0]
		g := tx.Topic(topic).Group(group)
		msgs, err := g.Fetch(max)
		if err != nil || len(msgs) == 0 {
			return err
		}
		for _, m := range msgs {
			out = append(out, m.Value)
		}
		return g.Ack(msgs[len(msgs)-1])
	})
	return
}
func (b *freshBroker) durability() string {
	if b.fsync {
		return "publish and consume+ack are each a durable transaction (fsync'd group commit)"
	}
	return "SetUnsafeNoSync (no fsync)"
}
func (b *freshBroker) close() { b.db.Close() }

// ---- NATS JetStream (embedded server, loopback client) ----

type natsBroker struct {
	srv   *server.Server
	nc    *nats.Conn
	js    nats.JetStreamContext
	mu    sync.Mutex
	subs  map[string]*nats.Subscription
	fsync bool
}

func openNATS(dir string, fsync bool) (*natsBroker, error) {
	opts := &server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: dir, NoLog: true, NoSigs: true, SyncAlways: fsync}
	s, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		return nil, errors.New("nats: server not ready")
	}
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream(nats.MaxWait(30 * time.Second))
	if err != nil {
		return nil, err
	}
	return &natsBroker{srv: s, nc: nc, js: js, subs: map[string]*nats.Subscription{}, fsync: fsync}, nil
}

func (b *natsBroker) setup(topic string, groups []string) error {
	if _, err := b.js.AddStream(&nats.StreamConfig{Name: topic, Subjects: []string{topic}, Storage: nats.FileStorage}); err != nil {
		return err
	}
	for _, g := range groups {
		sub, err := b.js.PullSubscribe(topic, g, nats.AckAll(), nats.MaxAckPending(-1))
		if err != nil {
			return err
		}
		b.mu.Lock()
		b.subs[topic+"/"+g] = sub
		b.mu.Unlock()
	}
	return nil
}
func (b *natsBroker) publish(topic string, p []byte) error {
	_, err := b.js.Publish(topic, p)
	return err
}
func (b *natsBroker) consume(topic, group string, max int) ([][]byte, error) {
	b.mu.Lock()
	sub := b.subs[topic+"/"+group]
	b.mu.Unlock()
	msgs, err := sub.Fetch(max, nats.MaxWait(5*time.Millisecond))
	if errors.Is(err, nats.ErrTimeout) || len(msgs) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(msgs))
	for i, m := range msgs {
		out[i] = m.Data
	}
	return out, msgs[len(msgs)-1].AckSync()
}
func (b *natsBroker) durability() string {
	if b.fsync {
		return "file storage, SyncAlways=true (fsync per write); publish waits for PubAck; consumer AckSync (AckAll) per batch"
	}
	return "file storage, default sync_interval (2m); publish waits for PubAck; consumer AckSync per batch"
}
func (b *natsBroker) close() { b.nc.Close(); b.srv.Shutdown(); b.srv.WaitForShutdown() }

// ---- workload ----

type msgRun struct {
	System      string  `json:"system"`
	Fsync       bool    `json:"fsync"`
	Durability  string  `json:"durability"`
	Kind        string  `json:"kind"` // fixed-rate | max-throughput
	Rate        float64 `json:"target_rate,omitempty"`
	Producers   int     `json:"producers"`
	Groups      int     `json:"groups"`
	Published   int64   `json:"published"`
	PublishRate float64 `json:"publish_per_sec"`
	Delivered   int64   `json:"delivered_per_group_min"`
	ConsumeRate float64 `json:"consume_per_sec_per_group"`
	Backlog     int64   `json:"backlog_at_end"` // max-throughput: unconsumed when producers stopped
	PublishLat  Lat     `json:"publish_latency"`
	EndToEnd    Lat     `json:"end_to_end_latency"`
	Errors      int64   `json:"errors"`
	FirstError  string  `json:"first_error,omitempty"`
}

func cmdMsg(args []string) {
	fs := flag.NewFlagSet("msg", flag.ExitOnError)
	system := fs.String("system", "freshdb", "freshdb|freshdb-journal|nats")
	fsync := fs.Bool("fsync", true, "durable writes")
	ratesF := fs.String("rates", "1000,5000,10000,20000", "fixed producer rates (msgs/s, aggregate)")
	groupsF := fs.String("groups", "1,4", "consumer group counts (each group sees every message); run at -group-rate")
	groupRate := fs.Float64("group-rate", 5000, "rate used for the consumer-group runs")
	producers := fs.Int("producers", 8, "producer clients")
	maxProducers := fs.Int("max-producers", 64, "closed-loop producers for the max-throughput run")
	size := fs.Int("size", 1024, "payload bytes (first 8 = intended send time)")
	batch := fs.Int("batch", 100, "max messages per consume")
	dur := fs.Duration("duration", 5*time.Second, "producing time per run")
	drain := fs.Duration("drain", 30*time.Second, "max time to let consumers catch up")
	dataDir := fs.String("dir", "/tmp/freshdb-bench", "data directory (wiped)")
	out := fs.String("out", "results", "output directory")
	fs.Parse(args)

	dir := filepath.Join(*dataDir, fmt.Sprintf("msg-%s-%v", *system, *fsync))
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0o755)
	var b broker
	switch *system {
	case "freshdb", "freshdb-journal":
		db, err := openFreshDB(dir, *fsync, *system == "freshdb-journal")
		if err != nil {
			fatal(err)
		}
		b = &freshBroker{db, *fsync}
	case "nats":
		nb, err := openNATS(dir, *fsync)
		if err != nil {
			fatal(err)
		}
		b = nb
	default:
		fatal(fmt.Errorf("unknown system %q", *system))
	}
	res := &Result{Suite: "msg", Env: envInfo(*dataDir), Params: map[string]any{
		"system": *system, "fsync": *fsync, "producers": *producers, "max_producers": *maxProducers, "payload_bytes": *size,
		"consume_batch": *batch, "duration": dur.String(), "drain_limit": drain.String(),
		"e2e": "consumer-side time after the durable ack minus the producer's intended send time (open-loop schedule)",
	}}
	if strings.HasPrefix(*system, "freshdb") {
		res.Notes = append(res.Notes, "freshDB consumers poll: an empty fetch sleeps 200us before retrying (no push notification yet).")
	}
	n := 0
	for _, r := range parseInts(*ratesF) {
		n++
		res.Runs = append(res.Runs, msgOnce(b, fmt.Sprintf("t%d", n), *system, *fsync, float64(r), *producers, 1, *size, *batch, *dur, *drain))
	}
	for _, g := range parseInts(*groupsF) {
		if g == 1 {
			continue // covered by the rate sweep when group-rate is in it
		}
		n++
		res.Runs = append(res.Runs, msgOnce(b, fmt.Sprintf("t%d", n), *system, *fsync, *groupRate, *producers, g, *size, *batch, *dur, *drain))
	}
	n++
	res.Runs = append(res.Runs, msgOnce(b, fmt.Sprintf("t%d", n), *system, *fsync, 0, *maxProducers, 1, *size, *batch, *dur, *drain))
	b.close()
	res.Params.(map[string]any)["peak_rss"] = peakRSS()
	res.Params.(map[string]any)["disk_after_run"] = diskUsage(dir)
	writeResult(*out, fmt.Sprintf("msg-%s-fsync-%v", *system, *fsync), res)
	wipe(dir)
}

// msgOnce runs producers (open loop at rate, or closed loop if rate == 0)
// while one consumer per group consumes concurrently.
func msgOnce(b broker, topic, system string, fsync bool, rate float64, producers, groups, size, batch int, dur, drain time.Duration) msgRun {
	gnames := make([]string, groups)
	for i := range gnames {
		gnames[i] = fmt.Sprintf("g%d", i)
	}
	if err := b.setup(topic, gnames); err != nil {
		fatal(err)
	}
	var published atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	e2e := make([]*hdrhistogram.Histogram, groups)
	delivered := make([]atomic.Int64, groups)
	var cerrs atomic.Int64
	var cfirst atomic.Value
	for gi := range gnames {
		e2e[gi] = newHist()
		wg.Add(1)
		go func(gi int) {
			defer wg.Done()
			deadline := time.Time{}
			for {
				msgs, err := b.consume(topic, gnames[gi], batch)
				now := time.Now()
				if err != nil {
					cerrs.Add(1)
					cfirst.CompareAndSwap(nil, err.Error())
				}
				for _, m := range msgs {
					sent := int64(binary.LittleEndian.Uint64(m))
					record(e2e[gi], now.Sub(time.Unix(0, sent)))
				}
				delivered[gi].Add(int64(len(msgs)))
				if stop.Load() {
					if delivered[gi].Load() >= published.Load() {
						return
					}
					if deadline.IsZero() {
						deadline = now.Add(drain)
					} else if now.After(deadline) {
						return
					}
				}
				if len(msgs) == 0 && strings.HasPrefix(system, "freshdb") {
					time.Sleep(200 * time.Microsecond)
				}
			}
		}(gi)
	}
	mk := func(int) func(time.Time) error {
		return func(due time.Time) error {
			p := make([]byte, size)
			binary.LittleEndian.PutUint64(p, uint64(due.UnixNano()))
			err := b.publish(topic, p)
			if err == nil {
				published.Add(1)
			}
			return err
		}
	}
	var ph Phase
	kind := "fixed-rate"
	if rate > 0 {
		ph = runOpenDue(producers, rate, dur, mk)
	} else {
		kind = "max-throughput"
		ph = runClosed(producers, dur, func(id int) opFunc {
			f := mk(id)
			return func() error { return f(time.Now()) }
		})
	}
	atStop := delivered[0].Load()
	for i := range delivered {
		atStop = min(atStop, delivered[i].Load())
	}
	stop.Store(true)
	wg.Wait()
	all := newHist()
	minDel := delivered[0].Load()
	for i, h := range e2e {
		all.Merge(h)
		minDel = min(minDel, delivered[i].Load())
	}
	r := msgRun{System: system, Fsync: fsync, Durability: b.durability(), Kind: kind, Rate: rate, Producers: producers, Groups: groups,
		Published: published.Load(), PublishRate: ph.OpsPerSec, Delivered: minDel, Backlog: published.Load() - minDel,
		PublishLat: ph.Latency, EndToEnd: summarize(all), Errors: ph.Errors + cerrs.Load(), FirstError: ph.FirstError}
	if s, ok := cfirst.Load().(string); ok && r.FirstError == "" {
		r.FirstError = s
	}
	if kind == "max-throughput" {
		// consumers ran concurrently with producers; report what they kept up with
		r.ConsumeRate = round(float64(atStop)/ph.Seconds, 1)
		r.Backlog = published.Load() - atStop
		r.EndToEnd = Lat{} // closed-loop producers have no schedule; e2e here would just measure backlog
	}
	logf("msg %s fsync=%v %s rate=%.0f groups=%d: published %d (%.0f/s) delivered %d | e2e p50 %.0fus p99 %.0fus p99.9 %.0fus | errs %d",
		system, fsync, kind, rate, groups, r.Published, r.PublishRate, minDel, r.EndToEnd.P50Us, r.EndToEnd.P99Us, r.EndToEnd.P999Us, r.Errors)
	return r
}
