package freshdb

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raman20/freshdb/store"
)

const crashG = 4

// crashChild is run in a subprocess: 4 goroutines commit forever, printing
// "ACK g i" only after Update returned nil (i.e. after fsync+rename).
func crashChild(loc string) {
	st, err := store.Open(loc)
	if err != nil {
		panic(err)
	}
	db, err := OpenStore(ctx, st, WithLogf(func(string, ...any) {}), WithSnapshotEvery(50))
	if err != nil {
		fmt.Println("OPENERR", err)
		os.Exit(3)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for g := 0; g < crashG; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			var start int
			db.View(ctx, func(tx *Tx) error {
				v, _ := tx.Get(fmt.Sprint("cnt", g))
				start, _ = strconv.Atoi(string(v))
				return nil
			})
			for i := start + 1; ; i++ {
				err := db.Update(ctx, func(tx *Tx) error {
					tx.Put(fmt.Sprintf("g%d/%08d", g, i), []byte("x"))
					tx.Put(fmt.Sprint("cnt", g), []byte(strconv.Itoa(i)))
					return tx.Topic("t").Publish([]byte(fmt.Sprintf("%d:%d", g, i)))
				})
				if err != nil {
					fmt.Println("ERR", err)
					os.Exit(4)
				}
				mu.Lock()
				os.Stdout.WriteString(fmt.Sprintf("ACK %d %d\n", g, i))
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
}

func TestMain(m *testing.M) {
	if d := os.Getenv("FRESHDB_CRASH_CHILD"); d != "" {
		crashChild(d)
		return
	}
	os.Exit(m.Run())
}

func TestCrashKill9PrefixConsistent(t *testing.T) { crashKill9(t, t.TempDir()) }

// The journal store keeps every segment in one file; same invariants.
func TestCrashKill9Journal(t *testing.T) { crashKill9(t, "journal://"+t.TempDir()) }

// crashKill9 repeatedly SIGKILLs a committing child process on the store at
// loc (a store URL or bare path) and checks the recovered state.
func crashKill9(t *testing.T, dir string) {
	if testing.Short() {
		t.Skip("short")
	}
	iters := 25
	if s := os.Getenv("FRESHDB_CRASH_ITERS"); s != "" {
		iters, _ = strconv.Atoi(s)
	}
	acked := make([]int, crashG)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for it := 0; it < iters; it++ {
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), "FRESHDB_CRASH_CHILD="+dir)
		out, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				f := strings.Fields(sc.Text())
				if len(f) == 3 && f[0] == "ACK" {
					g, _ := strconv.Atoi(f[1])
					i, _ := strconv.Atoi(f[2])
					if i > acked[g] {
						acked[g] = i
					}
				} else {
					t.Errorf("child: %s", sc.Text())
				}
			}
			close(done)
		}()
		time.Sleep(time.Duration(20+rng.Intn(150)) * time.Millisecond)
		cmd.Process.Kill() // SIGKILL mid-write
		cmd.Wait()
		<-done

		// recover in-process and check the invariants
		st, err := store.Open(dir)
		if err != nil {
			t.Fatalf("iter %d: store: %v", it, err)
		}
		db, err := OpenStore(ctx, st, quiet())
		if err != nil {
			t.Fatalf("iter %d: reopen: %v", it, err)
		}
		db.View(ctx, func(tx *Tx) error {
			seen := map[string]bool{}
			ms, _ := tx.Topic("t").Read(0, 0)
			for _, m := range ms {
				if seen[string(m.Value)] {
					t.Fatalf("iter %d: message %s applied twice", it, m.Value)
				}
				seen[string(m.Value)] = true
			}
			for g := 0; g < crashG; g++ {
				v, _ := tx.Get(fmt.Sprint("cnt", g))
				n, _ := strconv.Atoi(string(v))
				if n < acked[g] {
					t.Fatalf("iter %d: g%d recovered %d < acked %d: LOST ACKED WRITE", it, g, n, acked[g])
				}
				kvs, _ := tx.Scan(fmt.Sprintf("g%d/", g))
				if len(kvs) != n {
					t.Fatalf("iter %d g%d: %d keys but counter %d (not atomic)", it, g, len(kvs), n)
				}
				for i := 1; i <= n; i++ {
					if !seen[fmt.Sprintf("%d:%d", g, i)] {
						t.Fatalf("iter %d: g%d msg %d missing (not prefix-consistent)", it, g, i)
					}
				}
				if seen[fmt.Sprintf("%d:%d", g, n+1)] {
					t.Fatalf("iter %d: message beyond counter", it)
				}
				acked[g] = n // unacked-but-durable commits count from now on
			}
			return nil
		})
		r, err := db.Verify(ctx)
		if err != nil || !r.OK {
			t.Fatalf("iter %d verify: %+v %v", it, r, err)
		}
		d1, _, _ := db.LensDigests("main")
		d2, _ := db.RebuildDigests("main")
		if fmt.Sprint(d1) != fmt.Sprint(d2) {
			t.Fatalf("iter %d: lens != fold(log)", it)
		}
		db.Close()
	}
	total := 0
	for _, a := range acked {
		total += a
	}
	t.Logf("%d kill -9 cycles, %d transactions recovered, all acked writes present, prefix-consistent", iters, total)
}
