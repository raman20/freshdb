package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// MemSample is one reading of where this process's memory is.
// RSS = anon + file (+ shmem). Go-resident = runtime-mapped minus released.
// Off-heap anon (freshDB's mmap value cache arena, cgo) ≈ anon − Go-resident.
type MemSample struct {
	Phase       string `json:"phase"`
	RSSMiB      int64  `json:"rss_mib"`
	AnonMiB     int64  `json:"anon_mib"`
	FileMiB     int64  `json:"file_mib"`
	GoResMiB    int64  `json:"go_resident_mib"`  // runtime memory minus released to the OS
	HeapObjMiB  int64  `json:"heap_objects_mib"` // live + not-yet-swept heap objects
	HeapFreeMiB int64  `json:"heap_free_mib"`    // heap spans held by Go, free, not released
	GoalMiB     int64  `json:"gc_goal_mib"`
	OffHeapMiB  int64  `json:"anon_minus_go_mib"` // off-heap anon (value cache arena, cgo)
}

type memSampler struct {
	mu    sync.Mutex
	phase string
	peak  MemSample // sample with the highest RSS
	marks []MemSample
	stop  chan struct{}
	prof  string // heap profile prefix ("" = none)
}

var memS *memSampler

func startMem(prof string) *memSampler {
	s := &memSampler{phase: "start", stop: make(chan struct{}), prof: prof}
	go func() {
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				m := s.sample()
				s.mu.Lock()
				if m.RSSMiB > s.peak.RSSMiB {
					s.peak = m
				}
				s.mu.Unlock()
			}
		}
	}()
	return s
}

func (s *memSampler) setPhase(p string) { s.mu.Lock(); s.phase = p; s.mu.Unlock() }

// mark records a sample (after a GC, so heap objects ≈ live) and, if
// profiling, a heap profile named after the phase.
func (s *memSampler) mark(p string) {
	if s == nil {
		return
	}
	pre := s.sample() // before the forced GC: what the process really holds
	runtime.GC()
	m := s.sample()
	m.Phase = p + " (after GC)"
	pre.Phase = p
	s.mu.Lock()
	s.marks = append(s.marks, pre, m)
	s.mu.Unlock()
	logf("mem %s: rss %d anon %d file %d MiB | go-resident %d heap-live %d goal %d | off-heap anon %d MiB",
		p, pre.RSSMiB, pre.AnonMiB, pre.FileMiB, pre.GoResMiB, m.HeapObjMiB, pre.GoalMiB, pre.OffHeapMiB)
	if s.prof != "" {
		if f, err := os.Create(s.prof + "-" + strings.ReplaceAll(p, " ", "_") + ".pb.gz"); err == nil {
			pprof.Lookup("heap").WriteTo(f, 0)
			f.Close()
		}
	}
}

func (s *memSampler) result() map[string]any {
	if s == nil {
		return nil
	}
	close(s.stop)
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"peak_by_rss": s.peak, "marks": s.marks,
		"note": "sampled every 200 ms; anon_minus_go ≈ freshDB's off-heap value-cache arena + cgo; page cache for pread'd data files is not in RSS for any engine"}
}

var memNames = []string{"/memory/classes/total:bytes", "/memory/classes/heap/released:bytes",
	"/memory/classes/heap/objects:bytes", "/memory/classes/heap/free:bytes", "/gc/heap/goal:bytes"}

func (s *memSampler) sample() MemSample {
	ms := make([]metrics.Sample, len(memNames))
	for i, n := range memNames {
		ms[i].Name = n
	}
	metrics.Read(ms)
	u := func(i int) int64 { return int64(ms[i].Value.Uint64()) >> 20 }
	st := procStatus()
	m := MemSample{RSSMiB: st["VmRSS"], AnonMiB: st["RssAnon"], FileMiB: st["RssFile"],
		GoResMiB: u(0) - u(1), HeapObjMiB: u(2), HeapFreeMiB: u(3), GoalMiB: u(4)}
	m.OffHeapMiB = m.AnonMiB - m.GoResMiB
	s.mu.Lock()
	m.Phase = s.phase
	s.mu.Unlock()
	return m
}

func procStatus() map[string]int64 {
	out := map[string]int64{}
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return out
	}
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) >= 3 && f[2] == "kB" {
			v, _ := strconv.ParseInt(f[1], 10, 64)
			out[strings.TrimSuffix(f[0], ":")] = v >> 10
		}
	}
	return out
}

var _ = fmt.Sprint

// pageCacheMiB is how much of dir's files is in the OS page cache (mincore
// over a read-only mapping of each file). It is shared and reclaimable, and
// not in any engine's RSS when the engine reads with pread.
func pageCacheMiB(dir string) int64 {
	var pages int64
	filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return nil
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil || st.Size() == 0 {
			return nil
		}
		m, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			return nil
		}
		defer unix.Munmap(m)
		ps := os.Getpagesize()
		vec := make([]byte, (len(m)+ps-1)/ps)
		if _, _, e := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&m[0])), uintptr(len(m)), uintptr(unsafe.Pointer(&vec[0]))); e == 0 {
			for _, v := range vec {
				pages += int64(v & 1)
			}
		}
		return nil
	})
	return pages * int64(os.Getpagesize()) >> 20
}
