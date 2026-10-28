package freshdb

import (
	"os"
	"strconv"
	"strings"
)

// defaultValueCacheBytes sizes the value cache from the memory available
// to the process: 1/16 of it, at most 256 MiB and at least 16 MiB. On a
// 4 GiB+ machine that is 256 MiB (the earlier fixed default); in a 1 GiB
// container it is 64 MiB. Unknown memory (non-Linux) keeps 256 MiB.
func defaultValueCacheBytes() int {
	return cacheForMem(availableMem())
}

func cacheForMem(mem int64) int {
	const hi, lo = defaultValueCache, 16 << 20
	if mem <= 0 {
		return hi
	}
	return int(min(max(mem/16, lo), hi))
}

// availableMem returns the cgroup v2 memory limit if one is set, else the
// machine's MemTotal, else 0.
func availableMem() int64 {
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil && v > 0 {
			return v
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if f := strings.Fields(l); len(f) >= 2 && f[0] == "MemTotal:" {
				if v, err := strconv.ParseInt(f[1], 10, 64); err == nil {
					return v << 10
				}
			}
		}
	}
	return 0
}
