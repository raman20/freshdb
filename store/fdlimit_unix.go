//go:build unix

package store

import "syscall"

// fdBudget is a quarter of RLIMIT_NOFILE, clamped to [256, 16384].
func fdBudget() int {
	var r syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &r) != nil {
		return 256
	}
	return int(min(max(r.Cur/4, 256), 16384))
}
