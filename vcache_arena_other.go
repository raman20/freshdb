//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package freshdb

// allocArena falls back to the Go heap where anonymous mmap is not used.
func allocArena(n int) ([]byte, func()) { return make([]byte, n), nil }
