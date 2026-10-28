//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package freshdb

import "golang.org/x/sys/unix"

// allocArena returns n bytes outside the Go heap (anonymous mmap; pages are
// committed only when first written) and a function that releases them.
func allocArena(n int) ([]byte, func()) {
	b, err := unix.Mmap(-1, 0, n, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		return make([]byte, n), nil
	}
	return b, func() { unix.Munmap(b) }
}
