package store

import (
	"os"

	"golang.org/x/sys/unix"
)

func datasync(f *os.File) error { return unix.Fdatasync(int(f.Fd())) }

// punch deallocates [off, off+n) (reads return zeros), or zeroes it where
// the file system cannot punch holes.
func punch(f *os.File, off, n int64) error {
	if n == 0 {
		return nil
	}
	err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, off, n)
	if err == nil {
		return nil
	}
	return zeroRange(f, off, n)
}
