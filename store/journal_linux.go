package store

import (
	"os"

	"golang.org/x/sys/unix"
)

func datasync(f *os.File) error { return unix.Fdatasync(int(f.Fd())) }

// writeOut starts and waits for writeback of [off, off+n) (data only), so a
// later fdatasync finds those pages clean.
func writeOut(f *os.File, off, n int64) {
	unix.SyncFileRange(int(f.Fd()), off, n,
		unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
}

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
