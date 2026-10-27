//go:build unix && !linux

package store

import "os"

func datasync(f *os.File) error { return f.Sync() }

func punch(f *os.File, off, n int64) error { return zeroRange(f, off, n) }
