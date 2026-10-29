//go:build unix && !linux

package store

import "os"

func datasync(f *os.File) error { return f.Sync() }

func punch(f *os.File, off, n int64) error { return zeroRange(f, off, n) }

func writeOut(*os.File, int64, int64) {}

func nextData(_ *os.File, off int64) int64 { return off }

func nextHole(*os.File, int64) int64 { return -1 } // punch never makes holes here
