//go:build !unix

package store

import "errors"

// Journal is only available on Unix-like systems.
type Journal struct{ FS }

// NewJournal reports that the journal store is unsupported here.
func NewJournal(string) (*Journal, error) {
	return nil, errors.New("store: journal store needs a Unix-like OS")
}

// ShrinkStats reports what ShrinkJournal did.
type ShrinkStats struct {
	Objects                                        int
	SizeBefore, AllocBefore, SizeAfter, AllocAfter int64
	DeadRecsBefore                                 int
}

// ShrinkJournal reports that the journal store is unsupported here.
func ShrinkJournal(string) (ShrinkStats, error) {
	return ShrinkStats{}, errors.New("store: journal store needs a Unix-like OS")
}
