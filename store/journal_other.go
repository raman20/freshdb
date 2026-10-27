//go:build !unix

package store

import "errors"

// Journal is only available on Unix-like systems.
type Journal struct{ FS }

// NewJournal reports that the journal store is unsupported here.
func NewJournal(string) (*Journal, error) {
	return nil, errors.New("store: journal store needs a Unix-like OS")
}
