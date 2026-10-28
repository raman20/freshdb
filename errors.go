// Package freshdb is a single-writer storage engine where one append-only log on
// object storage (or a local directory) is the only source of truth, and
// key-value, topics/consumer groups, documents, secondary indexes and vectors
// are "lenses": pure folds over that log.
package freshdb

import (
	"errors"

	"github.com/raman20/freshdb/format"
)

var (
	ErrNotFound       = errors.New("freshdb: not found")
	ErrConflict       = errors.New("freshdb: transaction conflict (serializability); retry")
	ErrFenced         = errors.New("freshdb: fenced: another writer took over this database")
	ErrUnknownOutcome = errors.New("freshdb: commit outcome unknown (storage error); reopen to learn whether it committed")
	ErrShredded       = errors.New("freshdb: subject has been crypto-shredded")
	ErrClosed         = errors.New("freshdb: database closed")
	ErrCorrupt        = errors.New("freshdb: corrupt log")
	ErrReadOnly       = errors.New("freshdb: read-only transaction")
	ErrBranchExists   = errors.New("freshdb: branch already exists")
	ErrNoBranch       = errors.New("freshdb: no such branch")
	ErrNotFastForward = errors.New("freshdb: promote is not a fast-forward")
	ErrBadLSN         = errors.New("freshdb: LSN out of range")
	ErrMainBranch     = errors.New("freshdb: branch main cannot be deleted")
	// ErrCompacted: the requested history (time travel, branch point,
	// rewind target) is older than the log's compaction base.
	ErrCompacted = errors.New("freshdb: history before the compaction base was compacted away")
	// ErrWrongKEK: the key-encryption key does not match the one this
	// database was created with.
	ErrWrongKEK = errors.New("freshdb: wrong key-encryption key for this database")
)

// Op re-exports the log op type for advanced users (Entries).
type Op = format.Op
