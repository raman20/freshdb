package freshdb

import (
	"context"
	"fmt"
)

// Change notification for consumers (docs/design/06-push-consumers.md,
// spec/FreshNotify.tla). db.chg is created lazily by a waiter and closed by
// bump whenever a branch head may have moved, so commits with nobody
// waiting cost one uncontended mutex.

// changed returns the channel that the next bump closes.
func (db *DB) changed() <-chan struct{} {
	db.chgMu.Lock()
	defer db.chgMu.Unlock()
	if db.chg == nil {
		db.chg = make(chan struct{})
	}
	return db.chg
}

// bump wakes every waiter. Call it after the change is visible.
func (db *DB) bump() {
	db.chgMu.Lock()
	if db.chg != nil {
		close(db.chg)
		db.chg = nil
	}
	db.chgMu.Unlock()
}

// WaitHead blocks until the head LSN of branch differs from seen (a commit,
// or a rewind), and returns the new head. It returns ctx.Err() with the
// current head when ctx ends, ErrClosed once the DB is closed, and
// ErrNoBranch if the branch does not exist. Pass the head you last read
// (for example tx.At()) to wait for anything newer.
func (db *DB) WaitHead(ctx context.Context, branch string, seen uint64) (uint64, error) {
	for {
		ch := db.changed() // before reading the head: no lost wake-up
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return 0, ErrClosed
		}
		id, ok := db.man.Refs[branch]
		var h uint64
		if ok {
			h = db.headOf(id)
		}
		db.mu.RUnlock()
		if !ok {
			return 0, fmt.Errorf("%w: %q", ErrNoBranch, branch)
		}
		if h != seen {
			return h, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return h, ctx.Err()
		}
	}
}

// Head returns the head LSN of a branch.
func (db *DB) Head(branch string) (uint64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	id, ok := db.man.Refs[branch]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrNoBranch, branch)
	}
	return db.headOf(id), nil
}
