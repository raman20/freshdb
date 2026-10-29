package freshdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/raman20/freshdb/store"
)

// Read-path faults (a flipped bit, a misdirected read that returns another
// object) are transient: the stored object is fine and a re-read returns
// it. Decisions that act on damage (refuse with ErrCorrupt, repair a tail,
// report "fenced") must not rest on one read, so the log reads through
// getStable: a read that looks wrong is re-read, and damage is believed
// only when three consecutive reads return the same bytes.

// readVerdict classifies one read of an object.
type readVerdict int

const (
	readOK      readVerdict = iota // the object is what the caller expected
	readDamaged                    // bad bytes; believed once reads agree
	readMisread                    // a well-formed object naming another key: believed only if every read agrees
)

const (
	stableReads = 6 // reads of one object before giving up
	stableAgree = 3 // identical consecutive reads that make damage believable
)

// errUnstable: reads kept disagreeing or kept returning another object.
// Not ErrCorrupt: nothing is known to be damaged; the operation fails and
// may be retried.
var errUnstable = errors.New("freshdb: reads of the same object disagree (storage read faults)")

// getStable returns key's bytes: the first read judged readOK, or bytes
// that several consecutive reads returned identically (damage, or a wrong
// object that is really stored under key), for the caller to judge. judge
// is called on every read; the last call is on the returned bytes.
func getStable(ctx context.Context, st store.Store, key string, judge func([]byte) readVerdict) ([]byte, error) {
	var prev []byte
	same := 0
	for i := 0; i < stableReads; i++ {
		b, err := st.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		v := judge(b)
		if v == readOK {
			return b, nil
		}
		if i > 0 && string(b) == string(prev) {
			same++
		} else {
			same = 1
		}
		if (v == readDamaged && same >= stableAgree) || same == stableReads {
			return b, nil
		}
		prev = b
	}
	return nil, fmt.Errorf("%w: %s", errUnstable, key)
}

// okIf maps a yes/no check to a verdict (a "no" is damage).
func okIf(good bool) readVerdict {
	if good {
		return readOK
	}
	return readDamaged
}
