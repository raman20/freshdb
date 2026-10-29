package server

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/raman20/freshdb"
)

// Retryable conditions must not surface as 500.
func TestFailCodes(t *testing.T) {
	for err, want := range map[error]int{
		freshdb.ErrBusy:                      409,
		fmt.Errorf("x: %w", freshdb.ErrBusy): 409,
		freshdb.ErrFenced:                    503,
		freshdb.ErrNotFound:                  404,
		freshdb.ErrTooLarge:                  413,
		fmt.Errorf("%w: bad", errBad):        400,
	} {
		w := httptest.NewRecorder()
		fail(w, err)
		if w.Code != want {
			t.Errorf("%v: %d, want %d", err, w.Code, want)
		}
	}
}
