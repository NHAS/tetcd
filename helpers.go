package tetcd

import (
	"errors"

	"github.com/NHAS/tetcd/paths"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

var (
	ErrNoHandle = errors.New("no handle for operation")
	ErrKeysOnly = errors.New("Keys only operation")
	ErrNotDone  = errors.New("operation not done")
)

// Ignore empty responses
func IgnoreEmpty[T any](t T, err error) (T, error) {
	if errors.Is(err, paths.ErrNotFound) {
		return t, nil
	}
	return t, err
}
