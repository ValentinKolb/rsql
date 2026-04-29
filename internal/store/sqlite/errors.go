package sqlite

import (
	"errors"
	"fmt"
)

// ErrValidation marks an error from row-value validation (type, range,
// pattern, options, NOT NULL, formula). The service layer translates it
// to HTTP 400 via errors.Is so the wire response stays stable even if the
// underlying message text is reworded.
var ErrValidation = errors.New("validation_failed")

// validationErrf wraps the sentinel with a per-site message. Callers use
// it as a drop-in for fmt.Errorf when the cause is invalid input data.
func validationErrf(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrValidation}, a...)...)
}
