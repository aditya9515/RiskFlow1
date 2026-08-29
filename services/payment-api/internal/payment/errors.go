package payment

import (
	"errors"
	"fmt"
)

var (
	ErrIdempotencyKeyRequired = errors.New("idempotency key is required")
	ErrIdempotencyKeyInvalid  = errors.New("idempotency key is invalid")
	ErrIdempotencyConflict    = errors.New("idempotency key was already used with a different request")
)

// ValidationError reports domain validation failures by request field.
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("payment validation failed for %d field(s)", len(e.Fields))
}
