package pacelock

import (
	"errors"
	"fmt"
)

// Outcome describes accounting certainty for a failed invocation. It does not
// resolve uncertainty from an earlier invocation of the same admission.
type Outcome string

const (
	// NotCharged means this invocation is known not to have debited quota.
	NotCharged Outcome = "not_charged"
	// Unknown means a debit cannot be ruled out, including backend-specific partial writes.
	Unknown Outcome = "unknown"
)

// Error preserves the underlying error chain and reports operation/accounting context.
type Error struct {
	// Op identifies allow, wait, allow_static, wait_static or update_quotas.
	Op string
	// Outcome reports accounting certainty for this invocation.
	Outcome Outcome
	// Err preserves the underlying cause for errors.Is and errors.As.
	Err error
}

// Error formats operation, outcome and underlying cause.
func (e *Error) Error() string { return fmt.Sprintf("pacelock: %s (%s): %v", e.Op, e.Outcome, e.Err) }

// Unwrap exposes the underlying error chain.
func (e *Error) Unwrap() error { return e.Err }

// OutcomeOf returns Unknown for untyped errors and nil; call only on actual errors.
func OutcomeOf(err error) Outcome {
	var e *Error
	if errors.As(err, &e) && e != nil && e.Outcome == NotCharged {
		return NotCharged
	}
	return Unknown
}

func failure(op string, outcome Outcome, err error) error {
	return &Error{Op: op, Outcome: outcome, Err: err}
}

func driverFailure(op string, err error) error {
	var typed *Error
	if !errors.As(err, &typed) {
		err = fmt.Errorf("%w: %w", ErrBackend, err)
	}
	return failure(op, OutcomeOf(err), err)
}
