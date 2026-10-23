package pacelock

import (
	"errors"
	"fmt"
)

// Outcome describes whether a failed operation is known not to have charged.
// It never overrides uncertainty from an earlier attempt using the same ID.
type Outcome string

const (
	// NotCharged means this invocation is known not to have mutated quota.
	NotCharged Outcome = "not_charged"
	// Unknown means a debit (including partial debit) cannot be ruled out.
	Unknown Outcome = "unknown"
)

// Error provides operation and accounting context while preserving errors.Is/As.
// On any error, ignore the returned decision. Unknown must not trigger a blind
// retry of Allow or an automatic refund.
type Error struct {
	Op      string
	Outcome Outcome
	Err     error
}

func (e *Error) Error() string { return fmt.Sprintf("pacelock: %s (%s): %v", e.Op, e.Outcome, e.Err) }
func (e *Error) Unwrap() error { return e.Err }

// OutcomeOf returns Unknown for errors that do not carry SDK outcome metadata.
// Call it only when err is non-nil; nil also returns Unknown.
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
