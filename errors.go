package pacelock

import (
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
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
	// Op identifies the invocation: allow, allow_once, or lookup.
	Op string
	// Outcome describes accounting certainty; it does not resolve earlier ambiguous attempts.
	Outcome Outcome
	// Err is the underlying cause, available through errors.Is and errors.As.
	Err error
}

// Error formats the operation, outcome and underlying cause.
func (e *Error) Error() string { return fmt.Sprintf("pacelock: %s (%s): %v", e.Op, e.Outcome, e.Err) }

// Unwrap exposes the underlying cause for errors.Is and errors.As.
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

// failure attaches accounting metadata without discarding error identity.
func failure(op string, outcome Outcome, err error) error {
	return &Error{Op: op, Outcome: outcome, Err: err}
}

// scriptFailure recognizes SDK pre-write errors and conservatively classifies all other failures.
func scriptFailure(op string, err error) error {
	// Only exact SDK-owned pre-write error tokens are known not to debit.
	var server redis.Error
	token := ""
	if errors.As(err, &server) {
		token, _, _ = strings.Cut(strings.TrimPrefix(server.Error(), "ERR "), " ")
	}
	switch token {
	case "PACE_POLICY":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrPolicyMismatch, err))
	case "PACE_STATE":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrState, err))
	case "PACE_CONFLICT":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrConflict, err))
	case "PACE_PENDING":
		return failure(op, Unknown, fmt.Errorf("%w: %w", ErrOutcomeUnknown, err))
	case "PACE_RECORD":
		return failure(op, Unknown, fmt.Errorf("%w: %w", ErrState, err))
	default:
		return failure(op, Unknown, fmt.Errorf("%w: %w", ErrBackend, err))
	}
}
