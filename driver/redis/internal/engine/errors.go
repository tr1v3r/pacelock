package engine

import (
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

type Outcome = pacelock.Outcome

const NotCharged = pacelock.NotCharged
const Unknown = pacelock.Unknown

type Error = pacelock.Error

func OutcomeOf(err error) Outcome { return pacelock.OutcomeOf(err) }

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
	case "PACE_COMMIT":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrBackend, err))
	case "PACE_MISSING":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", pacelock.ErrPolicyMissing, err))
	case "PACE_INVALID":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrInvalid, err))
	case "PACE_PAUSED":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", pacelock.ErrPaused, err))
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
