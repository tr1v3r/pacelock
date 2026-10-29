package pacelock

import (
	"context"
	"time"
)

// Options holds optional integrations; zero values are valid.
type Options struct {
	// Observer is nil to disable synchronous observation.
	Observer Observer
}

// Observer receives one synchronous event per Allow or Wait call.
// Constructors and UpdateQuotas emit no event. The callback must be fast and
// concurrency-safe.
// Panics propagate; no background work is created.
type Observer func(context.Context, Event)

// Event describes a call without exposing quota names as fields.
// Err may contain identifiers and must not be used as a metric label.
type Event struct {
	// Operation is allow or wait.
	// Each Wait emits one event for the whole call.
	Operation string
	// Duration excludes the observer callback execution time.
	Duration time.Duration
	// RuleCount includes invalid supplied rules or charges.
	RuleCount int
	// Result is allowed, denied or error.
	Result string
	// RetryAfter is the last decision advisory wait (zero after allowance).
	RetryAfter time.Duration
	// Err is the returned error; do not label metrics with its text.
	Err error
}

func (l *limiterCore) observe(ctx context.Context, op string, count int) func(Decision, error) {
	if l == nil || l.observer == nil {
		return func(Decision, error) {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	return func(d Decision, err error) {
		result := "denied"
		if d.Allowed {
			result = "allowed"
		}
		if err != nil {
			result = "error"
		}
		l.observer(ctx, Event{Operation: op, Duration: time.Since(start), RuleCount: count, Result: result, RetryAfter: d.RetryAfter, Err: err})
	}
}
