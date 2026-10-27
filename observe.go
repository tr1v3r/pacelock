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

// Observer receives one synchronous event per Allow, AllowOnce or Lookup call.
// Bind emits no event. The callback must be fast
// and concurrency-safe. Panics propagate; no background work is created.
type Observer func(context.Context, Event)

// Event describes a call without exposing quota names or admission IDs as fields.
// Err may contain identifiers and must not be used as a metric label.
type Event struct {
	// Operation is allow, allow_once or lookup.
	Operation string
	// Duration excludes the observer callback execution time.
	Duration time.Duration
	// RuleCount includes invalid supplied rules or charges; Lookup reports zero.
	RuleCount int
	// Result is allowed, denied, found, missing or error.
	Result string
	// Replayed is true only for AllowOnce returning a retained decision.
	Replayed bool
	// RetryAfter is the original decision advisory wait.
	RetryAfter time.Duration
	// Err is the returned error; do not label metrics with its text.
	Err error
}

func (l *Limiter) observe(ctx context.Context, op string, count int) func(Decision, bool, error) {
	if l == nil || l.observer == nil {
		return func(Decision, bool, error) {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	return func(d Decision, replayed bool, err error) {
		result := "denied"
		if d.Allowed {
			result = "allowed"
		}
		if op == "lookup" {
			result = "missing"
			if replayed {
				result = "found"
			}
		}
		if err != nil {
			result = "error"
		}
		l.observer(ctx, Event{Operation: op, Duration: time.Since(start), RuleCount: count, Result: result, Replayed: op == "allow_once" && replayed, RetryAfter: d.RetryAfter, Err: err})
	}
}
