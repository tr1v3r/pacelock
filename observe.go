package pacelock

import (
	"context"
	"time"
)

// Options holds optional integrations. Zero values are valid.
type Options struct {
	// Observer receives operation results synchronously; nil disables observation.
	Observer Observer
}

// Observer receives one event per Allow, AllowOnce or Lookup invocation. It runs
// synchronously and must be fast and safe for concurrent use. Panics propagate.
// No quota names, attempt IDs or context values are copied into Event.
// An invalid nil context is reported using context.Background().
type Observer func(context.Context, Event)

// Event is a bounded-cardinality observation suitable for metrics or tracing.
// Result is allowed, denied, found, missing or error. RetryAfter on a replay or
// lookup is the original observation, not a countdown. Err may contain details;
// do not use its text as a metric label.
type Event struct {
	// Operation is allow, allow_once, or lookup.
	Operation string
	// Duration measures invocation time up to the observer call, excluding the callback itself.
	Duration time.Duration
	// RuleCount is the supplied rule count, including invalid input; Lookup reports zero.
	RuleCount int
	// Result is allowed or denied for admission, found or missing for lookup, or error.
	Result string
	// Replayed is true only when AllowOnce returns a retained decision.
	Replayed bool
	// RetryAfter copies the decision wait; retained results preserve the original duration.
	RetryAfter time.Duration
	// Err is the returned error; its text may contain identifiers and is unsuitable as a metric label.
	Err error
}

// observe starts timing and returns a completion hook; absent observers use a no-op hook.
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
