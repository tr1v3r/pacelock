package engine

import (
	"context"
	"github.com/tr1v3r/pacelock"
	"time"
)

type Options = pacelock.Options
type Observer = pacelock.Observer
type Event = pacelock.Event

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
