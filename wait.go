package pacelock

import (
	"context"
	"time"
)

// Wait blocks until all charges are atomically accepted or an error occurs.
// A nil error means quota was consumed once, including if ctx was canceled after
// a successful dispatch. Business failure does not refund that consumption.
//
// Only ordinary quota denials are retried, after their advisory RetryAfter.
// Every backend error stops waiting, including NotCharged errors; Unknown must
// not be blindly retried. Cancellation between attempts is NotCharged, while
// cancellation during dispatch preserves the driver's accounting outcome.
//
// Wait uses a snapshot of rules, an empty admission ID and the calling goroutine.
// It creates no queue, reservation or background goroutine and guarantees no
// fairness between callers. Contention can delay admission indefinitely; use a
// context deadline to bound waiting. Wait is not idempotent or recoverable with
// Lookup. AllowOnce retains denials and cannot be polled with one ID to wait.
func (l *Limiter) Wait(ctx context.Context, rules ...Rule) (err error) {
	finish := l.observe(ctx, "wait", len(rules))
	var decision Decision
	defer func() { finish(decision, false, err) }()

	rules = append([]Rule(nil), rules...)
	for {
		receipt, attemptErr := l.admit(ctx, "", 0, rules)
		if attemptErr != nil {
			return failure("wait", OutcomeOf(attemptErr), attemptErr)
		}
		decision = receipt.Decision
		if decision.Allowed {
			return nil
		}

		timer := time.NewTimer(decision.RetryAfter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return failure("wait", NotCharged, ctx.Err())
		case <-timer.C:
		}
	}
}
