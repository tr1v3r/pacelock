package pacelock

import (
	"context"
	"time"
)

// Wait blocks until all charges are atomically accepted or an error occurs.
// It snapshots the charges before dispatch, but each attempt uses the current
// backend-owned policies. Policy updates do not wake a sleeping Wait; an earlier
// RetryAfter may delay observing a relaxed policy.
//
// Only ordinary quota denials are retried, after their advisory RetryAfter.
// Every error stops waiting, including missing or paused policies, a cost above
// the current burst, and NotCharged backend errors. Unknown outcomes must not be
// blindly retried. Cancellation between attempts is NotCharged; cancellation
// during dispatch preserves the driver's accounting outcome. A nil error means
// quota was consumed once, even if ctx was canceled after a confirmed debit.
// Business failure does not refund that consumption.
//
// Wait uses an empty ID and the calling goroutine. It creates no queue,
// reservation or background goroutine and guarantees no fairness. Contention
// can delay admission indefinitely; use a deadline to bound waiting. Wait is
// neither idempotent nor recoverable with Lookup. It emits one observer event.
func (l *Limiter) Wait(ctx context.Context, charges ...Charge) (err error) {
	core := l.engine()
	finish := core.observe(ctx, "wait", len(charges))
	var decision Decision
	defer func() { finish(decision, false, err) }()
	charges = append([]Charge(nil), charges...)
	decision, err = waitAdmission(ctx, "wait", func() (Receipt, error) {
		return core.admitManaged(ctx, "", 0, charges)
	})
	return err
}

// waitAdmission retries only validated denials. Attempts validate context before
// dispatch and return a validated decision or an accounting-aware error. Keep
// the last decision for whole-call observation, including a canceled delay.
func waitAdmission(ctx context.Context, op string, attempt func() (Receipt, error)) (decision Decision, err error) {
	for {
		receipt, attemptErr := attempt()
		if attemptErr != nil {
			return decision, failure(op, OutcomeOf(attemptErr), attemptErr)
		}
		decision = receipt.Decision
		if decision.Allowed {
			return decision, nil
		}
		timer := time.NewTimer(decision.RetryAfter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return decision, failure(op, NotCharged, ctx.Err())
		case <-timer.C:
		}
	}
}
