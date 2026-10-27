package pacelock

import (
	"context"
	"fmt"
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
	var receipt Receipt
	defer func() { finish(receipt.Decision, false, err) }()
	receipt, err = l.wait(ctx, "", 0, rules)
	return err
}

// WaitOnce blocks until the charges are admitted once per retained ID or an error
// occurs. It requires a WaitOnceDriver; otherwise it returns ErrUnsupported /
// NotCharged before dispatch. ID, retention and ordered UTF-8 rules have the same
// bounds as AllowOnce. Persist the ID before dispatch and use Lookup to recover.
// On any error, ignore Receipt and inspect OutcomeOf; no error is retried.
//
// Ordinary denials create no record and can be retried with the same ID. The ID
// and parameters become bound only when an admission succeeds or leaves a pending
// record. Success retention begins at admission, not at the start of waiting.
// A retained success is replayed without charging again or extending retention.
// An existing AllowOnce denial returns ErrConflict instead of being overwritten.
// Never reuse an ID after its retained record expires or is lost.
//
// Waiting, cancellation and fairness follow Wait. A nil error always includes an
// allowed receipt, even when cancellation races with confirmed admission. Replayed
// does not authorize executing business work twice. Concurrent callers can receive
// the same successful receipt; execution coordination belongs to the application.
func (l *Limiter) WaitOnce(ctx context.Context, id string, retention time.Duration, rules ...Rule) (receipt Receipt, err error) {
	finish := l.observe(ctx, "wait_once", len(rules))
	defer func() { finish(receipt.Decision, receipt.Replayed, err) }()
	if id == "" {
		return Receipt{}, failure("wait_once", NotCharged, fmt.Errorf("%w: ID is required", ErrInvalid))
	}
	return l.wait(ctx, id, retention, rules)
}

// wait owns the caller-side retry loop; drivers make immediate atomic decisions.
func (l *Limiter) wait(ctx context.Context, id string, retention time.Duration, rules []Rule) (Receipt, error) {
	op := "wait"
	if id != "" {
		op = "wait_once"
	}
	rules = append([]Rule(nil), rules...)
	for {
		receipt, err := l.admit(ctx, id, retention, rules, id != "")
		if err != nil {
			return Receipt{}, failure(op, OutcomeOf(err), err)
		}
		if receipt.Allowed {
			return receipt, nil
		}

		timer := time.NewTimer(receipt.RetryAfter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return receipt, failure(op, NotCharged, ctx.Err())
		case <-timer.C:
		}
	}
}
