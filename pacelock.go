// Package pacelock provides weighted, multidimensional GCRA admission using
// backend-owned dynamic policies, with immediate checks and context-aware waits.
// Drivers own backend time and atomic persistence.
// The package does not queue work, refund capacity, execute upstream calls or bill usage.
package pacelock

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

const (
	// MaxRules bounds the work performed by one atomic admission.
	MaxRules = 32
	// MaxHorizon bounds periods and burst recovery.
	MaxHorizon = 24 * time.Hour
	// MaxRevision is the largest policy revision represented exactly by Redis Lua.
	MaxRevision int64 = 9007199254740991
)

var (
	// ErrInvalid reports invalid input, including a cost above the current backend burst.
	ErrInvalid = errors.New("pacelock: invalid argument")
	// ErrPolicyMismatch reports a stale or conflicting quota policy update,
	// or a policy mismatch detected by the offline math helpers.
	ErrPolicyMismatch = errors.New("pacelock: policy differs from active backend state")
	// ErrState reports malformed state or replies; inspect OutcomeOf.
	ErrState = errors.New("pacelock: invalid backend state")
	// ErrBackend wraps an unclassified backend or transport failure.
	ErrBackend = errors.New("pacelock: backend failure")
	// ErrPaused rejects a fresh admission until the managed policy is resumed.
	ErrPaused = errors.New("pacelock: quota is paused")
	// ErrPolicyMissing rejects default dynamic admission without backend configuration.
	ErrPolicyMissing = errors.New("pacelock: quota policy is not configured")
	// ErrUnsupported reports a driver without the requested optional capability.
	ErrUnsupported = errors.New("pacelock: operation is unsupported")
)

// Outcome describes accounting certainty for a failed invocation. It does not
// resolve uncertainty from an earlier invocation of the same admission.
type Outcome string

const (
	// NotCharged means this invocation is known not to have debited quota.
	NotCharged Outcome = "not_charged"
	// Unknown means a debit cannot be ruled out, including backend-specific partial writes.
	Unknown Outcome = "unknown"
)

// Error preserves the underlying error chain and reports operation/accounting context.
type Error struct {
	// Op identifies allow, wait or update_quotas (and driver maintenance operations).
	Op string
	// Outcome reports accounting certainty for this invocation.
	Outcome Outcome
	// Err preserves the underlying cause for errors.Is and errors.As.
	Err error
}

// Error formats operation, outcome and underlying cause.
func (e *Error) Error() string { return fmt.Sprintf("pacelock: %s (%s): %v", e.Op, e.Outcome, e.Err) }

// Unwrap exposes the underlying error chain.
func (e *Error) Unwrap() error { return e.Err }

// OutcomeOf returns Unknown for untyped errors and nil; call only on actual errors.
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

func driverFailure(op string, err error) error {
	var typed *Error
	if !errors.As(err, &typed) {
		err = fmt.Errorf("%w: %w", ErrBackend, err)
	}
	return failure(op, OutcomeOf(err), err)
}

// Decision describes one admission. On errors, ignore the returned decision.
type Decision struct {
	// Allowed is true when all quota charges were accepted.
	Allowed bool
	// RetryAfter is advisory, not a reservation.
	RetryAfter time.Duration
	// Limited lists every blocking quota in charge/rule order. It is empty on allowance.
	Limited []string
}

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

// Limiter admits charges using current backend-owned dynamic policies.
// It is safe for concurrent use with a concurrency-safe Driver. It does not
// connect, close the driver or create background goroutines. Construct with New
// or NewWithOptions; nil and zero receivers return ErrInvalid.
type Limiter struct {
	core *limiterCore
}

// limiterCore holds transport, scope and observation for admission and updates.
type limiterCore struct {
	driver   Driver
	scope    Scope
	observer Observer
}

// New constructs a dynamic limiter in a namespace/domain scope without backend access.
// Namespace isolates applications; domain groups quotas for atomic admission.
func New(driver Driver, namespace, domain string) (*Limiter, error) {
	return NewWithOptions(driver, namespace, domain, Options{})
}

// NewWithOptions constructs a limiter with optional synchronous observation.
func NewWithOptions(driver Driver, namespace, domain string, options Options) (*Limiter, error) {
	core, err := newLimiterCore(driver, namespace, domain, options)
	if err != nil {
		return nil, err
	}
	return &Limiter{core: core}, nil
}

func newLimiterCore(driver Driver, namespace, domain string, options Options) (*limiterCore, error) {
	scope := Scope{Namespace: namespace, Domain: domain}
	if isNil(driver) {
		return nil, fmt.Errorf("%w: driver is required", ErrInvalid)
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return &limiterCore{driver: driver, scope: scope, observer: options.Observer}, nil
}

func isNil(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	}
	return false
}

func (l *Limiter) engine() *limiterCore {
	if l == nil {
		return nil
	}
	return l.core
}

func (l *limiterCore) ready(ctx context.Context) error {
	if ctx == nil || l == nil || isNil(l.driver) {
		return fmt.Errorf("%w: initialized limiter and non-nil context required", ErrInvalid)
	}
	return ctx.Err()
}

func validateDecision(d Decision, charges []Charge) error {
	bad := func() error { return fmt.Errorf("%w: invalid driver decision", ErrState) }
	if d.RetryAfter < 0 || d.RetryAfter > MaxHorizon || len(d.Limited) > MaxRules {
		return bad()
	}
	if d.Allowed {
		if d.RetryAfter != 0 || len(d.Limited) != 0 {
			return bad()
		}
		return nil
	}
	if d.RetryAfter == 0 || len(d.Limited) == 0 {
		return bad()
	}
	seen := map[string]bool{}
	position := 0
	for _, name := range d.Limited {
		if name == "" || len(name) > 256 || seen[name] {
			return bad()
		}
		seen[name] = true
		if charges != nil {
			for position < len(charges) && charges[position].Name != name {
				position++
			}
			if position == len(charges) {
				return bad()
			}
			position++
		}
	}
	return nil
}

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
// Wait uses the calling goroutine. It creates no queue,
// reservation or background goroutine and guarantees no fairness. Contention
// can delay admission indefinitely; use a deadline to bound waiting.
// It emits one observer event.
func (l *Limiter) Wait(ctx context.Context, charges ...Charge) (err error) {
	core := l.engine()
	finish := core.observe(ctx, "wait", len(charges))
	var decision Decision
	defer func() { finish(decision, err) }()
	charges = append([]Charge(nil), charges...)
	decision, err = waitAdmission(ctx, "wait", func() (Decision, error) {
		return core.admitManaged(ctx, charges)
	})
	return err
}

// waitAdmission retries only validated denials. Attempts validate context before
// dispatch and return a validated decision or an accounting-aware error. Keep
// the last decision for whole-call observation, including a canceled delay.
func waitAdmission(ctx context.Context, op string, attempt func() (Decision, error)) (decision Decision, err error) {
	for {
		result, attemptErr := attempt()
		if attemptErr != nil {
			return decision, failure(op, OutcomeOf(attemptErr), attemptErr)
		}
		decision = result
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
