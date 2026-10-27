// Package pacelock provides immediate, weighted, multidimensional GCRA admission.
// Drivers own backend time, atomic persistence and retained admission records.
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
	// MaxHorizon bounds periods, burst recovery and admission retention.
	MaxHorizon = 24 * time.Hour
)

var (
	// ErrInvalid reports invalid input before dispatch.
	ErrInvalid = errors.New("pacelock: invalid argument")
	// ErrPolicyMismatch reports a policy conflict with active quota state.
	ErrPolicyMismatch = errors.New("pacelock: policy differs from active backend state")
	// ErrState reports malformed state or replies; inspect OutcomeOf.
	ErrState = errors.New("pacelock: invalid backend state")
	// ErrConflict reports reuse of a retained ID with different parameters.
	ErrConflict = errors.New("pacelock: admission ID used with different parameters")
	// ErrOutcomeUnknown reports an admission whose debit cannot be established.
	ErrOutcomeUnknown = errors.New("pacelock: admission outcome unknown")
	// ErrBackend wraps an unclassified backend or transport failure.
	ErrBackend = errors.New("pacelock: backend failure")
)

// Rule charges Cost units to Name. Rate units replenish per Period.
// Burst is total instantaneous capacity, including the first unit.
type Rule struct {
	// Name identifies a quota within a scope; it contains 1..256 bytes.
	Name string
	// Rate is the positive number of units replenished per Period.
	Rate int64
	// Period is positive and at most MaxHorizon.
	Period time.Duration
	// Burst is positive; its effective refill time is at most MaxHorizon.
	Burst int64
	// Cost is the known charge for this attempt, from 1 through Burst.
	Cost int64
}

// Interval validates the rule and returns its per-unit recovery interval, rounded
// upward to whole microseconds. Drivers must use this effective interval.
func (r Rule) Interval() (time.Duration, error) {
	if r.Name == "" || len(r.Name) > 256 || r.Rate <= 0 || r.Period <= 0 || r.Period > MaxHorizon || r.Burst <= 0 || r.Cost <= 0 || r.Cost > r.Burst {
		return 0, fmt.Errorf("%w: invalid quota %q", ErrInvalid, r.Name)
	}
	us := (r.Period.Nanoseconds() + 999) / 1000
	step := us / r.Rate
	if us%r.Rate != 0 {
		step++
	}
	if r.Burst > MaxHorizon.Microseconds()/step {
		return 0, fmt.Errorf("%w: burst refill horizon exceeds 24 hours for %q", ErrInvalid, r.Name)
	}
	return time.Duration(step) * time.Microsecond, nil
}

// Validate checks 1..MaxRules rules with unique names without backend access.
// It cannot detect conflicts with active backend policies.
func Validate(rules ...Rule) error {
	if len(rules) == 0 || len(rules) > MaxRules {
		return fmt.Errorf("%w: require 1..%d rules", ErrInvalid, MaxRules)
	}
	seen := make(map[string]bool, len(rules))
	for _, r := range rules {
		if _, err := r.Interval(); err != nil {
			return err
		}
		if seen[r.Name] {
			return fmt.Errorf("%w: duplicate quota %q", ErrInvalid, r.Name)
		}
		seen[r.Name] = true
	}
	return nil
}

// Decision describes one admission. On errors, ignore the returned decision.
type Decision struct {
	// Allowed is true when all quota charges were accepted.
	Allowed bool
	// RetryAfter is advisory, not a reservation. Retained results preserve the original wait.
	RetryAfter time.Duration
	// Limited lists every blocking quota in input order; it is empty on allowance.
	Limited []string
}

// Receipt contains an original decision and its replay status. Replayed admission
// does not authorize executing upstream work twice.
type Receipt struct {
	// Decision is the original allow/deny result; ignore it on error.
	Decision
	// Replayed reports reuse of a retained allow or deny decision.
	Replayed bool
}

// Limiter validates and observes operations on a caller-owned, concurrency-safe
// Driver. It does not connect, close the driver or create background goroutines.
type Limiter struct {
	driver   Driver
	scope    Scope
	observer Observer
}

// New constructs a limiter in a namespace/domain scope without backend access.
// Namespace isolates applications; domain groups quotas for atomic admission.
func New(driver Driver, namespace, domain string) (*Limiter, error) {
	return NewWithOptions(driver, namespace, domain, Options{})
}

// NewWithOptions constructs a limiter with optional synchronous observation.
func NewWithOptions(driver Driver, namespace, domain string, options Options) (*Limiter, error) {
	scope := Scope{Namespace: namespace, Domain: domain}
	if isNil(driver) {
		return nil, fmt.Errorf("%w: driver is required", ErrInvalid)
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return &Limiter{driver: driver, scope: scope, observer: options.Observer}, nil
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

func (l *Limiter) ready(ctx context.Context) error {
	if ctx == nil || l == nil || isNil(l.driver) {
		return fmt.Errorf("%w: initialized limiter and non-nil context required", ErrInvalid)
	}
	return ctx.Err()
}

// Allow immediately attempts all charges; a normal denial debits none. It does
// not wait for quota recovery. Backend synchronization may wait until ctx expires.
// It never retries an uncertain operation. On any error ignore Decision.
func (l *Limiter) Allow(ctx context.Context, rules ...Rule) (decision Decision, err error) {
	finish := l.observe(ctx, "allow", len(rules))
	defer func() { finish(decision, false, err) }()
	r, err := l.admit(ctx, "", 0, rules)
	return r.Decision, err
}

// AllowOnce retains both allow and deny results for an ID. Retention is whole
// milliseconds in [1ms,24h]; replay never extends it. Rules are ordered and their
// names must be UTF-8. Persist the attempt ID before dispatch. Each genuinely new
// business attempt needs a new ID. On errors ignore Receipt and inspect OutcomeOf.
func (l *Limiter) AllowOnce(ctx context.Context, id string, retention time.Duration, rules ...Rule) (receipt Receipt, err error) {
	finish := l.observe(ctx, "allow_once", len(rules))
	defer func() { finish(receipt.Decision, receipt.Replayed, err) }()
	if id == "" {
		return Receipt{}, failure("allow_once", NotCharged, fmt.Errorf("%w: ID is required", ErrInvalid))
	}
	return l.admit(ctx, id, retention, rules)
}

func (l *Limiter) admit(ctx context.Context, id string, retention time.Duration, rules []Rule) (Receipt, error) {
	op := "allow"
	if id != "" {
		op = "allow_once"
	}
	if err := l.ready(ctx); err != nil {
		return Receipt{}, failure(op, NotCharged, err)
	}
	request := Admission{Scope: l.scope, Rules: append([]Rule(nil), rules...), ID: id, Retention: retention}
	if err := request.Validate(); err != nil {
		return Receipt{}, failure(op, NotCharged, err)
	}
	result, err := l.driver.Admit(ctx, request)
	if err != nil {
		return Receipt{}, driverFailure(op, err)
	}
	if err := validateDecision(result.Decision, rules); err != nil {
		return Receipt{}, failure(op, Unknown, err)
	}
	if id == "" && result.Replayed {
		return Receipt{}, failure(op, Unknown, fmt.Errorf("%w: replay of ordinary admission", ErrState))
	}
	return result, nil
}

// Lookup returns a retained complete decision without charging or extending its
// retention. A missing result does not prove no debit: the original operation may
// still be in flight or its record may have expired or been lost. Drivers route
// recovery to the authoritative primary. Use Decision only when found and err==nil.
func (l *Limiter) Lookup(ctx context.Context, id string) (decision Decision, found bool, err error) {
	finish := l.observe(ctx, "lookup", 0)
	defer func() { finish(decision, found, err) }()
	if err := l.ready(ctx); err != nil {
		return Decision{}, false, failure("lookup", NotCharged, err)
	}
	request := LookupRequest{Scope: l.scope, ID: id}
	if err := request.Validate(); err != nil {
		return Decision{}, false, failure("lookup", NotCharged, err)
	}
	decision, found, err = l.driver.Lookup(ctx, request)
	if err != nil {
		return Decision{}, false, driverFailure("lookup", err)
	}
	if found {
		if err := validateDecision(decision, nil); err != nil {
			return Decision{}, false, failure("lookup", Unknown, err)
		}
	}
	if !found {
		decision = Decision{}
	}
	return decision, found, nil
}

func validateDecision(d Decision, rules []Rule) error {
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
		if rules != nil {
			for position < len(rules) && rules[position].Name != name {
				position++
			}
			if position == len(rules) {
				return bad()
			}
			position++
		}
	}
	return nil
}
