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

// Policy defines replenishment and total burst capacity independently of a
// charge. The same named quota must use the same effective policy throughout
// a domain.
type Policy struct {
	// Revision is 1..MaxRevision for policies installed by UpdateQuotas.
	// Revisions never expire. Zero is supported only by offline math helpers.
	Revision int64
	// Rate is units replenished per Period. Zero pauses a managed policy.
	Rate int64
	// Period is positive and at most MaxHorizon; unit intervals round up to microseconds.
	Period time.Duration
	// Burst is positive total capacity; its effective refill time must not exceed MaxHorizon.
	Burst int64
}

// Rule charges Cost units to the quota named by Name under a caller-owned Policy.
// Burst is total instantaneous capacity, including the first unit. Rule is the
// offline math format, not a backend admission request. Limiter accepts Charge
// values and resolves policies from the backend.
type Rule struct {
	// Name identifies a quota within a scope; it contains 1..256 bytes.
	Name string
	// Policy specifies how the named quota replenishes and how much it can accumulate.
	Policy Policy
	// Cost is the known charge for this attempt, from 1 through Burst.
	Cost int64
}

// Interval validates the rule and returns its per-unit recovery interval, rounded
// upward to whole microseconds. Drivers must use this effective interval.
func (r Rule) Interval() (time.Duration, error) {
	p := r.Policy
	if r.Name == "" || len(r.Name) > 256 || p.Revision < 0 || p.Revision > MaxRevision || p.Rate < 0 || (p.Rate == 0 && p.Revision == 0) || p.Period <= 0 || p.Period > MaxHorizon || p.Burst <= 0 || p.Burst > MaxHorizon.Microseconds() || r.Cost <= 0 || r.Cost > p.Burst {
		return 0, fmt.Errorf("%w: invalid quota %q", ErrInvalid, r.Name)
	}
	if p.Rate == 0 {
		return 0, nil // A managed pause has no finite refill interval.
	}
	us := (p.Period.Nanoseconds() + 999) / 1000
	step := us / p.Rate
	if us%p.Rate != 0 {
		step++
	}
	if p.Burst > MaxHorizon.Microseconds()/step {
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
	// RetryAfter is advisory, not a reservation.
	RetryAfter time.Duration
	// Limited lists every blocking quota in charge/rule order. It is empty on allowance.
	Limited []string
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
