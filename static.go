package pacelock

import (
	"context"
	"fmt"
)

// Quota associates an independent quota name with its policy. Static quotas have
// Revision zero; UpdateQuotas requires a positive managed revision.
type Quota struct {
	// Name identifies shared state within the domain and must contain 1..256 bytes.
	Name string
	// Policy specifies how this quota replenishes and how much it can accumulate.
	Policy Policy
}

func (q Quota) rule(cost int64) Rule {
	return Rule{Name: q.Name, Policy: q.Policy, Cost: cost}
}

// StaticLimiter admits charges using an immutable, ordered set of caller-owned
// quota policies. Every admission must charge a nonempty subset exactly once.
// Charges may arrive in any order; decisions use quota order.
// It is safe for concurrent use with a concurrency-safe Driver. Construct with
// NewStatic or NewStaticWithOptions; nil and zero receivers return ErrInvalid.
// It does not own the driver, connect, close it or start background goroutines.
type StaticLimiter struct {
	core *limiterCore
	// rules holds copied policies in construction order with placeholder costs.
	rules []Rule
	// index maps each quota name to its immutable position in rules.
	index map[string]int
}

// NewStatic validates and copies 1..MaxRules uniquely named static quotas in a
// namespace/domain scope. It performs no backend access or policy registration.
// Policies must have Revision zero. Invalid inputs wrap ErrInvalid; active
// backend policy conflicts are detected at admission, not construction.
// Include all applicable account/model/tenant quotas; omissions cannot be inferred.
// Construct another StaticLimiter to change policies or select another quota set.
func NewStatic(driver Driver, namespace, domain string, quotas ...Quota) (*StaticLimiter, error) {
	return NewStaticWithOptions(driver, namespace, domain, Options{}, quotas...)
}

// NewStaticWithOptions constructs a static limiter with synchronous observation.
// Like NewStatic it copies policies and performs no backend access. A driver
// without StaticDriver can be constructed, but admission returns ErrUnsupported
// with NotCharged.
func NewStaticWithOptions(driver Driver, namespace, domain string, options Options, quotas ...Quota) (*StaticLimiter, error) {
	core, err := newLimiterCore(driver, namespace, domain, options)
	if err != nil {
		return nil, err
	}
	if len(quotas) == 0 || len(quotas) > MaxRules {
		return nil, fmt.Errorf("%w: require 1..%d quotas", ErrInvalid, MaxRules)
	}
	rules := make([]Rule, len(quotas))
	for i, q := range quotas {
		rules[i] = q.rule(1)
	}
	if err := ValidateStatic(rules...); err != nil {
		return nil, err
	}
	index := make(map[string]int, len(rules))
	for i, r := range rules {
		index[r.Name] = i
	}
	return &StaticLimiter{core: core, rules: rules, index: index}, nil
}

// Allow immediately checks and charges a nonempty subset of configured quotas,
// or denies without charging any. Each name must appear once with 0 < Cost <=
// Burst. Empty, duplicate, unknown or invalid charges return ErrInvalid with
// NotCharged before dispatch. Omitted quotas are neither checked nor charged.
// Separate calls are independent: a later denial never rolls back an earlier debit.
// It never waits for quota recovery or queues work; backend synchronization may
// wait until ctx expires. After dispatch an error may have an Unknown debit
// outcome. Ignore Decision on error and inspect OutcomeOf.
func (s *StaticLimiter) Allow(ctx context.Context, charges ...Charge) (decision Decision, err error) {
	core := s.engine()
	finish := core.observe(ctx, "allow_static", len(charges))
	defer func() { finish(decision, err) }()
	rules, err := s.chargedRules(ctx, charges)
	if err != nil {
		return Decision{}, failure("allow_static", NotCharged, err)
	}
	return core.admitStatic(ctx, rules)
}

// Wait blocks until all selected charges are atomically accepted or an error
// occurs. It snapshots the selected charges and fixed policies before dispatch.
// A nil error means quota was consumed once, including if ctx was canceled after
// a confirmed debit. Business failure does not refund that consumption.
// Only ordinary quota denials are retried, after their advisory RetryAfter.
// Every backend error stops waiting, including NotCharged errors; Unknown must
// not be blindly retried. Cancellation between attempts is NotCharged, while
// cancellation during dispatch preserves the driver's accounting outcome.
// Wait uses the calling goroutine. It creates no queue,
// reservation or background goroutine and guarantees no fairness. Contention
// can delay admission indefinitely; use a deadline to bound waiting.
// It emits one observer event.
func (s *StaticLimiter) Wait(ctx context.Context, charges ...Charge) (err error) {
	core := s.engine()
	finish := core.observe(ctx, "wait_static", len(charges))
	var decision Decision
	defer func() { finish(decision, err) }()
	rules, err := s.chargedRules(ctx, charges)
	if err != nil {
		return failure("wait_static", NotCharged, err)
	}
	decision, err = waitAdmission(ctx, "wait_static", func() (Decision, error) {
		return core.admitStatic(ctx, rules)
	})
	return err
}

func (s *StaticLimiter) engine() *limiterCore {
	if s == nil {
		return nil
	}
	return s.core
}

// chargedRules validates a nonempty subset and assembles a per-call snapshot of
// selected rules in construction order. Caller and limiter data are not mutated.
func (s *StaticLimiter) chargedRules(ctx context.Context, charges []Charge) ([]Rule, error) {
	if err := s.engine().ready(ctx); err != nil {
		return nil, err
	}
	if len(charges) == 0 || len(charges) > len(s.rules) {
		return nil, fmt.Errorf("%w: require a nonempty subset of configured quotas", ErrInvalid)
	}
	costs := make([]int64, len(s.rules))
	for _, charge := range charges {
		i, ok := s.index[charge.Name]
		if !ok {
			return nil, fmt.Errorf("%w: unknown quota %q", ErrInvalid, charge.Name)
		}
		if costs[i] != 0 {
			return nil, fmt.Errorf("%w: duplicate charge for quota %q", ErrInvalid, charge.Name)
		}
		if charge.Cost <= 0 || charge.Cost > s.rules[i].Policy.Burst {
			return nil, fmt.Errorf("%w: invalid cost for quota %q", ErrInvalid, charge.Name)
		}
		costs[i] = charge.Cost
	}
	rules := make([]Rule, 0, len(charges))
	for i, rule := range s.rules {
		if costs[i] != 0 {
			rule.Cost = costs[i]
			rules = append(rules, rule)
		}
	}
	return rules, nil
}
