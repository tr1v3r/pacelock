package pacelock

import (
	"context"
	"fmt"
	"time"
)

// Quota associates an independent quota name with its fixed policy.
type Quota struct {
	// Name identifies shared state within the domain and must contain 1..256 bytes.
	Name string
	// Policy specifies how this quota replenishes and how much it can accumulate.
	Policy Policy
}

// Charge captures a quota's name and policy together with its per-attempt cost.
// Construct it with Quota.Charge. A zero Charge is invalid.
type Charge struct {
	quota Quota
	cost  int64
}

// Charge captures a value copy of q with the known consumption for one attempt.
// It does not validate inputs or contact the backend. BoundLimiter.Allow and
// AllowOnce require a valid policy matching the binding's effective interval and
// Burst, and a cost from 1 through Burst. Later changes to q do not affect the charge.
func (q Quota) Charge(cost int64) Charge {
	return Charge{quota: q, cost: cost}
}

func (q Quota) rule(cost int64) Rule {
	return Rule{Name: q.Name, Policy: q.Policy, Cost: cost}
}

// BoundLimiter binds an immutable, ordered quota set to a Limiter. Every admission
// must charge all bound quotas exactly once. Charges may arrive in any order;
// decisions and idempotency fingerprints use binding order, not charge order.
// It is safe for concurrent use under the parent Limiter's driver contract.
// Construct it with Limiter.Bind; nil and zero receivers return ErrInvalid.
type BoundLimiter struct {
	// limiter provides the driver, scope and observer used by every bound operation.
	limiter *Limiter
	// rules is a private copy of policies in binding order; per-call costs are copied.
	rules []Rule
	// index maps each bound name to its position and is read-only after construction.
	index map[string]int
}

// Bind validates and copies 1..MaxRules uniquely named quotas without contacting
// the backend or registering policies there. Changing the input afterward has no effect.
// Errors wrap ErrInvalid. Active backend policy conflicts are detected at admission.
// Bind all applicable account/model/tenant quotas; omitted quotas cannot be inferred.
// Create a new binding to change policies or select another tenant/model quota set.
func (l *Limiter) Bind(quotas ...Quota) (*BoundLimiter, error) {
	if err := l.ready(context.Background()); err != nil {
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
	return &BoundLimiter{limiter: l, rules: rules, index: index}, nil
}

// Allow immediately checks and charges the entire bound quota set or denies
// without charging any. Use Quota.Charge to supply each bound quota exactly once,
// in any order, with 0 < cost <= Burst. Each charge must carry a valid policy with
// the same effective interval and Burst as its bound quota. Missing, duplicate,
// unknown, mismatched or invalid charges return ErrInvalid with NotCharged before
// dispatch. No cost is defaulted or skipped.
// It never waits for quota recovery or queues work; backend synchronization may
// wait until ctx expires. After dispatch an error may have an Unknown debit
// outcome. Ignore Decision on error and inspect OutcomeOf.
func (b *BoundLimiter) Allow(ctx context.Context, charges ...Charge) (decision Decision, err error) {
	l := b.parent()
	finish := l.observe(ctx, "allow", len(charges))
	defer func() { finish(decision, false, err) }()
	rules, err := b.chargedRules(ctx, charges)
	if err != nil {
		return Decision{}, failure("allow", NotCharged, err)
	}
	receipt, err := l.admit(ctx, "", 0, rules)
	return receipt.Decision, err
}

// AllowOnce adds the same bounded deduplication and recovery contract as
// Limiter.AllowOnce. ID and retention have the same bounds. Charges are validated
// as in BoundLimiter.Allow, and quota names must be valid UTF-8. Reordering charges
// does not change a retained attempt; binding order, effective policies, costs and
// retention must still match. Retain the original binding for attempt recovery.
// Both allows and denials are retained. Ignore Receipt on error and inspect OutcomeOf.
func (b *BoundLimiter) AllowOnce(ctx context.Context, id string, retention time.Duration, charges ...Charge) (receipt Receipt, err error) {
	l := b.parent()
	finish := l.observe(ctx, "allow_once", len(charges))
	defer func() { finish(receipt.Decision, receipt.Replayed, err) }()
	rules, err := b.chargedRules(ctx, charges)
	if err != nil {
		return Receipt{}, failure("allow_once", NotCharged, err)
	}
	if id == "" {
		return Receipt{}, failure("allow_once", NotCharged, fmt.Errorf("%w: ID is required", ErrInvalid))
	}
	return l.admit(ctx, id, retention, rules)
}

// Lookup queries a retained result in the parent's namespace/domain, even if it
// was created through a different binding or Limiter.AllowOnce. It does not filter
// by this quota set, debit quotas or extend retention. A missing result does not
// prove no debit. Use Decision only when err is nil and found is true.
func (b *BoundLimiter) Lookup(ctx context.Context, id string) (Decision, bool, error) {
	return b.parent().Lookup(ctx, id)
}

// parent permits nil bound receivers to use the standard error and observer paths.
func (b *BoundLimiter) parent() *Limiter {
	if b == nil {
		return nil
	}
	return b.limiter
}

// chargedRules validates a complete charge set and assembles a per-call copy in
// binding order. No caller-owned slice or shared binding is mutated.
func (b *BoundLimiter) chargedRules(ctx context.Context, charges []Charge) ([]Rule, error) {
	if err := b.parent().ready(ctx); err != nil {
		return nil, err
	}
	if len(charges) != len(b.rules) {
		return nil, fmt.Errorf("%w: require one charge per bound quota", ErrInvalid)
	}
	rules := append([]Rule(nil), b.rules...)
	seen := make([]bool, len(rules))
	for _, charge := range charges {
		rule := charge.quota.rule(charge.cost)
		i, ok := b.index[rule.Name]
		if !ok {
			return nil, fmt.Errorf("%w: unknown quota %q", ErrInvalid, rule.Name)
		}
		if seen[i] {
			return nil, fmt.Errorf("%w: duplicate charge for quota %q", ErrInvalid, rule.Name)
		}
		interval, err := rule.Interval()
		if err != nil {
			return nil, err
		}
		// Bind validated the immutable template, including its placeholder cost.
		boundInterval, _ := rules[i].Interval()
		if interval != boundInterval || rule.Policy.Burst != rules[i].Policy.Burst || rule.Policy.Revision != rules[i].Policy.Revision {
			return nil, fmt.Errorf("%w: quota %q policy differs from binding", ErrInvalid, rule.Name)
		}
		seen[i] = true
		rules[i].Cost = charge.cost
	}
	return rules, nil
}
