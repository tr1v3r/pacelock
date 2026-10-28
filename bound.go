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

// StaticCharge captures a quota's name and policy together with its per-attempt cost.
// Construct it with Quota.StaticCharge. A zero StaticCharge is invalid.
type StaticCharge struct {
	quota Quota
	cost  int64
}

// StaticCharge captures a value copy of q with the known consumption for one attempt.
// It does not validate inputs or contact the backend. StaticLimiter.Allow and
// AllowOnce require a valid policy matching the binding's effective interval and
// Burst, and a cost from 1 through Burst. Later changes to q do not affect the charge.
func (q Quota) StaticCharge(cost int64) StaticCharge {
	return StaticCharge{quota: q, cost: cost}
}

func (q Quota) rule(cost int64) Rule {
	return Rule{Name: q.Name, Policy: q.Policy, Cost: cost}
}

// StaticLimiter binds an immutable, ordered quota set to a Limiter. Every admission
// must charge a nonempty subset of bound quotas exactly once. Charges may arrive
// in any order; decisions and idempotency fingerprints use binding order.
// It is safe for concurrent use under the parent Limiter's driver contract.
// Construct it with Limiter.BindStatic; nil and zero receivers return ErrInvalid.
type StaticLimiter struct {
	// limiter provides the driver, scope and observer used by every bound operation.
	limiter *Limiter
	// rules is a private copy of policies in binding order; per-call costs are copied.
	rules []Rule
	// index maps each bound name to its position and is read-only after construction.
	index map[string]int
}

// BindStatic validates and copies 1..MaxRules uniquely named quotas without contacting
// the backend or registering policies there. Changing the input afterward has no effect.
// Errors wrap ErrInvalid. Active backend policy conflicts are detected at admission.
// Bind all applicable account/model/tenant quotas; omitted quotas cannot be inferred.
// Create a new binding to change policies or select another tenant/model quota set.
func (l *Limiter) BindStatic(quotas ...Quota) (*StaticLimiter, error) {
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
	return &StaticLimiter{limiter: l, rules: rules, index: index}, nil
}

// Allow immediately checks and charges a nonempty subset of bound quotas or denies
// without charging any. Use Quota.StaticCharge to supply each selected quota exactly once,
// in any order, with 0 < cost <= Burst. Each charge must carry a valid policy with
// the same effective interval and Burst as its bound quota. Empty, duplicate,
// unknown, mismatched or invalid charges return ErrInvalid with NotCharged before
// dispatch. Omitted quotas are neither checked nor charged. Separate calls are
// independent admissions: a later denial does not roll back an earlier debit.
// It never waits for quota recovery or queues work; backend synchronization may
// wait until ctx expires. After dispatch an error may have an Unknown debit
// outcome. Ignore Decision on error and inspect OutcomeOf.
func (b *StaticLimiter) Allow(ctx context.Context, charges ...StaticCharge) (decision Decision, err error) {
	l := b.parent()
	finish := l.observe(ctx, "allow_static", len(charges))
	defer func() { finish(decision, false, err) }()
	rules, err := b.chargedRules(ctx, charges)
	if err != nil {
		return Decision{}, failure("allow_static", NotCharged, err)
	}
	receipt, err := l.admitStatic(ctx, "", 0, rules)
	return receipt.Decision, err
}

// AllowOnce adds the same bounded deduplication and recovery contract as
// Limiter.AllowStaticOnce. ID and retention have the same bounds. Charges are validated
// as in StaticLimiter.Allow, and quota names must be valid UTF-8. Reordering charges
// does not change a retained attempt; only selected quotas enter the fingerprint,
// in binding order. Their effective policies, costs and retention must still match.
// Changing the selected subset with the same retained ID returns ErrConflict.
// Retain the original binding and selected charges for attempt recovery.
// Both allows and denials are retained. Ignore Receipt on error and inspect OutcomeOf.
func (b *StaticLimiter) AllowOnce(ctx context.Context, id string, retention time.Duration, charges ...StaticCharge) (receipt Receipt, err error) {
	l := b.parent()
	finish := l.observe(ctx, "allow_static_once", len(charges))
	defer func() { finish(receipt.Decision, receipt.Replayed, err) }()
	rules, err := b.chargedRules(ctx, charges)
	if err != nil {
		return Receipt{}, failure("allow_static_once", NotCharged, err)
	}
	if id == "" {
		return Receipt{}, failure("allow_static_once", NotCharged, fmt.Errorf("%w: ID is required", ErrInvalid))
	}
	return l.admitStatic(ctx, id, retention, rules)
}

// Lookup queries a retained result in the parent's namespace/domain, even if it
// was created through a different binding or Limiter.AllowStaticOnce. It does not filter
// by this quota set, debit quotas or extend retention. A missing result does not
// prove no debit. Use Decision only when err is nil and found is true.
func (b *StaticLimiter) Lookup(ctx context.Context, id string) (Decision, bool, error) {
	return b.parent().Lookup(ctx, id)
}

// parent permits nil bound receivers to use the standard error and observer paths.
func (b *StaticLimiter) parent() *Limiter {
	if b == nil {
		return nil
	}
	return b.limiter
}

// chargedRules validates a nonempty charge subset and assembles a per-call copy
// of only selected rules in binding order. Caller and binding data are not mutated.
func (b *StaticLimiter) chargedRules(ctx context.Context, charges []StaticCharge) ([]Rule, error) {
	if err := b.parent().ready(ctx); err != nil {
		return nil, err
	}
	if len(charges) == 0 || len(charges) > len(b.rules) {
		return nil, fmt.Errorf("%w: require a nonempty subset of bound quotas", ErrInvalid)
	}
	costs := make([]int64, len(b.rules))
	for _, charge := range charges {
		rule := charge.quota.rule(charge.cost)
		i, ok := b.index[rule.Name]
		if !ok {
			return nil, fmt.Errorf("%w: unknown quota %q", ErrInvalid, rule.Name)
		}
		if costs[i] != 0 {
			return nil, fmt.Errorf("%w: duplicate charge for quota %q", ErrInvalid, rule.Name)
		}
		interval, err := rule.Interval()
		if err != nil {
			return nil, err
		}
		// BindStatic validated the immutable template, including its placeholder cost.
		boundInterval, _ := b.rules[i].Interval()
		if interval != boundInterval || rule.Policy.Burst != b.rules[i].Policy.Burst || rule.Policy.Revision != b.rules[i].Policy.Revision {
			return nil, fmt.Errorf("%w: quota %q policy differs from binding", ErrInvalid, rule.Name)
		}
		costs[i] = charge.cost
	}
	rules := make([]Rule, 0, len(charges))
	for i, rule := range b.rules {
		if costs[i] != 0 {
			rule.Cost = costs[i]
			rules = append(rules, rule)
		}
	}
	return rules, nil
}
