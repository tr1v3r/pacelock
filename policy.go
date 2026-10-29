package pacelock

import (
	"context"
	"fmt"
	"time"
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

// Quota associates an independent quota name with its backend-owned policy.
// UpdateQuotas requires a positive revision.
type Quota struct {
	// Name identifies shared state within the domain and must contain 1..256 bytes.
	Name string
	// Policy specifies how this quota replenishes and how much it can accumulate.
	Policy Policy
}

func (q Quota) rule(cost int64) Rule {
	return Rule{Name: q.Name, Policy: q.Policy, Cost: cost}
}

// QuotaDriver is the optional, atomic quota-configuration extension to Driver.
// Implementations serialize updates with admission and preserve absolute TAT.
// Revisions are permanent fences, not expiring debt. No automatic retries occur.
type QuotaDriver interface {
	UpdateQuotas(context.Context, QuotaBatch) error
}

// QuotaBatch is an ordered batch of managed quota configurations in one atomic scope.
// Revisions must increase; retrying an identical revision/effective policy is safe.
// Same revision with different policy or a lower revision is ErrPolicyMismatch.
type QuotaBatch struct {
	Scope
	Quotas []Quota
}

// Validate checks the batch without accessing backend state.
func (u QuotaBatch) Validate() error {
	if err := u.Scope.Validate(); err != nil {
		return err
	}
	if len(u.Quotas) == 0 || len(u.Quotas) > MaxRules {
		return fmt.Errorf("%w: require 1..%d policies", ErrInvalid, MaxRules)
	}
	rules := make([]Rule, len(u.Quotas))
	for i, q := range u.Quotas {
		if q.Policy.Revision <= 0 {
			return fmt.Errorf("%w: updates require managed revisions", ErrInvalid)
		}
		rules[i] = q.rule(1)
	}
	return Validate(rules...)
}

// UpdateQuotas installs quota configurations without charging or refilling capacity,
// resetting debt, or changing TAT.
// Higher revisions take effect atomically, even while a quota has outstanding debt.
// A managed Rate=0 pauses new admissions; debt continues to recover with time.
// Update all application binaries before activating managed policies. Managed
// admissions load the current backend policy, so callers need no local snapshot.
// On a transport/commit error the update may have committed; inspect OutcomeOf
// and retry the SAME revision/policy explicitly if appropriate. Never lower a
// revision for rollback: publish the old parameters at a higher revision.
func (l *Limiter) UpdateQuotas(ctx context.Context, quotas ...Quota) error {
	const op = "update_quotas"
	core := l.engine()
	if err := core.ready(ctx); err != nil {
		return failure(op, NotCharged, err)
	}
	u := QuotaBatch{Scope: core.scope, Quotas: append([]Quota(nil), quotas...)}
	if err := u.Validate(); err != nil {
		return failure(op, NotCharged, err)
	}
	driver, ok := core.driver.(QuotaDriver)
	if !ok {
		return failure(op, NotCharged, ErrUnsupported)
	}
	if err := driver.UpdateQuotas(ctx, u); err != nil {
		return driverFailure(op, err)
	}
	return nil
}

// ApplyQuotas calculates an all-or-none configuration update without I/O.
// states contains configuration only (Interval, Burst, Revision); TAT and ExpiresAt
// must be zero. Drivers serialize this with admission and persist configurations
// separately from debt. This function neither reads nor modifies debt.
func ApplyQuotas(now int64, quotas []Quota, states []QuotaState) ([]QuotaState, error) {
	if err := (QuotaBatch{Scope: Scope{Namespace: "validate", Domain: "validate"}, Quotas: quotas}).Validate(); err != nil {
		return nil, err
	}
	if len(states) != len(quotas) {
		return nil, fmt.Errorf("%w: state count", ErrInvalid)
	}
	if err := validateBackendTime(now); err != nil {
		return nil, err
	}
	next := make([]QuotaState, len(states))
	for i, q := range quotas {
		s := states[i]
		if s != (QuotaState{}) {
			if err := validatePolicyState(s); err != nil {
				return nil, err
			}
		}
		interval, _ := q.rule(1).Interval()
		step := interval.Microseconds()
		if s.Revision > q.Policy.Revision || (s.Revision == q.Policy.Revision && (s.Interval != step || s.Burst != q.Policy.Burst)) {
			return nil, fmt.Errorf("%w: quota %q revision", ErrPolicyMismatch, q.Name)
		}
		s.Interval = step
		s.Burst = q.Policy.Burst
		s.Revision = q.Policy.Revision
		next[i] = s
	}
	return next, nil
}

func validateBackendTime(now int64) error {
	if now < 0 || now > MaxRevision-2*MaxHorizon.Microseconds() {
		return fmt.Errorf("%w: backend clock outside supported range", ErrState)
	}
	return nil
}

func validateQuotaState(now int64, s QuotaState) error {
	h := MaxHorizon.Microseconds()
	if s.Revision < 0 || s.Revision > MaxRevision || s.Interval < 0 || s.Interval > h || s.Burst < 1 || s.Burst > h || (s.Interval == 0 && s.Revision == 0) || (s.Interval > 0 && s.Burst > h/s.Interval) || s.TAT < 0 || s.TAT > now+h || s.ExpiresAt < s.TAT || s.ExpiresAt-s.TAT > 999 || (s.Revision == 0 && s.ExpiresAt <= 0) {
		return fmt.Errorf("%w: invalid quota state", ErrState)
	}
	return nil
}
