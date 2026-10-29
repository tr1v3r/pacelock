package pacelock

import (
	"context"
	"fmt"
)

// QuotaDriver is the optional, atomic quota-configuration extension to Driver.
// Implementations serialize updates with admission and preserve absolute TAT.
// Revisions are permanent fences, not expiring debt. No automatic retries occur.
type QuotaDriver interface {
	UpdateQuotas(context.Context, QuotaUpdate) error
}

// QuotaUpdate installs an ordered batch of managed quota configurations in one atomic scope.
// Revisions must increase; retrying an identical revision/effective policy is safe.
// Same revision with different policy or a lower revision is ErrPolicyMismatch.
type QuotaUpdate struct {
	Scope
	Quotas []Quota
}

// Validate checks the update without accessing backend state.
func (u QuotaUpdate) Validate() error {
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
	u := QuotaUpdate{Scope: core.scope, Quotas: append([]Quota(nil), quotas...)}
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

// ApplyPolicies calculates an all-or-none configuration update without I/O.
// states contains configuration only (Interval, Burst, Revision); TAT and ExpiresAt
// must be zero. Drivers serialize this with admission and persist configurations
// separately from debt. This function neither reads nor modifies debt.
func ApplyPolicies(now int64, quotas []Quota, states []QuotaState) ([]QuotaState, error) {
	if err := (QuotaUpdate{Scope: Scope{Namespace: "validate", Domain: "validate"}, Quotas: quotas}).Validate(); err != nil {
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
