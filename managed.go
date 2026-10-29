package pacelock

import (
	"context"
	"fmt"
	"time"
)

// Charge requests a known cost against a named quota. It contains no policy or
// revision: Limiter resolves backend configuration atomically.
// Include every applicable quota in one call.
type Charge struct {
	// Name identifies a quota within the limiter's scope.
	Name string
	// Cost is positive and must not exceed the effective policy's Burst.
	Cost int64
}

// Admission requests an atomic charge using backend-owned policies.
// Drivers must not retain or mutate the caller's Charges slice.
type Admission struct {
	Scope
	Charges []Charge
}

// Validate checks identities and costs, without assuming local policy knowledge.
func (a Admission) Validate() error {
	if err := a.Scope.Validate(); err != nil {
		return err
	}
	if len(a.Charges) == 0 || len(a.Charges) > MaxRules {
		return fmt.Errorf("%w: require 1..%d charges", ErrInvalid, MaxRules)
	}
	seen := map[string]bool{}
	for _, c := range a.Charges {
		if c.Name == "" || len(c.Name) > 256 || seen[c.Name] || c.Cost < 1 || c.Cost > MaxHorizon.Microseconds() {
			return fmt.Errorf("%w: invalid managed charge %q", ErrInvalid, c.Name)
		}
		seen[c.Name] = true
	}
	return nil
}

// Allow uses the current backend configuration for every named charge.
// This default admission never falls back to caller-owned static policies.
// Missing configuration returns ErrPolicyMissing. Pause returns ErrPaused, with
// no debit. Ordinary capacity denial returns Allowed=false and a RetryAfter.
// A cost exceeding the current burst returns ErrInvalid, not a retryable denial.
func (l *Limiter) Allow(ctx context.Context, charges ...Charge) (decision Decision, err error) {
	core := l.engine()
	finish := core.observe(ctx, "allow", len(charges))
	defer func() { finish(decision, err) }()
	return core.admitManaged(ctx, charges)
}

func (l *limiterCore) admitManaged(ctx context.Context, charges []Charge) (Decision, error) {
	op := "allow"
	if err := l.ready(ctx); err != nil {
		return Decision{}, failure(op, NotCharged, err)
	}
	a := Admission{Scope: l.scope, Charges: append([]Charge(nil), charges...)}
	if err := a.Validate(); err != nil {
		return Decision{}, failure(op, NotCharged, err)
	}
	decision, err := l.driver.Admit(ctx, a)
	if err != nil {
		return Decision{}, driverFailure(op, err)
	}
	rules := make([]Rule, len(charges))
	for i, c := range charges {
		rules[i].Name = c.Name
	}
	if err := validateDecision(decision, rules); err != nil {
		return Decision{}, failure(op, Unknown, err)
	}
	return decision, nil
}

func validatePolicyState(s QuotaState) error {
	if s.Revision <= 0 || s.TAT != 0 || s.ExpiresAt != 0 {
		return fmt.Errorf("%w: invalid policy configuration", ErrState)
	}
	return validateQuotaState(0, s)
}

// EvaluateManaged resolves current configurations against independent debt.
// policies contains configuration-only states (TAT=ExpiresAt=0); a zero state is
// missing configuration. debts contains persisted state from prior admissions.
// Drivers must serialize reads with updates and atomically persist every next
// debt only when Allowed. Configurations are unchanged.
func EvaluateManaged(now int64, charges []Charge, policies, debts []QuotaState) (Decision, []QuotaState, error) {
	a := Admission{Scope: Scope{Namespace: "validate", Domain: "validate"}, Charges: charges}
	if err := a.Validate(); err != nil {
		return Decision{}, nil, err
	}
	if len(policies) != len(charges) || len(debts) != len(charges) {
		return Decision{}, nil, fmt.Errorf("%w: state count", ErrInvalid)
	}
	if err := validateBackendTime(now); err != nil {
		return Decision{}, nil, err
	}
	rules := make([]Rule, len(charges))
	states := make([]QuotaState, len(charges))
	for i, c := range charges {
		p := policies[i]
		d := debts[i]
		if p == (QuotaState{}) {
			return Decision{}, nil, fmt.Errorf("%w: quota %q", ErrPolicyMissing, c.Name)
		}
		if err := validatePolicyState(p); err != nil {
			return Decision{}, nil, err
		}
		if d != (QuotaState{}) {
			if err := validateQuotaState(now, d); err != nil {
				return Decision{}, nil, err
			}
		}
		if p.Interval == 0 {
			return Decision{}, nil, fmt.Errorf("%w: quota %q", ErrPaused, c.Name)
		}
		rules[i] = Rule{Name: c.Name, Cost: c.Cost, Policy: Policy{Revision: p.Revision, Rate: 1, Period: time.Duration(p.Interval) * time.Microsecond, Burst: p.Burst}}
		states[i] = p
		states[i].TAT = d.TAT
		states[i].ExpiresAt = d.ExpiresAt
	}
	return Evaluate(now, rules, states)
}
