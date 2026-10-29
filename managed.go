package pacelock

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"
)

// Charge requests a known cost against a named quota. It contains no policy or
// revision: Limiter resolves backend configuration atomically, while StaticLimiter
// uses its immutable local policies. Include every applicable quota in one call.
type Charge struct {
	// Name identifies a quota within the limiter's scope.
	Name string
	// Cost is positive and must not exceed the effective policy's Burst.
	Cost int64
}

// Admission requests an atomic charge using backend-owned policies.
// ID empty means ordinary admission; nonempty retains the original decision.
// Drivers must not retain or mutate the caller's Charges slice.
type Admission struct {
	Scope
	Charges   []Charge
	ID        string
	Retention time.Duration
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
		if c.Name == "" || len(c.Name) > 256 || seen[c.Name] || c.Cost < 1 || c.Cost > MaxHorizon.Microseconds() || (a.ID != "" && !utf8.ValidString(c.Name)) {
			return fmt.Errorf("%w: invalid managed charge %q", ErrInvalid, c.Name)
		}
		seen[c.Name] = true
	}
	if a.ID == "" {
		if a.Retention != 0 {
			return fmt.Errorf("%w: retention requires ID", ErrInvalid)
		}
		return nil
	}
	if err := (LookupRequest{Scope: a.Scope, ID: a.ID}).Validate(); err != nil {
		return err
	}
	if a.Retention < time.Millisecond || a.Retention > MaxHorizon || a.Retention%time.Millisecond != 0 {
		return fmt.Errorf("%w: retention must be whole milliseconds in [1ms,24h]", ErrInvalid)
	}
	return nil
}

// Fingerprint binds ordered names/costs and retention, NOT the current policy.
// A retry of the same attempt replays the original decision across config changes.
// The discriminator prevents reuse of a static admission ID as a managed one.
// Call Validate first. Scope and ID identify the retained record separately.
func (a Admission) Fingerprint() string {
	args := []any{"managed-admission-v1", a.Retention.Milliseconds()}
	for _, c := range a.Charges {
		args = append(args, c.Name, c.Cost)
	}
	data, _ := json.Marshal(args)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// Allow uses the current backend configuration for every named charge.
// This default admission never falls back to caller-owned static policies.
// Missing configuration returns ErrPolicyMissing. Pause returns ErrPaused, with
// no debit. Ordinary capacity denial returns Allowed=false and a RetryAfter.
// A cost exceeding the current burst returns ErrInvalid, not a retryable denial.
func (l *Limiter) Allow(ctx context.Context, charges ...Charge) (decision Decision, err error) {
	core := l.engine()
	finish := core.observe(ctx, "allow", len(charges))
	defer func() { finish(decision, false, err) }()
	receipt, err := core.admitManaged(ctx, "", 0, charges)
	return receipt.Decision, err
}

// AllowOnce retains an admission decision using backend-owned policies.
// Same-ID retries replay the original decision even after updates or pause; they
// never authorize executing upstream work twice. Lookup reads either API's result.
// Paused/missing-policy errors do not create a retained receipt.
func (l *Limiter) AllowOnce(ctx context.Context, id string, retention time.Duration, charges ...Charge) (receipt Receipt, err error) {
	core := l.engine()
	finish := core.observe(ctx, "allow_once", len(charges))
	defer func() { finish(receipt.Decision, receipt.Replayed, err) }()
	if id == "" {
		return Receipt{}, failure("allow_once", NotCharged, fmt.Errorf("%w: ID is required", ErrInvalid))
	}
	return core.admitManaged(ctx, id, retention, charges)
}

func (l *limiterCore) admitManaged(ctx context.Context, id string, retention time.Duration, charges []Charge) (Receipt, error) {
	op := "allow"
	if id != "" {
		op = "allow_once"
	}
	if err := l.ready(ctx); err != nil {
		return Receipt{}, failure(op, NotCharged, err)
	}
	a := Admission{Scope: l.scope, Charges: append([]Charge(nil), charges...), ID: id, Retention: retention}
	if err := a.Validate(); err != nil {
		return Receipt{}, failure(op, NotCharged, err)
	}
	receipt, err := l.driver.Admit(ctx, a)
	if err != nil {
		return Receipt{}, driverFailure(op, err)
	}
	rules := make([]Rule, len(charges))
	for i, c := range charges {
		rules[i].Name = c.Name
	}
	if err := validateDecision(receipt.Decision, rules); err != nil {
		return Receipt{}, failure(op, Unknown, err)
	}
	if id == "" && receipt.Replayed {
		return Receipt{}, failure(op, Unknown, fmt.Errorf("%w: replay of ordinary managed admission", ErrState))
	}
	return receipt, nil
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
// debt plus any retained receipt only when Allowed. Configurations are unchanged.
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

// ValidateStatic prevents the caller-policy API from masquerading as managed
// admission. Backend configuration checks additionally prevent a static writer
// from bypassing an already managed quota. Validate remains a general math check.
func ValidateStatic(rules ...Rule) error {
	if err := Validate(rules...); err != nil {
		return err
	}
	for _, r := range rules {
		if r.Policy.Revision != 0 {
			return fmt.Errorf("%w: use Allow for backend-owned policy", ErrInvalid)
		}
	}
	return nil
}
