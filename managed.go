package pacelock

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"
)

// NamedCharge requests known cost against a backend-managed quota. It contains
// no local policy or revision; the backend resolves current configuration during
// atomic admission. The application must include every applicable quota.
type NamedCharge struct {
	Name string
	Cost int64
}

// ManagedDriver is the optional backend-authoritative admission extension.
// Load configuration and debt under the same atomic boundary. Retained decisions
// replay before current configuration checks. Missing configuration fails closed.
type ManagedDriver interface {
	AdmitManaged(context.Context, ManagedAdmission) (Receipt, error)
}

// ManagedAdmission requests an atomic charge using backend-owned policies.
// ID empty means ordinary admission; nonempty retains the original decision.
type ManagedAdmission struct {
	Scope
	Charges   []NamedCharge
	ID        string
	Retention time.Duration
}

// Validate checks identities and costs, without assuming local policy knowledge.
func (a ManagedAdmission) Validate() error {
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
func (a ManagedAdmission) Fingerprint() string {
	args := []any{"managed-admission-v1", a.Retention.Milliseconds()}
	for _, c := range a.Charges {
		args = append(args, c.Name, c.Cost)
	}
	data, _ := json.Marshal(args)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// AllowManaged uses the current backend configuration for every named charge.
// Missing configuration returns ErrPolicyMissing. Pause returns ErrPaused, with
// no debit. Ordinary capacity denial returns Allowed=false and a RetryAfter.
// A cost exceeding the current burst returns ErrInvalid, not a retryable denial.
func (l *Limiter) AllowManaged(ctx context.Context, charges ...NamedCharge) (decision Decision, err error) {
	finish := l.observe(ctx, "allow_managed", len(charges))
	defer func() { finish(decision, false, err) }()
	receipt, err := l.admitManaged(ctx, "", 0, charges)
	return receipt.Decision, err
}

// AllowManagedOnce retains an admission decision using backend-owned policies.
// Same-ID retries replay the original decision even after updates or pause; they
// never authorize executing upstream work twice. Lookup reads either API's result.
// Paused/missing-policy errors do not create a retained receipt.
func (l *Limiter) AllowManagedOnce(ctx context.Context, id string, retention time.Duration, charges ...NamedCharge) (receipt Receipt, err error) {
	finish := l.observe(ctx, "allow_managed_once", len(charges))
	defer func() { finish(receipt.Decision, receipt.Replayed, err) }()
	if id == "" {
		return Receipt{}, failure("allow_managed_once", NotCharged, fmt.Errorf("%w: ID is required", ErrInvalid))
	}
	return l.admitManaged(ctx, id, retention, charges)
}

func (l *Limiter) admitManaged(ctx context.Context, id string, retention time.Duration, charges []NamedCharge) (Receipt, error) {
	op := "allow_managed"
	if id != "" {
		op = "allow_managed_once"
	}
	if err := l.ready(ctx); err != nil {
		return Receipt{}, failure(op, NotCharged, err)
	}
	a := ManagedAdmission{Scope: l.scope, Charges: append([]NamedCharge(nil), charges...), ID: id, Retention: retention}
	if err := a.Validate(); err != nil {
		return Receipt{}, failure(op, NotCharged, err)
	}
	driver, ok := l.driver.(ManagedDriver)
	if !ok {
		return Receipt{}, failure(op, NotCharged, ErrUnsupported)
	}
	receipt, err := driver.AdmitManaged(ctx, a)
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
func EvaluateManaged(now int64, charges []NamedCharge, policies, debts []QuotaState) (Decision, []QuotaState, error) {
	a := ManagedAdmission{Scope: Scope{Namespace: "validate", Domain: "validate"}, Charges: charges}
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
			return fmt.Errorf("%w: use AllowManaged for backend-owned policy", ErrInvalid)
		}
	}
	return nil
}
