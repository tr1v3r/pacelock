package pacelock

import (
	"fmt"
	"time"
)

// QuotaState represents GCRA state in integer Unix microseconds; zero means absent.
// ExpiresAt is debt expiry, rounded up to milliseconds from admission time.
// Managed drivers persist configuration separately from debt: a configuration-only
// state carries Interval/Burst/Revision and zero TAT/ExpiresAt. EvaluateManaged
// combines them temporarily for calculation. State is not a billing ledger.
type QuotaState struct {
	// TAT is the theoretical recovery-completion time in Unix microseconds.
	TAT int64
	// Interval is the effective per-unit recovery interval in microseconds.
	Interval int64
	// Burst is the active policy total instantaneous capacity.
	Burst int64
	// ExpiresAt is the logical expiry in Unix microseconds.
	ExpiresAt int64
	// Revision identifies managed configuration. The configuration record persists
	// even when its separate debt record expires. Interval=0 means paused.
	Revision int64
}

// Evaluate calculates an atomic multi-quota transition without I/O or mutation.
// now is authoritative backend Unix time in microseconds, sampled once after
// synchronization. states corresponds to rules in order. On allowance next holds
// every replacement state; on denial next is nil. The caller must persist all next
// states atomically before reporting allowance. Redis implements the same formula
// in Lua; drivers can use this function within their own persistence boundary.
func Evaluate(now int64, rules []Rule, states []QuotaState) (decision Decision, next []QuotaState, err error) {
	if err := Validate(rules...); err != nil {
		return Decision{}, nil, err
	}
	if len(states) != len(rules) {
		return Decision{}, nil, fmt.Errorf("%w: state count", ErrInvalid)
	}
	if err := validateBackendTime(now); err != nil {
		return Decision{}, nil, err
	}
	decision.Allowed = true
	next = make([]QuotaState, len(rules))
	for i, r := range rules {
		interval, _ := r.Interval()
		step := interval.Microseconds()
		s := states[i]
		tat := now
		if s.Revision != 0 || r.Policy.Revision != 0 {
			if s != (QuotaState{}) {
				if err := validateQuotaState(now, s); err != nil {
					return Decision{}, nil, err
				}
			}
			if s.Revision == 0 || s.Revision != r.Policy.Revision || s.Interval != step || s.Burst != r.Policy.Burst {
				return Decision{}, nil, fmt.Errorf("%w: quota %q revision", ErrPolicyMismatch, r.Name)
			}
			if step == 0 {
				return Decision{}, nil, fmt.Errorf("%w: quota %q", ErrPaused, r.Name)
			}
			tat = max(tat, s.TAT)
		} else if s != (QuotaState{}) && s.ExpiresAt > now {
			if err := validateQuotaState(now, s); err != nil {
				return Decision{}, nil, err
			}
			if s.Interval != step || s.Burst != r.Policy.Burst {
				return Decision{}, nil, fmt.Errorf("%w: quota %q", ErrPolicyMismatch, r.Name)
			}
			tat = max(tat, s.TAT)
		} else if s != (QuotaState{}) && s.ExpiresAt <= 0 {
			return Decision{}, nil, fmt.Errorf("%w: missing state expiry", ErrState)
		}
		candidate := tat + r.Cost*step
		wait := max(int64(0), candidate-r.Policy.Burst*step-now)
		if wait > 0 {
			decision.Allowed = false
			decision.Limited = append(decision.Limited, r.Name)
			decision.RetryAfter = max(decision.RetryAfter, time.Duration(wait)*time.Microsecond)
		}
		next[i] = QuotaState{TAT: candidate, Interval: step, Burst: r.Policy.Burst, Revision: r.Policy.Revision, ExpiresAt: now + ((candidate-now+999)/1000)*1000}
	}
	if !decision.Allowed {
		next = nil
	}
	return decision, next, nil
}

// Validate checks structural consistency of an allowance or ordinary quota denial.
// It does not verify a decision against a particular request's quota names.
func (d Decision) Validate() error { return validateDecision(d, nil) }
