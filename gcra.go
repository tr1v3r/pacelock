package pacelock

import (
	"fmt"
	"time"
)

// QuotaState is a persisted GCRA state in integer Unix microseconds. Its zero
// value means absent. ExpiresAt is the logical expiry, rounded up to milliseconds
// from the admission time; expiry restores full capacity and permits a new policy.
// State is not an execution record or a billing ledger.
type QuotaState struct {
	// TAT is the theoretical recovery-completion time in Unix microseconds.
	TAT int64
	// Interval is the effective per-unit recovery interval in microseconds.
	Interval int64
	// Burst is the active policy total instantaneous capacity.
	Burst int64
	// ExpiresAt is the logical expiry in Unix microseconds.
	ExpiresAt int64
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
	const exactLimit int64 = 9007199254740991
	horizon := MaxHorizon.Microseconds()
	if now < 0 || now > exactLimit-2*horizon {
		return Decision{}, nil, fmt.Errorf("%w: backend clock outside supported range", ErrState)
	}
	decision.Allowed = true
	next = make([]QuotaState, len(rules))
	for i, r := range rules {
		interval, _ := r.Interval()
		step := interval.Microseconds()
		s := states[i]
		tat := now
		if s != (QuotaState{}) && s.ExpiresAt > now {
			if s.Interval < 1 || s.Interval > horizon || s.Burst < 1 || s.Burst > horizon/s.Interval || s.TAT < 0 || s.TAT > now+horizon || s.ExpiresAt < s.TAT || s.ExpiresAt-s.TAT > 999 {
				return Decision{}, nil, fmt.Errorf("%w: invalid quota state", ErrState)
			}
			if s.Interval != step || s.Burst != r.Burst {
				return Decision{}, nil, fmt.Errorf("%w: quota %q", ErrPolicyMismatch, r.Name)
			}
			tat = max(tat, s.TAT)
		} else if s != (QuotaState{}) && s.ExpiresAt <= 0 {
			return Decision{}, nil, fmt.Errorf("%w: missing state expiry", ErrState)
		}
		candidate := tat + r.Cost*step
		wait := max(int64(0), candidate-r.Burst*step-now)
		if wait > 0 {
			decision.Allowed = false
			decision.Limited = append(decision.Limited, r.Name)
			decision.RetryAfter = max(decision.RetryAfter, time.Duration(wait)*time.Microsecond)
		}
		next[i] = QuotaState{TAT: candidate, Interval: step, Burst: r.Burst, ExpiresAt: now + ((candidate-now+999)/1000)*1000}
	}
	if !decision.Allowed {
		next = nil
	}
	return decision, next, nil
}

// Validate checks structural consistency of a decision, including retained waits.
// It does not verify a decision against a particular request's quota names.
func (d Decision) Validate() error { return validateDecision(d, nil) }
