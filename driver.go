package pacelock

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"
)

// Driver atomically admits all named charges using backend-owned dynamic policies
// or denies without debiting any. It loads configuration and debt under the same
// atomic boundary. Retained decisions replay before current configuration checks;
// missing configuration fails closed with ErrPolicyMissing, never a static fallback.
// Implementations must be concurrency-safe, use authoritative backend time, and
// preserve effective policy checks, ordered rules, bounded ID deduplication and
// original retry durations. Calls may synchronize on locks but must not wait for
// quota recovery. They must not replay ambiguous mutations. Errors should carry
// *Error with NotCharged only when this invocation certainly did not debit;
// untyped errors are conservatively Unknown. Lookup must not mutate quota or
// refresh retention. A missing lookup never establishes zero debit.
// All writers to a scope must share a backend and its synchronization protocol.
// Drivers do not migrate state or deduplicate across different backends.
type Driver interface {
	// Admit completes one atomic multi-quota decision and optional retained receipt.
	Admit(context.Context, Admission) (Receipt, error)
	// Lookup reads a retained result without extending its lifetime.
	Lookup(context.Context, LookupRequest) (Decision, bool, error)
}

// StaticDriver optionally supports admission using caller-owned policies.
// Implementations must reject static writes to backend-managed quotas.
type StaticDriver interface {
	AdmitStatic(context.Context, StaticAdmission) (Receipt, error)
}

// Scope identifies one application's atomic quota domain. Each field contains
// 1..256 bytes. Different scopes have independent quotas and admission IDs.
type Scope struct {
	// Namespace isolates applications sharing one backend.
	Namespace string
	// Domain identifies a set of quotas admitted atomically.
	Domain string
}

// Validate checks scope names without contacting a backend.
func (s Scope) Validate() error {
	if s.Namespace == "" || s.Domain == "" || len(s.Namespace) > 256 || len(s.Domain) > 256 {
		return fmt.Errorf("%w: namespace and domain require 1..256 bytes", ErrInvalid)
	}
	return nil
}

// StaticAdmission requests a single atomic operation. An empty ID requires Retention=0
// and means ordinary, non-idempotent admission. A nonempty ID retains either
// decision. Drivers must not retain or mutate the caller's Rules slice.
type StaticAdmission struct {
	// Scope identifies the atomic quota and ID domain.
	Scope
	// Rules contains ordered, uniquely named charges.
	Rules []Rule
	// ID is empty for AllowStatic or contains 1..256 bytes for retained admission.
	ID string
	// Retention is zero for AllowStatic or whole milliseconds in [1ms,24h].
	Retention time.Duration
}

// Validate checks the complete driver request without backend access.
func (a StaticAdmission) Validate() error {
	if err := a.Scope.Validate(); err != nil {
		return err
	}
	if err := ValidateStatic(a.Rules...); err != nil {
		return err
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
	for _, r := range a.Rules {
		if !utf8.ValidString(r.Name) {
			return fmt.Errorf("%w: idempotent names require UTF-8", ErrInvalid)
		}
	}
	return nil
}

// Fingerprint binds ordered names, effective intervals, bursts, costs and
// retention to the Redis v1-compatible argument encoding. Call Validate first.
// Scope and ID identify the record and are not part of this fingerprint.
func (a StaticAdmission) Fingerprint() string {
	args := []any{a.Retention.Milliseconds(), ""}
	for _, r := range a.Rules {
		step, _ := r.Interval()
		args = append(args, step.Microseconds(), r.Policy.Burst, r.Cost, r.Name)
	}
	data, _ := json.Marshal(args)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// LookupRequest locates an admission ID within a scope. ID contains 1..256 bytes.
type LookupRequest struct {
	// Scope selects the ID domain.
	Scope
	// ID contains 1..256 bytes.
	ID string
}

// Validate checks lookup identity without backend access.
func (r LookupRequest) Validate() error {
	if err := r.Scope.Validate(); err != nil {
		return err
	}
	if r.ID == "" || len(r.ID) > 256 {
		return fmt.Errorf("%w: ID requires 1..256 bytes", ErrInvalid)
	}
	return nil
}
