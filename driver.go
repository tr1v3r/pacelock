package pacelock

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"
)

// Driver atomically admits all named charges or denies without debiting any.
// Implementations must be concurrency-safe, use authoritative backend time, and
// preserve effective policy checks, ordered rules, bounded ID deduplication and
// original retry durations. Calls may synchronize on locks but must not wait for
// quota recovery. They must not replay ambiguous mutations. Errors should carry
// *Error with NotCharged only when this invocation certainly did not debit;
// untyped errors are conservatively Unknown. Lookup must not mutate quota or
// refresh retention. A missing lookup never establishes zero debit.
// All writers to a scope must share a backend and its synchronization protocol.
// Drivers do not migrate state or deduplicate across different backends.
// Admit must honor Admission.SuccessOnly: denials create no retained record,
// successes retain the ordinary fingerprint, and existing retained denials
// return ErrConflict / NotCharged. Redis-style partial writes must retain a
// pending marker. Denied success-only attempts do not bind ID parameters.
type Driver interface {
	// Admit completes one atomic multi-quota decision and optional retained receipt.
	Admit(context.Context, Admission) (Receipt, error)
	// Lookup reads a retained result without extending its lifetime.
	Lookup(context.Context, LookupRequest) (Decision, bool, error)
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

// Admission requests a single atomic operation. An empty ID requires Retention=0
// and means ordinary, non-idempotent admission. With a nonempty ID, SuccessOnly
// selects which decisions to retain. Drivers must not retain or mutate the
// caller's Rules slice.
type Admission struct {
	// Scope identifies the atomic quota and ID domain.
	Scope
	// Rules contains ordered, uniquely named charges.
	Rules []Rule
	// ID is empty for Allow or contains 1..256 bytes for retained admission.
	ID string
	// Retention is zero for Allow or whole milliseconds in [1ms,24h].
	Retention time.Duration
	// SuccessOnly requires an ID and retains only successful admission. Denials
	// neither debit nor create/refresh a receipt, so the ID can wait for recovery.
	// Successful or pending admission binds the ID's parameters; success retention
	// starts at that backend decision. Replays never extend retention. An existing
	// retained denial returns ErrConflict / NotCharged and must not be replaced.
	// False retains both outcomes for an ID, preserving AllowOnce behavior.
	SuccessOnly bool
}

// Validate checks the complete driver request without backend access.
func (a Admission) Validate() error {
	if err := a.Scope.Validate(); err != nil {
		return err
	}
	if err := Validate(a.Rules...); err != nil {
		return err
	}
	if a.ID == "" {
		if a.Retention != 0 || a.SuccessOnly {
			return fmt.Errorf("%w: retention and success-only admission require ID", ErrInvalid)
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
// SuccessOnly is excluded so AllowOnce and WaitOnce can replay the same success.
func (a Admission) Fingerprint() string {
	args := []any{a.Retention.Milliseconds(), ""}
	for _, r := range a.Rules {
		step, _ := r.Interval()
		args = append(args, step.Microseconds(), r.Burst, r.Cost, r.Name)
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
