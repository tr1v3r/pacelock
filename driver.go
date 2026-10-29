package pacelock

import (
	"context"
	"fmt"
)

// Driver atomically admits all named charges using backend-owned dynamic policies
// or denies without debiting any. Configuration and debt share one atomic boundary.
// Missing configuration fails closed with ErrPolicyMissing, never a static fallback.
// Implementations must be concurrency-safe, use authoritative backend time, and
// preserve charge order. Calls may synchronize on locks but must not wait for
// quota recovery or retry ambiguous mutations. Errors should carry *Error with
// NotCharged only when this invocation certainly did not debit; untyped errors
// are conservatively Unknown. All writers to a scope must share a backend and
// its synchronization protocol. Drivers do not migrate state across backends.
type Driver interface {
	// Admit completes one atomic multi-quota decision.
	Admit(context.Context, Admission) (Decision, error)
}

// Scope identifies one application's atomic quota domain. Each field contains
// 1..256 bytes. Different scopes have independent quotas.
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
