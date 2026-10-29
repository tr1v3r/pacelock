// Package engine implements the Redis driver's scoped Lua operations.
//
// Admit immediately admits known costs across named quotas. It never queues
// work, owns concurrency, refunds capacity or bills usage.
//
// Use one namespace/domain for all quotas requiring atomic admission and explicitly
// handle unknown debit outcomes. Built-in go-redis clients receive per-command
// replay protection. The client and business execution lifecycle belong to the caller.
package engine

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"fmt"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

// Shared contracts come from the backend-independent core.
const MaxRules = pacelock.MaxRules
const MaxHorizon = pacelock.MaxHorizon

var (
	ErrInvalid        = pacelock.ErrInvalid
	ErrPolicyMismatch = pacelock.ErrPolicyMismatch
	ErrState          = pacelock.ErrState
	ErrBackend        = pacelock.ErrBackend
)

type Rule = pacelock.Rule
type Decision = pacelock.Decision

// Limiter is safe for concurrent use when its Redis client is safe for concurrent use.
// It neither owns nor closes the client. One domain is one Redis Cluster hash slot.
// Construct it with New; nil/zero receivers and nil operation contexts return
// ErrInvalid wrapped in Error before dispatch.
type Limiter struct {
	// client is the caller-owned transport, configured to avoid command replay.
	client redis.Scripter
	// prefix hashes namespace and domain into a shared Redis Cluster hash tag.
	prefix string
}

// New constructs a limiter. Namespace isolates applications; domain selects the
// set of quotas that can be checked together. Neither may be empty or exceed 256 bytes.
// Concrete go-redis clients support default, positive and disabled MaxRetries
// budgets. SDK script commands carry NoRetry to prevent ambiguous replay while
// preserving bounded MOVED/ASK recovery. Other commands retain the client's retry
// policy. Unsupported negative budgets return ErrInvalid. go-redis clients must
// be initialized with their constructors; their zero values are unsupported.
// Custom Scripters, hooks and node factories must preserve the no-replay contract.
// redis.Ring is unsupported and returns ErrInvalid.
// New does not connect to Redis or change the supplied client's configuration.
func New(client redis.Scripter, namespace, domain string) (*Limiter, error) {
	if nilClient(client) || namespace == "" || domain == "" || len(namespace) > 256 || len(domain) > 256 {
		return nil, fmt.Errorf("%w: client, namespace and domain are required (names <= 256 bytes)", ErrInvalid)
	}
	if err := validateTransport(client); err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(namespace), namespace, domain)))
	return &Limiter{client: safeScripter(client), prefix: fmt.Sprintf("pacelock:{%x}:", digest)}, nil
}

// nilClient detects nil interfaces and typed nil transports without invoking them.
func nilClient(client redis.Scripter) bool {
	if client == nil {
		return true
	}
	v := reflect.ValueOf(client)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

// protocolSource contains shared quota and policy readers.
//
//go:embed protocol.lua
var protocolSource string

// admissionSource computes the decision before its only mutation, MSET.
//
//go:embed gcra.lua
var admissionSource string

// source is the complete script, also used by model and failure-injection tests.
var source = protocolSource + "\n" + admissionSource

// admissionScript caches the script hash and falls back to EVAL on NOSCRIPT.
var admissionScript = redis.NewScript(source)

// key hashes a quota name while preserving the domain hash slot.
func (l *Limiter) key(name string) string {
	return fmt.Sprintf("%s%x", l.prefix, sha256.Sum256([]byte(name)))
}

// ready rejects unusable receivers and contexts before any backend request.
func (l *Limiter) ready(ctx context.Context) error {
	if ctx == nil || l == nil || l.prefix == "" || nilClient(l.client) {
		return fmt.Errorf("%w: initialized limiter and non-nil context required", ErrInvalid)
	}
	return ctx.Err()
}

// decodeDecision validates the script result and converts microsecond waits to durations.
func decodeDecision(values []int64, names []string) (Decision, error) {
	if len(names) == 0 || len(names) > MaxRules || len(values) != len(names)+2 || (values[0] != 0 && values[0] != 1) || values[1] < 0 || values[1] > MaxHorizon.Microseconds() {
		return Decision{}, fmt.Errorf("%w: unexpected script response", ErrState)
	}
	d := Decision{Allowed: values[0] == 1, RetryAfter: time.Duration(values[1]) * time.Microsecond}
	seen := make(map[string]bool, len(names))
	for i, name := range names {
		if name == "" || len(name) > 256 || seen[name] || (values[i+2] != 0 && values[i+2] != 1) {
			return Decision{}, fmt.Errorf("%w: invalid quota in script response", ErrState)
		}
		seen[name] = true
		if values[i+2] == 1 {
			d.Limited = append(d.Limited, name)
		}
	}
	if (d.Allowed && (d.RetryAfter != 0 || len(d.Limited) != 0)) || (!d.Allowed && (d.RetryAfter == 0 || len(d.Limited) == 0)) {
		return Decision{}, fmt.Errorf("%w: inconsistent script response", ErrState)
	}
	return d, nil
}
