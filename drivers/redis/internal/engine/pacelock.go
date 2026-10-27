// Package pacelock provides distributed, weighted GCRA rate admission using Redis.
//
// Allow immediately admits known costs across named quotas. AllowOnce adds
// bounded deduplication and Lookup recovers retained admission results. Neither
// method queues work, owns concurrency, refunds capacity or bills usage.
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
	ErrConflict       = pacelock.ErrConflict
	ErrOutcomeUnknown = pacelock.ErrOutcomeUnknown
	ErrBackend        = pacelock.ErrBackend
)

type Rule = pacelock.Rule
type Decision = pacelock.Decision

// Limiter is safe for concurrent use when its Redis client is safe for concurrent use.
// It neither owns nor closes the client. One domain is one Redis Cluster hash slot.
// Construct it with New or NewWithOptions; nil/zero receivers and nil operation
// contexts return ErrInvalid wrapped in Error before dispatch.
type Limiter struct {
	// client is the caller-owned transport, configured to avoid command replay.
	client redis.Scripter
	// prefix hashes namespace and domain into a shared Redis Cluster hash tag.
	prefix string
	// observer is an optional synchronous callback shared by concurrent invocations.
	observer Observer
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
func New(client redis.Scripter, namespace, domain string) (*Limiter, error) {
	return NewWithOptions(client, namespace, domain, Options{})
}

// NewWithOptions constructs a limiter with optional synchronous observation.
// The client, namespace and domain requirements are the same as New.
// It does not connect to Redis or change the supplied client's configuration.
func NewWithOptions(client redis.Scripter, namespace, domain string, options Options) (*Limiter, error) {
	if nilClient(client) || namespace == "" || domain == "" || len(namespace) > 256 || len(domain) > 256 {
		return nil, fmt.Errorf("%w: client, namespace and domain are required (names <= 256 bytes)", ErrInvalid)
	}
	if err := validateTransport(client); err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(namespace), namespace, domain)))
	return &Limiter{client: safeScripter(client), prefix: fmt.Sprintf("pacelock:v1:{%x}:", digest), observer: options.Observer}, nil
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

// protocolSource contains shared logical-expiry and legacy-format readers.
//
//go:embed protocol.lua
var protocolSource string

// admissionSource computes the decision before its only mutation, MSET.
//
//go:embed gcra.lua
var admissionSource string

// source is the complete script, also used by model and failure-injection tests.
var source = protocolSource + admissionSource

// admissionScript caches the script hash and falls back to EVAL on NOSCRIPT.
var admissionScript = redis.NewScript(source)

// key hashes a quota name while preserving the domain hash slot.
func (l *Limiter) key(name string) string {
	return fmt.Sprintf("%s%x", l.prefix, sha256.Sum256([]byte(name)))
}

// interval validates a rule and returns its conservative per-unit refill time in microseconds.
func interval(r Rule) (int64, error) {
	d, err := r.Interval()
	return d.Microseconds(), err
}

// Allow immediately checks and charges every rule or denies without charging any.
// It does not wait, queue work, reserve future capacity, or retry transport errors.
// Rules must satisfy Validate. A denial returns Allowed=false with a nil error.
// A timeout or cancellation after dispatch can leave the debit outcome unknown.
// On error, ignore Decision and inspect OutcomeOf before deciding how to recover.
func (l *Limiter) Allow(ctx context.Context, rules ...Rule) (decision Decision, err error) {
	finish := l.observe(ctx, "allow", len(rules))
	defer func() { finish(decision, false, err) }()
	if err := l.ready(ctx); err != nil {
		return Decision{}, failure("allow", NotCharged, err)
	}
	keys, args, err := l.prepare(rules)
	if err != nil {
		return Decision{}, failure("allow", NotCharged, err)
	}
	cmd := admissionScript.Run(ctx, l.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return Decision{}, scriptFailure("allow", err)
	}
	values, err := cmd.Int64Slice()
	if err != nil {
		return Decision{}, failure("allow", Unknown, fmt.Errorf("%w: invalid script reply: %v", ErrState, err))
	}
	d, err := decodeDecision(values, namesOf(rules))
	if err != nil {
		return Decision{}, failure("allow", Unknown, err)
	}
	return d, nil
}

// Validate checks all rule and effective-policy bounds without contacting Redis.
// Supply 1..MaxRules rules with unique names. It wraps ErrInvalid on failure;
// it cannot detect conflicts with policies already stored in Redis.
func Validate(rules ...Rule) error { return pacelock.Validate(rules...) }

// ready rejects unusable receivers and contexts before any backend request.
func (l *Limiter) ready(ctx context.Context) error {
	if ctx == nil || l == nil || l.prefix == "" || nilClient(l.client) {
		return fmt.Errorf("%w: initialized limiter and non-nil context required", ErrInvalid)
	}
	return ctx.Err()
}

// prepare validates ordered rules and encodes keys and arguments for ordinary admission.
func (l *Limiter) prepare(rules []Rule) ([]string, []interface{}, error) {
	if err := Validate(rules...); err != nil {
		return nil, nil, err
	}
	keys := make([]string, len(rules))
	args := make([]interface{}, 0, 2+len(rules)*4)
	args = append(args, int64(0), "")
	for i, r := range rules {
		step, _ := interval(r)
		keys[i] = l.key(r.Name)
		args = append(args, step, r.Burst, r.Cost, r.Name)
	}
	return keys, args, nil
}

// namesOf copies quota names in request order for response validation.
func namesOf(rules []Rule) []string {
	names := make([]string, len(rules))
	for i := range rules {
		names[i] = rules[i].Name
	}
	return names
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
