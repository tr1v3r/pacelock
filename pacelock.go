// Package pacelock provides distributed, weighted GCRA rate admission using Redis.
//
// Allow immediately admits known costs across named quotas. AllowOnce adds
// bounded deduplication and Lookup recovers retained admission results. Neither
// method queues work, owns concurrency, refunds capacity or bills usage.
//
// Use one namespace/domain for all quotas requiring atomic admission and explicitly
// handle unknown debit outcomes. Built-in go-redis clients receive per-command
// replay protection. The client and business execution lifecycle belong to the caller.
package pacelock

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// MaxRules bounds the work performed by one atomic Redis script.
	MaxRules = 32
	// MaxHorizon bounds both policy periods and the time needed to refill a burst.
	MaxHorizon = 24 * time.Hour
)

var (
	// ErrInvalid reports invalid input before dispatch.
	ErrInvalid = errors.New("pacelock: invalid argument")
	// ErrPolicyMismatch reports an effective policy conflict with active quota state.
	ErrPolicyMismatch = errors.New("pacelock: policy differs from active Redis state")
	// ErrState reports malformed Redis state or a malformed response. Check OutcomeOf.
	ErrState = errors.New("pacelock: invalid Redis state")
	// ErrConflict reports reuse of a retained admission ID with different parameters.
	ErrConflict = errors.New("pacelock: admission ID used with different parameters")
	// ErrOutcomeUnknown reports a retained admission without a complete result.
	ErrOutcomeUnknown = errors.New("pacelock: admission outcome unknown")
	// ErrBackend wraps an unclassified Redis or transport failure.
	ErrBackend = errors.New("pacelock: Redis backend failure")
)

// Rule charges Cost units to a named quota. Rate units replenish per Period.
// Burst is the total immediately available capacity, including the first unit.
// The same quota name must use the same policy across all callers in a domain.
type Rule struct {
	Name   string
	Rate   int64
	Period time.Duration
	Burst  int64
	Cost   int64
}

// Decision describes an admission attempt. RetryAfter is advisory, not a reservation.
// On denial, Limited contains all dimensions that blocked admission.
// On error the returned Decision must not be used.
type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
	Limited    []string
}

// Limiter is safe for concurrent use when its Redis client is safe for concurrent use.
// It neither owns nor closes the client. One domain is one Redis Cluster hash slot.
type Limiter struct {
	client   redis.Scripter
	prefix   string
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

//go:embed gcra.lua
var source string
var admissionScript = redis.NewScript(source)

func (l *Limiter) key(name string) string {
	return fmt.Sprintf("%s%x", l.prefix, sha256.Sum256([]byte(name)))
}

func interval(r Rule) (int64, error) {
	if r.Name == "" || len(r.Name) > 256 || r.Rate <= 0 || r.Period <= 0 || r.Period > MaxHorizon || r.Burst <= 0 || r.Cost <= 0 || r.Cost > r.Burst {
		return 0, fmt.Errorf("%w: invalid quota %q; require positive rate/period/burst and 0 < cost <= burst", ErrInvalid, r.Name)
	}
	// Round upward to whole microseconds, never admitting faster than configured.
	us := r.Period.Nanoseconds() / 1000
	if r.Period.Nanoseconds()%1000 != 0 {
		us++
	}
	step := us / r.Rate
	if us%r.Rate != 0 {
		step++
	}
	if r.Burst > MaxHorizon.Microseconds()/step {
		return 0, fmt.Errorf("%w: burst refill horizon exceeds 24 hours for %q", ErrInvalid, r.Name)
	}
	return step, nil
}

// Allow immediately checks and charges every rule or denies without charging any.
// It does not wait, queue work, reserve future capacity, or retry transport errors.
// A timeout or cancellation after dispatch can leave the debit outcome unknown.
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
func Validate(rules ...Rule) error {
	if len(rules) == 0 || len(rules) > MaxRules {
		return fmt.Errorf("%w: require 1..%d rules", ErrInvalid, MaxRules)
	}
	seen := make(map[string]bool, len(rules))
	for _, r := range rules {
		if _, err := interval(r); err != nil {
			return err
		}
		if seen[r.Name] {
			return fmt.Errorf("%w: duplicate quota %q", ErrInvalid, r.Name)
		}
		seen[r.Name] = true
	}
	return nil
}

func (l *Limiter) ready(ctx context.Context) error {
	if ctx == nil || l == nil || l.prefix == "" || nilClient(l.client) {
		return fmt.Errorf("%w: initialized limiter and non-nil context required", ErrInvalid)
	}
	return ctx.Err()
}

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

func namesOf(rules []Rule) []string {
	names := make([]string, len(rules))
	for i := range rules {
		names[i] = rules[i].Name
	}
	return names
}

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
