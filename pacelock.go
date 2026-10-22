// Package pacelock provides distributed, weighted GCRA rate admission using Redis.
package pacelock

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"
	"strings"
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
	ErrInvalid        = errors.New("pacelock: invalid argument")
	ErrPolicyMismatch = errors.New("pacelock: policy differs from active Redis state")
	ErrState          = errors.New("pacelock: invalid Redis state")
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
	client redis.Scripter
	prefix string
}

// New constructs a limiter. Namespace isolates applications; domain selects the
// set of quotas that can be checked together. Neither may be empty or exceed 256 bytes.
// Use a dedicated Redis client with MaxRetries: -1 to avoid ambiguous replay.
func New(client redis.Scripter, namespace, domain string) (*Limiter, error) {
	if client == nil || namespace == "" || domain == "" || len(namespace) > 256 || len(domain) > 256 {
		return nil, fmt.Errorf("%w: client, namespace and domain are required (names <= 256 bytes)", ErrInvalid)
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(namespace), namespace, domain)))
	return &Limiter{client: client, prefix: fmt.Sprintf("pacelock:v1:{%x}:", digest)}, nil
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
func (l *Limiter) Allow(ctx context.Context, rules ...Rule) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	if len(rules) == 0 || len(rules) > MaxRules {
		return Decision{}, fmt.Errorf("%w: require 1..%d rules", ErrInvalid, MaxRules)
	}
	keys := make([]string, len(rules))
	args := make([]interface{}, 0, len(rules)*3)
	seen := make(map[string]bool, len(rules))
	for i, r := range rules {
		step, err := interval(r)
		if err != nil {
			return Decision{}, err
		}
		if seen[r.Name] {
			return Decision{}, fmt.Errorf("%w: duplicate quota %q", ErrInvalid, r.Name)
		}
		seen[r.Name] = true
		keys[i] = l.key(r.Name)
		args = append(args, step, r.Burst, r.Cost)
	}
	values, err := admissionScript.Run(ctx, l.client, keys, args...).Int64Slice()
	if err != nil {
		switch {
		case strings.HasPrefix(err.Error(), "PACE_POLICY"):
			return Decision{}, fmt.Errorf("%w: %s", ErrPolicyMismatch, err)
		case strings.HasPrefix(err.Error(), "PACE_STATE"):
			return Decision{}, fmt.Errorf("%w: %s", ErrState, err)
		default:
			return Decision{}, fmt.Errorf("pacelock: Redis admission failed (debit may be unknown): %w", err)
		}
	}
	if len(values) != len(rules)+2 || (values[0] != 0 && values[0] != 1) || values[1] < 0 || values[1] > MaxHorizon.Microseconds() {
		return Decision{}, fmt.Errorf("%w: unexpected script response", ErrState)
	}
	d := Decision{Allowed: values[0] == 1, RetryAfter: time.Duration(values[1]) * time.Microsecond}
	for i, r := range rules {
		if values[i+2] != 0 {
			d.Limited = append(d.Limited, r.Name)
		}
	}
	return d, nil
}
