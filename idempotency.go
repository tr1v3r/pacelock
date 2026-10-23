package pacelock

import (
	"context"
	"fmt"
	"time"
	"unicode/utf8"
)

// Receipt contains the original decision and whether this call replayed it.
// Replayed admission is not permission to execute the upstream work twice.
type Receipt struct {
	Decision
	Replayed bool
}

// AllowOnce checks and charges rules once per retained ID within this domain.
// Both allow and deny decisions are retained; replays do not extend retention.
// ID must contain 1..256 bytes. Retention must be a whole number of milliseconds
// in [1ms, 24h]. Ordered names, effective policies, costs and retention must match
// on replay. Each new business attempt needs a new ID. Records can be lost on
// eviction, deletion or failover; this is bounded deduplication, not exactly-once.
func (l *Limiter) AllowOnce(ctx context.Context, id string, retention time.Duration, rules ...Rule) (receipt Receipt, err error) {
	finish := l.observe(ctx, "allow_once", len(rules))
	defer func() { finish(receipt.Decision, receipt.Replayed, err) }()
	if err := l.ready(ctx); err != nil {
		return Receipt{}, failure("allow_once", NotCharged, err)
	}
	if err := validID(id); err != nil {
		return Receipt{}, failure("allow_once", NotCharged, err)
	}
	if retention < time.Millisecond || retention > MaxHorizon || retention%time.Millisecond != 0 {
		return Receipt{}, failure("allow_once", NotCharged, fmt.Errorf("%w: retention must be whole milliseconds in [1ms, 24h]", ErrInvalid))
	}
	if err := Validate(rules...); err != nil {
		return Receipt{}, failure("allow_once", NotCharged, err)
	}
	// JSON records require valid UTF-8; Allow continues to accept opaque names.
	for _, r := range rules {
		if !utf8.ValidString(r.Name) {
			return Receipt{}, failure("allow_once", NotCharged, fmt.Errorf("%w: idempotent quota names must be UTF-8", ErrInvalid))
		}
	}
	return l.backend.allowOnce(ctx, id, retention, rules)
}

// Lookup returns a retained complete decision. found=false does not prove no
// debit: the record may be expired, lost, or the original command still in flight.
// A pending record returns ErrOutcomeUnknown. This method never debits and does
// not extend TTL. RetryAfter is the original observation, not a countdown.
// Primary routing is intentional: a replica may not yet have the admission.
func (l *Limiter) Lookup(ctx context.Context, id string) (decision Decision, found bool, err error) {
	finish := l.observe(ctx, "lookup", 0)
	defer func() { finish(decision, found, err) }()
	if err := l.ready(ctx); err != nil {
		return Decision{}, false, failure("lookup", NotCharged, err)
	}
	if err := validID(id); err != nil {
		return Decision{}, false, failure("lookup", NotCharged, err)
	}
	return l.backend.lookup(ctx, id)
}

func validID(id string) error {
	if id == "" || len(id) > 256 {
		return fmt.Errorf("%w: admission ID must contain 1..256 bytes", ErrInvalid)
	}
	return nil
}
