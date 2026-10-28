package engine

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

type Receipt = pacelock.Receipt

// AllowOnce checks and charges rules once per retained ID within this domain.
// Both allow and deny decisions are retained; replays do not extend retention.
// ID must contain 1..256 bytes. Retention must be a whole number of milliseconds
// in [1ms, 24h]. Ordered names, effective policies, costs and retention must match
// on replay. Quota names must be valid UTF-8 and all rules must satisfy Validate.
// On error, ignore Receipt and inspect OutcomeOf; a retained denial remains denied.
// Each new business attempt needs a new ID. Records can be lost on
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
	keys, args, err := l.prepare(rules)
	if err != nil {
		return Receipt{}, failure("allow_once", NotCharged, err)
	}
	// JSON records require valid UTF-8; Allow continues to accept opaque names.
	for _, r := range rules {
		if !utf8.ValidString(r.Name) {
			return Receipt{}, failure("allow_once", NotCharged, fmt.Errorf("%w: idempotent quota names must be UTF-8", ErrInvalid))
		}
	}
	args[0] = retention.Milliseconds()
	fp := (pacelock.Admission{Rules: rules, Retention: retention}).Fingerprint()
	args[1] = fp
	keys = append(l.withPolicyKeys(keys, rules), l.attemptKey(id))
	cmd := admissionScript.Run(ctx, l.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return Receipt{}, scriptFailure("allow_once", err)
	}
	values, err := cmd.Slice()
	if err != nil {
		return Receipt{}, failure("allow_once", Unknown, fmt.Errorf("%w: invalid receipt reply: %v", ErrState, err))
	}
	if len(values) != 2 {
		return Receipt{}, failure("allow_once", Unknown, fmt.Errorf("%w: invalid receipt", ErrState))
	}
	replay, ok := values[0].(int64)
	raw, textOK := values[1].(string)
	if !ok || !textOK || (replay != 0 && replay != 1) {
		return Receipt{}, failure("allow_once", Unknown, fmt.Errorf("%w: invalid receipt", ErrState))
	}
	d, err := decodeRecord(raw, fp, namesOf(rules))
	if err != nil {
		return Receipt{}, failure("allow_once", Unknown, err)
	}
	return Receipt{Decision: d, Replayed: replay == 1}, nil
}

// Lookup returns a retained complete decision. found=false does not prove no
// debit: the record may be expired, lost, or the original command still in flight.
// A pending record returns ErrOutcomeUnknown. This method never debits and does
// not extend TTL. RetryAfter is the original observation, not a countdown.
// Primary routing is intentional: a replica may not yet have the admission.
// ID has the same bounds as AllowOnce. Use Decision only when err is nil and found is true.
func (l *Limiter) Lookup(ctx context.Context, id string) (decision Decision, found bool, err error) {
	finish := l.observe(ctx, "lookup", 0)
	defer func() { finish(decision, found, err) }()
	if err := l.ready(ctx); err != nil {
		return Decision{}, false, failure("lookup", NotCharged, err)
	}
	if err := validID(id); err != nil {
		return Decision{}, false, failure("lookup", NotCharged, err)
	}
	cmd := lookupScript.Run(ctx, l.client, []string{l.attemptKey(id)})
	if cmd.Err() == redis.Nil {
		return Decision{}, false, nil
	}
	if err := cmd.Err(); err != nil {
		return Decision{}, false, scriptFailure("lookup", err)
	}
	raw, err := cmd.Text()
	if err != nil {
		return Decision{}, false, failure("lookup", Unknown, fmt.Errorf("%w: invalid lookup reply: %v", ErrState, err))
	}
	d, err := decodeRecord(raw, "", nil)
	if err != nil {
		return Decision{}, false, failure("lookup", Unknown, err)
	}
	return d, true, nil
}

// lookupSource filters expired v2 records while preserving legacy receipts.
//
//go:embed lookup.lua
var lookupSource string

// lookupScript keeps EVAL routing on the primary, even though it does not write.
var lookupScript = redis.NewScript(protocolSource + lookupSource)

// validID bounds opaque admission IDs before hashing; UTF-8 is not required.
func validID(id string) error {
	if id == "" || len(id) > 256 {
		return fmt.Errorf("%w: admission ID must contain 1..256 bytes", ErrInvalid)
	}
	return nil
}

// attemptKey separates receipt keys from quota keys within the same domain hash slot.
func (l *Limiter) attemptKey(id string) string {
	return fmt.Sprintf("%sattempt:%x", l.prefix, sha256.Sum256([]byte(id)))
}

// fingerprint binds retention and ordered effective rules to the versioned script arguments.
func fingerprint(args []interface{}) string {
	// All values are validated integers or UTF-8 strings. The empty second slot
	// is included to bind the versioned argument layout, without hash ambiguity.
	data, _ := json.Marshal(args)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// decodeRecord validates a retained result; an empty fingerprint or nil name list
// skips the corresponding caller-specific match.
func decodeRecord(raw, expected string, expectedNames []string) (Decision, error) {
	if len(raw) > 65536 || len(raw) < 67 || (raw[:3] != "P1|" && raw[:3] != "C1|") {
		return Decision{}, fmt.Errorf("%w: malformed admission record", ErrState)
	}
	fp := raw[3:67]
	if _, err := hex.DecodeString(fp); err != nil {
		return Decision{}, fmt.Errorf("%w: invalid admission fingerprint", ErrState)
	}
	if expected != "" && fp != expected {
		return Decision{}, fmt.Errorf("%w: unexpected admission fingerprint", ErrState)
	}
	if raw[:3] == "P1|" {
		if len(raw) != 67 {
			return Decision{}, fmt.Errorf("%w: malformed pending record", ErrState)
		}
		return Decision{}, ErrOutcomeUnknown
	}
	if len(raw) <= 68 || raw[67] != '|' {
		return Decision{}, fmt.Errorf("%w: missing admission result", ErrState)
	}
	var record struct {
		Names  []string `json:"names"`
		Values []int64  `json:"values"`
	}
	if err := json.Unmarshal([]byte(raw[68:]), &record); err != nil {
		return Decision{}, fmt.Errorf("%w: invalid admission result", ErrState)
	}
	if expectedNames != nil && (len(record.Names) != len(expectedNames) || !equalNames(record.Names, expectedNames)) {
		return Decision{}, fmt.Errorf("%w: unexpected admission quota names", ErrState)
	}
	return decodeDecision(record.Values, record.Names)
}

// equalNames compares ordered quota names; callers must first verify equal lengths.
func equalNames(a, b []string) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
