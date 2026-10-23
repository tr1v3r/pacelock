package pacelock

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// This file defines the existing Redis wire/storage protocol: argument
// fingerprints, decision/record decoding and SDK-owned Lua error tokens.
// Moving these helpers must not change persisted records or error outcomes.

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

func fingerprint(args []interface{}) string {
	// All values are validated integers or UTF-8 strings. The empty second slot
	// is included to bind the versioned argument layout, without hash ambiguity.
	data, _ := json.Marshal(args)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

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

func equalNames(a, b []string) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func scriptFailure(op string, err error) error {
	// Only exact SDK-owned pre-write error tokens are known not to debit.
	var server redis.Error
	token := ""
	if errors.As(err, &server) {
		token, _, _ = strings.Cut(strings.TrimPrefix(server.Error(), "ERR "), " ")
	}
	switch token {
	case "PACE_POLICY":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrPolicyMismatch, err))
	case "PACE_STATE":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrState, err))
	case "PACE_CONFLICT":
		return failure(op, NotCharged, fmt.Errorf("%w: %w", ErrConflict, err))
	case "PACE_PENDING":
		return failure(op, Unknown, fmt.Errorf("%w: %w", ErrOutcomeUnknown, err))
	case "PACE_RECORD":
		return failure(op, Unknown, fmt.Errorf("%w: %w", ErrState, err))
	default:
		return failure(op, Unknown, fmt.Errorf("%w: %w", ErrBackend, err))
	}
}
