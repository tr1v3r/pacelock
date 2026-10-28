package engine

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

var managedScript = redis.NewScript(protocolSource + "\nlocal managed = true\n" + admissionSource)

func (l *Limiter) policyKey(name string) string {
	return fmt.Sprintf("%spolicy:%x", l.prefix, sha256.Sum256([]byte(name)))
}
func (l *Limiter) withPolicyKeys(keys []string, rules []Rule) []string {
	for _, r := range rules {
		keys = append(keys, l.policyKey(r.Name))
	}
	return keys
}

// AdmitManaged resolves current backend policy in the same atomic admission.
func (l *Limiter) AdmitManaged(ctx context.Context, request pacelock.Admission) (receipt Receipt, err error) {
	op := "allow"
	if request.ID != "" {
		op = "allow_once"
	}
	finish := l.observe(ctx, op, len(request.Charges))
	defer func() { finish(receipt.Decision, receipt.Replayed, err) }()
	if err := l.ready(ctx); err != nil {
		return Receipt{}, failure(op, NotCharged, err)
	}
	if err := request.Validate(); err != nil {
		return Receipt{}, failure(op, NotCharged, err)
	}
	n := len(request.Charges)
	keys := make([]string, 2*n, 2*n+1)
	names := make([]string, n)
	args := []interface{}{request.Retention.Milliseconds(), request.Fingerprint()}
	for i, c := range request.Charges {
		keys[i] = l.key(c.Name)
		keys[n+i] = l.policyKey(c.Name)
		names[i] = c.Name
		args = append(args, int64(0), int64(0), c.Cost, c.Name)
	}
	if request.ID != "" {
		keys = append(keys, l.attemptKey(request.ID))
	}
	cmd := managedScript.Run(ctx, l.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return Receipt{}, scriptFailure(op, err)
	}
	if request.ID == "" {
		values, err := cmd.Int64Slice()
		if err != nil {
			return Receipt{}, failure(op, Unknown, fmt.Errorf("%w: invalid decision reply", ErrState))
		}
		d, err := decodeDecision(values, names)
		if err != nil {
			return Receipt{}, failure(op, Unknown, err)
		}
		return Receipt{Decision: d}, nil
	}
	values, err := cmd.Slice()
	if err != nil || len(values) != 2 {
		return Receipt{}, failure(op, Unknown, fmt.Errorf("%w: invalid receipt reply", ErrState))
	}
	replay, ok := values[0].(int64)
	raw, textOK := values[1].(string)
	if !ok || !textOK || (replay != 0 && replay != 1) {
		return Receipt{}, failure(op, Unknown, fmt.Errorf("%w: invalid receipt", ErrState))
	}
	d, err := decodeRecord(raw, request.Fingerprint(), names)
	if err != nil {
		return Receipt{}, failure(op, Unknown, err)
	}
	return Receipt{Decision: d, Replayed: replay == 1}, nil
}
