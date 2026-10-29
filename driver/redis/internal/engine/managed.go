package engine

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/tr1v3r/pacelock"
)

func (l *Limiter) policyKey(name string) string {
	return fmt.Sprintf("%spolicy:%x", l.prefix, sha256.Sum256([]byte(name)))
}

// Admit resolves current backend policy in the same atomic admission.
func (l *Limiter) Admit(ctx context.Context, request pacelock.Admission) (decision Decision, err error) {
	op := "allow"
	finish := l.observe(ctx, op, len(request.Charges))
	defer func() { finish(decision, err) }()
	if err := l.ready(ctx); err != nil {
		return Decision{}, failure(op, NotCharged, err)
	}
	if err := request.Validate(); err != nil {
		return Decision{}, failure(op, NotCharged, err)
	}
	n := len(request.Charges)
	keys := make([]string, 2*n)
	names := make([]string, n)
	args := make([]interface{}, 0, n)
	for i, c := range request.Charges {
		keys[i] = l.key(c.Name)
		keys[n+i] = l.policyKey(c.Name)
		names[i] = c.Name
		args = append(args, c.Cost)
	}
	cmd := admissionScript.Run(ctx, l.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return Decision{}, scriptFailure(op, err)
	}
	values, err := cmd.Int64Slice()
	if err != nil {
		return Decision{}, failure(op, Unknown, fmt.Errorf("%w: invalid decision reply", ErrState))
	}
	d, err := decodeDecision(values, names)
	if err != nil {
		return Decision{}, failure(op, Unknown, err)
	}
	return d, nil
}
