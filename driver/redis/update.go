package redisdriver

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

//go:embed update.lua
var updateSource string

var updateScript = redis.NewScript(protocolSource + updateSource)

// UpdateQuotas atomically installs managed policies without resetting debt.
// Transport ambiguity is Unknown; no implicit retries are performed.
func (l *limiter) UpdateQuotas(ctx context.Context, quotas ...pacelock.Quota) error {
	const op = "update_quotas"
	if err := l.ready(ctx); err != nil {
		return failure(op, NotCharged, err)
	}
	// Scope was already validated by construction; validate the quota contract here.
	request := pacelock.QuotaUpdate{Scope: pacelock.Scope{Namespace: "validated", Domain: "validated"}, Quotas: quotas}
	if err := request.Validate(); err != nil {
		return failure(op, NotCharged, err)
	}
	keys := make([]string, len(quotas))
	args := make([]interface{}, 0, 3*len(quotas))
	for i, q := range quotas {
		step, _ := (Rule{Name: q.Name, Policy: q.Policy, Cost: 1}).Interval()
		keys[i] = l.policyKey(q.Name)
		args = append(args, step.Microseconds(), q.Policy.Burst, q.Policy.Revision)
	}
	cmd := updateScript.Run(ctx, l.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return scriptFailure(op, err)
	}
	reply, err := cmd.Int64()
	if err != nil || reply != 1 {
		return failure(op, Unknown, fmt.Errorf("%w: invalid policy update reply", ErrState))
	}
	return nil
}
