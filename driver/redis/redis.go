// Package redisdriver implements Pacelock atomic admission using Redis Lua.
// It commits quotas in one MSET, with logical expiry and
// caller-scheduled bounded cleanup. New policies and debt use P1 and D1
// respectively; P3/D3 and legacy tuple debt remain readable for upgrades. One domain
// occupies one Redis Cluster hash slot; transport commands prevent ambiguous replay.
package redisdriver

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	"github.com/tr1v3r/pacelock/driver/redis/internal/engine"
)

// Driver is concurrency-safe when its caller-owned Scripter is concurrency-safe.
// It does not own or close the client. Concrete go-redis clients receive command
// replay protection while retaining bounded MOVED/ASK redirects. Custom clients,
// hooks, factories and proxies must preserve this contract. Ring is unsupported.
type Driver struct{ client redis.Scripter }

var _ pacelock.Driver = (*Driver)(nil)
var _ pacelock.QuotaDriver = (*Driver)(nil)

// Admit reads authoritative policies and atomically admits named costs.
func (d *Driver) Admit(ctx context.Context, request pacelock.Admission) (pacelock.Decision, error) {
	op := "allow"
	if err := request.Validate(); err != nil {
		return pacelock.Decision{}, inputError(op, err)
	}
	if d == nil {
		return pacelock.Decision{}, inputError(op, pacelock.ErrInvalid)
	}
	l, err := engine.New(d.client, request.Namespace, request.Domain)
	if err != nil {
		return pacelock.Decision{}, inputError(op, err)
	}
	return l.Admit(ctx, request)
}

// UpdateQuotas atomically changes managed policies while preserving all debt.
// Unknown failures may have committed; explicitly retrying the same revision and
// effective policy is idempotent. The transport never retries ambiguous commands.
func (d *Driver) UpdateQuotas(ctx context.Context, request pacelock.QuotaUpdate) error {
	if err := request.Validate(); err != nil {
		return inputError("update_quotas", err)
	}
	if d == nil {
		return inputError("update_quotas", pacelock.ErrInvalid)
	}
	l, err := engine.New(d.client, request.Namespace, request.Domain)
	if err != nil {
		return inputError("update_quotas", err)
	}
	return l.UpdateQuotas(ctx, request.Quotas...)
}

// New validates an initialized go-redis client without connecting or changing its
// options. Configure the client before use. The constructor accepts Client,
// ClusterClient and custom Scripters with an audited no-replay policy.
func New(client redis.Scripter) (*Driver, error) {
	if _, err := engine.New(client, "validate", "validate"); err != nil {
		return nil, err
	}
	return &Driver{client: client}, nil
}

func inputError(op string, err error) error {
	return &pacelock.Error{Op: op, Outcome: pacelock.NotCharged, Err: err}
}

// MaxPruneKeys bounds candidate values checked by one Prune call.
const MaxPruneKeys = engine.MaxPruneKeys

// PruneCursor is an opaque scope-bound discovery position. Start with its zero
// value and retain the returned cursor until Done; restart at zero for a new cycle.
type PruneCursor = engine.PruneCursor

// PruneResult reports candidate checks, deletions and a cursor. Ignore it on error.
type PruneResult = engine.PruneResult

// Prune checks at most limit candidate values and performs at most one SCAN page
// on the scope's current primary. Limit must be 1..MaxPruneKeys. SCAN COUNT is a
// hint, so discovery has no hard latency bound and overflow is buffered in Cursor.
// Logically expired values are deleted only after an atomic recheck. Malformed
// candidates abort the batch; active debt and legacy attempt receipts are preserved.
// Recovered legacy tuple debt is prunable. The application schedules repeated cycles;
// constructors and admissions never start cleanup goroutines.
func (d *Driver) Prune(ctx context.Context, scope pacelock.Scope, cursor PruneCursor, limit int) (PruneResult, error) {
	if err := scope.Validate(); err != nil {
		return PruneResult{}, inputError("prune", err)
	}
	if d == nil {
		return PruneResult{}, inputError("prune", pacelock.ErrInvalid)
	}
	l, err := engine.New(d.client, scope.Namespace, scope.Domain)
	if err != nil {
		return PruneResult{}, inputError("prune", err)
	}
	return l.Prune(ctx, cursor, limit)
}
