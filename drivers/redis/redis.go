// Package redisdriver implements Pacelock atomic admission using Redis Lua.
// It preserves the Redis v1 keys, record encoding, server clock and no-replay
// transport contract. A domain occupies one Redis Cluster hash slot.
package redisdriver

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	"github.com/tr1v3r/pacelock/drivers/redis/internal/engine"
)

// Driver is concurrency-safe when its caller-owned Scripter is concurrency-safe.
// It does not own or close the client. Concrete go-redis clients receive command
// replay protection while retaining bounded MOVED/ASK redirects. Custom clients,
// hooks, factories and proxies must preserve this contract. Ring is unsupported.
type Driver struct{ client redis.Scripter }

var _ pacelock.Driver = (*Driver)(nil)

// New validates an initialized go-redis client without connecting or changing its
// options. Configure the client before use. The constructor accepts Client,
// ClusterClient and custom Scripters with an audited no-replay policy.
func New(client redis.Scripter) (*Driver, error) {
	if _, err := engine.New(client, "validate", "validate"); err != nil {
		return nil, err
	}
	return &Driver{client: client}, nil
}

// Admit executes all dimensions and optional retained deduplication in one Lua
// operation. SuccessOnly denials write nothing; existing retained denials
// conflict. Runtime write errors may partially debit; errors retain their Outcome.
func (d *Driver) Admit(ctx context.Context, request pacelock.Admission) (pacelock.Receipt, error) {
	op := "allow"
	if request.ID != "" {
		op = "allow_once"
	}
	if request.SuccessOnly {
		op = "wait_once"
	}
	if err := request.Validate(); err != nil {
		return pacelock.Receipt{}, inputError(op, err)
	}
	if d == nil {
		return pacelock.Receipt{}, inputError(op, pacelock.ErrInvalid)
	}
	l, err := engine.New(d.client, request.Namespace, request.Domain)
	if err != nil {
		return pacelock.Receipt{}, inputError(op, err)
	}
	if request.ID != "" {
		return l.AdmitOnce(ctx, request.ID, request.Retention, request.SuccessOnly, request.Rules...)
	}
	decision, err := l.Allow(ctx, request.Rules...)
	return pacelock.Receipt{Decision: decision}, err
}

// Lookup reads the authoritative primary without debiting or extending retention.
// A missing result does not establish that the original operation did not debit.
func (d *Driver) Lookup(ctx context.Context, request pacelock.LookupRequest) (pacelock.Decision, bool, error) {
	if err := request.Validate(); err != nil {
		return pacelock.Decision{}, false, inputError("lookup", err)
	}
	if d == nil {
		return pacelock.Decision{}, false, inputError("lookup", pacelock.ErrInvalid)
	}
	l, err := engine.New(d.client, request.Namespace, request.Domain)
	if err != nil {
		return pacelock.Decision{}, false, inputError("lookup", err)
	}
	return l.Lookup(ctx, request.ID)
}

func inputError(op string, err error) error {
	return &pacelock.Error{Op: op, Outcome: pacelock.NotCharged, Err: err}
}
