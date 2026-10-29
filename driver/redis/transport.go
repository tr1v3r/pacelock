package redisdriver

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// validateTransport checks normalized options, not the values originally passed
// to go-redis constructors. It must not connect, instantiate Cluster nodes, or
// change caller-owned options. Wrappers, hooks and custom node factories remain
// the caller's responsibility: redis.Scripter cannot expose their replay policy.
func validateTransport(client redis.Scripter) error {
	switch c := client.(type) {
	case *redis.Ring:
		return fmt.Errorf("%w: redis.Ring is unsupported; use redis.Client or redis.ClusterClient", ErrInvalid)
	case *redis.Client:
		// NoRetry protects script commands independently of the client retry budget.
		// Negative normalized values are invalid (input -1 normalizes to zero).
		if c.Options() == nil || c.Options().MaxRetries < 0 {
			return fmt.Errorf("%w: redis.Client requires a nonnegative normalized retry budget", ErrInvalid)
		}
	case *redis.ClusterClient:
		// NoRetry suppresses ambiguous replay, while the redirect budget permits
		// safe MOVED/ASK recovery. Only reject unsupported negative budgets.
		options := c.Options()
		if options == nil || options.MaxRedirects < 0 || options.MaxRetries < -1 {
			return fmt.Errorf("%w: redis.ClusterClient requires MaxRetries >= -1 and a nonnegative normalized redirect budget", ErrInvalid)
		}
	}
	return nil
}

// noRetryScripter adapts Pacelock's internal EVAL/EVALSHA calls for the supported
// go-redis clients. It retains Script.Run cache-miss handling and safe MOVED/ASK
// redirects while preventing ambiguous replay, including within ASK pipelines.
type noRetryScripter struct {
	redis.Scripter
	process func(context.Context, redis.Cmder) error
}

// noRetryCmd changes only the command replay policy. Routing and execution
// remain owned by go-redis.
type noRetryCmd struct{ *redis.Cmd }

func (noRetryCmd) NoRetry() bool { return true }

// safeScripter deliberately recognizes concrete clients: a custom transport
// exposing Process is not necessarily guaranteed to honor NoRetry.
func safeScripter(c redis.Scripter) redis.Scripter {
	switch c := c.(type) {
	case *redis.Client:
		return noRetryScripter{c, c.Process}
	case *redis.ClusterClient:
		return noRetryScripter{c, c.Process}
	default:
		return c // Custom transports own their replay policy.
	}
}
func (s noRetryScripter) send(ctx context.Context, command, script string, keys []string, args ...interface{}) *redis.Cmd {
	values := make([]interface{}, 0, 3+len(keys)+len(args))
	values = append(values, command, script, len(keys))
	for _, key := range keys {
		values = append(values, key)
	}
	values = append(values, args...)
	cmd := redis.NewCmd(ctx, values...)
	cmd.SetFirstKeyPos(3)
	err := s.process(ctx, noRetryCmd{cmd})
	cmd.SetErr(err)
	return cmd
}
func (s noRetryScripter) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return s.send(ctx, "eval", script, keys, args...)
}
func (s noRetryScripter) EvalSha(ctx context.Context, hash string, keys []string, args ...interface{}) *redis.Cmd {
	return s.send(ctx, "evalsha", hash, keys, args...)
}
