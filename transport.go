package pacelock

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
		// NewClient converts MaxRetries: -1 to 0; its default becomes 3.
		if c.Options() == nil || c.Options().MaxRetries != 0 {
			return fmt.Errorf("%w: redis.Client requires MaxRetries: -1", ErrInvalid)
		}
	case *redis.ClusterClient:
		// NoRetry suppresses ambiguous replay, while the redirect budget permits
		// safe MOVED/ASK recovery. Retain the node retry configuration contract.
		options := c.Options()
		if options == nil || options.MaxRedirects < 0 || options.MaxRetries != -1 {
			return fmt.Errorf("%w: redis.ClusterClient requires MaxRetries: -1 and a nonnegative normalized redirect budget", ErrInvalid)
		}
	}
	return nil
}

// markedScripter retains Script.Run cache-miss handling while preventing ambiguous
// replay at both go-redis's node and Cluster layers, including ASK pipelines.
type markedScripter struct {
	redis.Scripter
	process func(context.Context, redis.Cmder) error
}
type admissionCommand struct{ *redis.Cmd }

func (admissionCommand) NoRetry() bool { return true }
func safeScripter(c redis.Scripter) redis.Scripter {
	switch c := c.(type) {
	case *redis.Client:
		return markedScripter{c, c.Process}
	case *redis.ClusterClient:
		return markedScripter{c, c.Process}
	default:
		return c // Custom transports own their replay policy.
	}
}
func (s markedScripter) send(ctx context.Context, command, script string, keys []string, args ...interface{}) *redis.Cmd {
	values := make([]interface{}, 0, 3+len(keys)+len(args))
	values = append(values, command, script, len(keys))
	for _, key := range keys {
		values = append(values, key)
	}
	values = append(values, args...)
	cmd := redis.NewCmd(ctx, values...)
	cmd.SetFirstKeyPos(3)
	err := s.process(ctx, admissionCommand{cmd})
	cmd.SetErr(err)
	return cmd
}
func (s markedScripter) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return s.send(ctx, "eval", script, keys, args...)
}
func (s markedScripter) EvalSha(ctx context.Context, hash string, keys []string, args ...interface{}) *redis.Cmd {
	return s.send(ctx, "evalsha", hash, keys, args...)
}
