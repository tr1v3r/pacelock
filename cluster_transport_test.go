package pacelock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lose exactly one successful script reply at the node-client boundary, after
// Redis has executed the real Lua. This is result fault injection, not a WAN
// outage simulation. Placing it below ClusterClient exercises its outer loop.
type clusterLostReplyHook struct {
	dropped    atomic.Bool
	executions atomic.Int64
	admissions atomic.Int64
}

func (h *clusterLostReplyHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *clusterLostReplyHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *clusterLostReplyHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && (cmd.Name() == "eval" || cmd.Name() == "evalsha") {
			h.executions.Add(1)
			if reply, ok := cmd.(*redis.Cmd); ok {
				if values, err := reply.Int64Slice(); err == nil && len(values) > 0 && values[0] == 1 {
					h.admissions.Add(1)
				}
			}
			if h.dropped.CompareAndSwap(false, true) {
				return io.EOF
			}
		}
		return err
	}
}

func TestRedisClusterLostReply(t *testing.T) {
	addresses := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS")
	if addresses == "" {
		t.Skip("set PACELOCK_REDIS_CLUSTER_ADDRS for real Cluster integration")
	}
	for _, burst := range []int64{1, 2} {
		t.Run(fmt.Sprintf("burst=%d", burst), func(t *testing.T) {
			hook := &clusterLostReplyHook{}
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: strings.Split(addresses, ","), MaxRetries: -1, MaxRedirects: -1,
				ContextTimeoutEnabled: true,
				NewClient: func(opt *redis.Options) *redis.Client {
					c := redis.NewClient(opt)
					c.AddHook(hook)
					return c
				},
			})
			defer client.Close()
			l, err := New(client, "pacelock-transport-test", fmt.Sprintf("%d-%d", time.Now().UnixNano(), burst))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := client.Del(ctx, l.key("q")).Err(); err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			r := rule("q", burst, 1)
			d, err := l.Allow(ctx, r)
			if !errors.Is(err, ErrBackend) || !errors.Is(err, io.EOF) || OutcomeOf(err) != Unknown {
				t.Fatalf("lost reply must remain unknown, not a normal decision: decision=%+v err=%v", d, err)
			}
			if hook.executions.Load() != 1 || hook.admissions.Load() != 1 {
				t.Fatalf("ambiguous command replayed: executions=%d admissions=%d", hook.executions.Load(), hook.admissions.Load())
			}
			t.Logf("lost reply: burst=%d executions=1 admissions=1 outcome=%s", burst, OutcomeOf(err))
			// This is a separate business attempt, not a recommended recovery of
			// the unknown one. It proves stable-topology routing still works and
			// that the failed public call consumed exactly one unit, not two.
			d, err = l.Allow(ctx, r)
			if err != nil || d.Allowed != (burst == 2) {
				t.Fatalf("wrong remaining capacity: decision=%+v err=%v", d, err)
			}
			t.Logf("new attempt: burst=%d allowed=%t", burst, d.Allowed)
			if burst == 2 {
				if d, err := l.Allow(ctx, r); err != nil || d.Allowed {
					t.Fatalf("quota did not exhaust after two admissions: decision=%+v err=%v", d, err)
				}
			}
		})
	}
}
