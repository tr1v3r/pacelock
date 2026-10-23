package pacelock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// replyLoss discards real response bytes below go-redis's retry loops. Hooks
// outside Process alone cannot detect a replay inside a node's Process call.
// Each fixture runs sequentially with warmed connections and no concurrent work.
type replyLoss struct {
	armed   atomic.Bool
	dropped atomic.Int64
}
type replyLossConn struct {
	net.Conn
	loss   *replyLoss
	broken atomic.Bool
}

func (c *replyLossConn) Read(p []byte) (int, error) {
	if c.broken.Load() {
		return 0, io.EOF
	}
	n, err := c.Conn.Read(p)
	if n > 0 && c.loss.armed.CompareAndSwap(true, false) {
		c.loss.dropped.Add(1)
		// Keep EOF sticky: a buffered reader may probe the connection again.
		c.broken.Store(true)
		return 0, io.EOF
	}
	return n, err
}
func (l *replyLoss) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	c, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return &replyLossConn{Conn: c, loss: l}, nil
}

type armReplyLoss struct {
	loss    *replyLoss
	command string
	once    atomic.Bool
}

func (*armReplyLoss) DialHook(next redis.DialHook) redis.DialHook { return next }
func (*armReplyLoss) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *armReplyLoss) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == h.command && h.once.CompareAndSwap(false, true) {
			h.loss.armed.Store(true)
		}
		return next(ctx, cmd)
	}
}

func TestRedisRetryBudgetsLostReply(t *testing.T) {
	for _, backend := range []string{"standalone", "cluster"} {
		for _, retries := range []int{-1, 0, 2} {
			for _, command := range []string{"evalsha", "eval"} {
				t.Run(fmt.Sprintf("%s/retries=%d/%s", backend, retries, command), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					loss := &replyLoss{}
					hook := &armReplyLoss{loss: loss, command: command}
					var client redis.UniversalClient
					var warm func(func(*redis.Client) error) error
					if backend == "standalone" {
						addr := os.Getenv("PACELOCK_REDIS_ADDR")
						if addr == "" {
							t.Skip("standalone fixture required")
						}
						c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: retries, PoolSize: 1, Dialer: loss.dial, ContextTimeoutEnabled: true})
						c.AddHook(hook)
						client = c
						warm = func(fn func(*redis.Client) error) error { return fn(c) }
					} else {
						addr := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS")
						if addr == "" {
							t.Skip("Cluster fixture required")
						}
						c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addr, ","), MaxRetries: retries, MaxRedirects: 3, PoolSize: 1, Dialer: loss.dial, ContextTimeoutEnabled: true,
							NewClient: func(o *redis.Options) *redis.Client { node := redis.NewClient(o); node.AddHook(hook); return node },
						})
						client = c
						warm = func(fn func(*redis.Client) error) error {
							return c.ForEachMaster(ctx, func(_ context.Context, node *redis.Client) error { return fn(node) })
						}
					}
					defer client.Close()
					if err := warm(func(node *redis.Client) error {
						if err := node.Command(ctx).Err(); err != nil {
							return err
						}
						if command == "eval" {
							return node.ScriptFlush(ctx).Err()
						}
						return admissionScript.Load(ctx, node).Err()
					}); err != nil {
						t.Fatal(err)
					}
					limiter, err := New(client, "retry-budget", fmt.Sprint(time.Now().UnixNano()))
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := client.Del(cleanup, limiter.key("q")).Err(); err != nil {
							t.Error(err)
						}
					}()
					r := rule("q", 2, 1)
					_, err = limiter.Allow(ctx, r)
					if !errors.Is(err, io.EOF) || !errors.Is(err, ErrBackend) || OutcomeOf(err) != Unknown || loss.dropped.Load() != 1 {
						t.Fatalf("lost reply: dropped=%d outcome=%v err=%v", loss.dropped.Load(), OutcomeOf(err), err)
					}
					// Exactly one debit must have occurred: one further admission, then denial.
					for _, want := range []bool{true, false} {
						d, err := limiter.Allow(ctx, r)
						if err != nil || d.Allowed != want {
							t.Fatalf("remaining quota: want allowed=%t got %+v err=%v", want, d, err)
						}
					}
				})
			}
		}
	}
}
