package redisdriver

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewClientRetryOptions(t *testing.T) {
	for _, retries := range []int{-2, -1, 0, 1, 3} {
		t.Run(fmt.Sprintf("retries=%d", retries), func(t *testing.T) {
			var dials atomic.Int64
			options := &redis.Options{Addr: "127.0.0.1:1", MaxRetries: retries,
				Dialer: func(context.Context, string, string) (net.Conn, error) {
					dials.Add(1)
					return nil, errors.New("constructor must not dial")
				},
			}
			client := redis.NewClient(options)
			defer client.Close()
			normalized := client.Options().MaxRetries
			wantNormalized := retries
			if retries == -1 {
				wantNormalized = 0
			} else if retries == 0 {
				wantNormalized = 3
			}
			if normalized != wantNormalized {
				t.Fatalf("unexpected go-redis normalization: %d", normalized)
			}
			l, err := newLimiter(client, "transport-test", "domain")
			if retries >= -1 {
				if err != nil || l == nil {
					t.Fatalf("safe client rejected: %v", err)
				}
			} else if !errors.Is(err, ErrInvalid) || l != nil {
				t.Fatalf("unsafe client accepted: limiter=%v err=%v", l, err)
			}
			if dials.Load() != 0 || client.Options().MaxRetries != normalized || options.MaxRetries != retries {
				t.Fatal("constructor dialed or changed retry options")
			}
		})
	}
}

func TestNewClusterRetryOptions(t *testing.T) {
	for _, redirects := range []int{-2, -1, 0, 1, 3} {
		for _, retries := range []int{-2, -1, 0, 1, 3} {
			t.Run(fmt.Sprintf("redirects=%d/retries=%d", redirects, retries), func(t *testing.T) {
				var dials, slots, nodes atomic.Int64
				options := &redis.ClusterOptions{
					Addrs: []string{"127.0.0.1:1"}, MaxRedirects: redirects, MaxRetries: retries,
					Dialer: func(context.Context, string, string) (net.Conn, error) {
						dials.Add(1)
						return nil, errors.New("constructor must not dial")
					},
					ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
						slots.Add(1)
						return nil, errors.New("constructor must not resolve slots")
					},
					NewClient: func(opt *redis.Options) *redis.Client {
						nodes.Add(1)
						return redis.NewClient(opt)
					},
				}
				client := redis.NewClusterClient(options)
				defer client.Close()
				normalizedRedirects, normalizedRetries := client.Options().MaxRedirects, client.Options().MaxRetries
				wantRedirects, wantRetries := redirects, retries
				if redirects == -1 {
					wantRedirects = 0
				} else if redirects == 0 {
					wantRedirects = 3
				}
				if retries == 0 {
					wantRetries = -1
				}
				if normalizedRedirects != wantRedirects || normalizedRetries != wantRetries {
					t.Fatalf("unexpected go-redis normalization: redirects=%d retries=%d", normalizedRedirects, normalizedRetries)
				}
				l, err := newLimiter(client, "transport-test", "domain")
				if redirects >= -1 && retries >= -1 {
					if err != nil || l == nil {
						t.Fatalf("safe client rejected: %v", err)
					}
				} else if !errors.Is(err, ErrInvalid) || l != nil {
					t.Fatalf("unsafe client accepted: limiter=%v err=%v", l, err)
				}
				if dials.Load() != 0 || slots.Load() != 0 || nodes.Load() != 0 {
					t.Fatalf("constructor performed I/O: dials=%d slots=%d nodes=%d", dials.Load(), slots.Load(), nodes.Load())
				}
				if client.Options().MaxRedirects != normalizedRedirects || client.Options().MaxRetries != normalizedRetries {
					t.Fatal("constructor changed Cluster retry options")
				}
			})
		}
	}
}

func TestNewCustomTransport(t *testing.T) {
	if l, err := newLimiter(&stubScripter{}, "transport-test", "domain"); err != nil || l == nil {
		t.Fatalf("custom Scripter rejected: %v", err)
	}
	var nilCluster *redis.ClusterClient
	if l, err := newLimiter(nilCluster, "transport-test", "domain"); l != nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("typed-nil Cluster accepted: %v", err)
	}
}

// Directly supply a redirect reply at the node hook so this test needs no Redis.
// With redirects disabled, an application must handle the error rather than
// assume transparent MOVED/ASK recovery remains available.
type redirectHook struct {
	err   error
	calls atomic.Int64
}

func (h *redirectHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *redirectHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *redirectHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "command" {
			cmd.(*redis.CommandsInfoCmd).SetVal(map[string]*redis.CommandInfo{})
			return nil
		}
		h.calls.Add(1)
		return h.err
	}
}

func TestClusterRedirectsDisabled(t *testing.T) {
	for _, kind := range []string{"MOVED", "ASK"} {
		t.Run(kind, func(t *testing.T) {
			hook := &redirectHook{err: serverError(kind + " 1 127.0.0.1:2")}
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: []string{"127.0.0.1:1"}, MaxRetries: -1, MaxRedirects: -1,
				ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
					return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: "127.0.0.1:1"}}}}, nil
				},
				NewClient: func(opt *redis.Options) *redis.Client {
					c := redis.NewClient(opt)
					c.AddHook(hook)
					return c
				},
			})
			defer client.Close()
			l, err := newLimiter(client, "transport-test", kind)
			if err != nil {
				t.Fatal(err)
			}
			_, err = l.admitRules(context.Background(), rule("q", 1, 1))
			if !errors.Is(err, ErrBackend) || !errors.Is(err, hook.err) || OutcomeOf(err) != Unknown || hook.calls.Load() != 1 {
				t.Fatalf("redirect not surfaced once: calls=%d err=%v", hook.calls.Load(), err)
			}
		})
	}
}

func TestNewRejectsRing(t *testing.T) {
	for _, retries := range []int{-1, 0, 3} {
		ring := redis.NewRing(&redis.RingOptions{Addrs: map[string]string{}, MaxRetries: retries, HeartbeatFrequency: time.Hour})
		before := ring.Options().MaxRetries
		limiter, err := newLimiter(ring, "transport-test", "domain")
		if limiter != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("Ring accepted: %v %v", limiter, err)
		}
		if ring.Options().MaxRetries != before {
			t.Fatal("caller options modified")
		}
		ring.Close()
	}
	var ring *redis.Ring
	if l, err := newLimiter(ring, "test", "domain"); l != nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil Ring accepted: %v", err)
	}
}

func TestClusterRedirectBudget(t *testing.T) {
	hook := &redirectHook{err: serverError("MOVED 1 127.0.0.1:1")}
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"127.0.0.1:1"}, MaxRetries: -1, MaxRedirects: 2,
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: "127.0.0.1:1"}}}}, nil
		},
		NewClient: func(o *redis.Options) *redis.Client { c := redis.NewClient(o); c.AddHook(hook); return c },
	})
	defer client.Close()
	limiter, err := newLimiter(client, "budget", "domain")
	if err != nil {
		t.Fatal(err)
	}
	_, err = limiter.admitRules(context.Background(), rule("q", 1, 1))
	if !errors.Is(err, ErrBackend) || hook.calls.Load() != 3 {
		t.Fatalf("unbounded redirects: calls=%d err=%v", hook.calls.Load(), err)
	}
}

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
					limiter, err := newLimiter(client, "retry-budget", fmt.Sprint(time.Now().UnixNano()))
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := client.Del(cleanup, limiter.key("q"), limiter.policyKey("q")).Err(); err != nil {
							t.Error(err)
						}
					}()
					r := rule("q", 2, 1)
					if err := client.Set(ctx, limiter.policyKey("q"), "P1|3600000000:2:1", 0).Err(); err != nil {
						t.Fatal(err)
					}
					_, err = limiter.admitRules(ctx, r)
					if !errors.Is(err, io.EOF) || !errors.Is(err, ErrBackend) || OutcomeOf(err) != Unknown || loss.dropped.Load() != 1 {
						t.Fatalf("lost reply: dropped=%d outcome=%v err=%v", loss.dropped.Load(), OutcomeOf(err), err)
					}
					// Exactly one debit must have occurred: one further admission, then denial.
					for _, want := range []bool{true, false} {
						d, err := limiter.admitRules(ctx, r)
						if err != nil || d.Allowed != want {
							t.Fatalf("remaining quota: want allowed=%t got %+v err=%v", want, d, err)
						}
					}
				})
			}
		}
	}
}

func TestRedisManagedLostReplyNoRetry(t *testing.T) {
	l, admin := integration(t)
	ctx := context.Background()
	r := rule("q", 2, 1)
	r.Policy.Revision = 1
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	loss := &replyLoss{}
	c := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, MaxRetries: 3, PoolSize: 1, Dialer: loss.dial})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := admissionScript.Load(ctx, c).Err(); err != nil {
		t.Fatal(err)
	}
	c.AddHook(&armReplyLoss{loss: loss, command: "evalsha"})
	broken := &limiter{client: safeScripter(c), prefix: l.prefix}
	req := managedRequest("q", 1)
	if _, err := broken.Admit(ctx, req); !errors.Is(err, io.EOF) || OutcomeOf(err) != Unknown {
		t.Fatalf("lost reply: %v", err)
	}
	for _, allowed := range []bool{true, false} {
		d, err := l.Admit(ctx, req)
		if err != nil || d.Allowed != allowed {
			t.Fatalf("replayed debit: %+v %v", d, err)
		}
	}
}

func TestRedisUpdateLostReplyNoRetry(t *testing.T) {
	addr := os.Getenv("PACELOCK_REDIS_ADDR")
	if addr == "" {
		t.Skip("set PACELOCK_REDIS_ADDR")
	}
	for _, command := range []string{"evalsha", "eval"} {
		t.Run(command, func(t *testing.T) {
			ctx := context.Background()
			loss := &replyLoss{}
			c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: 3, PoolSize: 1, Dialer: loss.dial})
			defer c.Close()
			c.AddHook(&armReplyLoss{loss: loss, command: command})
			if err := c.Ping(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			if command == "evalsha" {
				if err := updateScript.Load(ctx, c).Err(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := c.ScriptFlush(ctx).Err(); err != nil {
					t.Fatal(err)
				}
			}
			l, err := newLimiter(c, "update-lost", fmt.Sprint(time.Now().UnixNano()))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Del(ctx, l.policyKey("q"))
			r := rule("q", 1, 1)
			r.Policy.Revision = 1
			err = l.UpdateQuotas(ctx, quotaOf(r))
			if !errors.Is(err, io.EOF) || OutcomeOf(err) != Unknown || loss.dropped.Load() != 1 {
				t.Fatalf("lost reply: %v drops=%d", err, loss.dropped.Load())
			}
			if got := c.Get(ctx, l.policyKey("q")).Val(); got != "P1|3600000000:1:1" {
				t.Fatalf("commit absent: %s", got)
			}
			if err = l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
				t.Fatalf("explicit retry: %v", err)
			}
		})
	}
}

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
			if reply, ok := cmd.(interface {
				SetVal(interface{})
				Int64Slice() ([]int64, error)
			}); ok {
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
	for _, retries := range []int{-1, 0, 2} {
		for _, burst := range []int64{1, 2} {
			t.Run(fmt.Sprintf("retries=%d/burst=%d", retries, burst), func(t *testing.T) {
				hook := &clusterLostReplyHook{}
				client := redis.NewClusterClient(&redis.ClusterOptions{
					Addrs: strings.Split(addresses, ","), MaxRetries: retries, MaxRedirects: 3,
					ContextTimeoutEnabled: true,
					NewClient: func(opt *redis.Options) *redis.Client {
						c := redis.NewClient(opt)
						c.AddHook(hook)
						return c
					},
				})
				defer client.Close()
				l, err := newLimiter(client, "pacelock-transport-test", fmt.Sprintf("%d-%d", time.Now().UnixNano(), burst))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := client.Del(ctx, l.key("q"), l.policyKey("q")).Err(); err != nil {
						t.Error(err)
					}
				}()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				r := rule("q", burst, 1)
				if err := client.Set(ctx, l.policyKey("q"), fmt.Sprintf("P1|3600000000:%d:1", burst), 0).Err(); err != nil {
					t.Fatal(err)
				}
				d, err := l.admitRules(ctx, r)
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
				d, err = l.admitRules(ctx, r)
				if err != nil || d.Allowed != (burst == 2) {
					t.Fatalf("wrong remaining capacity: decision=%+v err=%v", d, err)
				}
				t.Logf("new attempt: burst=%d allowed=%t", burst, d.Allowed)
				if burst == 2 {
					if d, err := l.admitRules(ctx, r); err != nil || d.Allowed {
						t.Fatalf("quota did not exhaust after two admissions: decision=%+v err=%v", d, err)
					}
				}
			})
		}
	}
}
