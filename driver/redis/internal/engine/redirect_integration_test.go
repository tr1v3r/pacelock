package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type askLossHook struct {
	loss      *replyLoss
	lose      atomic.Bool
	successes atomic.Int64
}

func (*askLossHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (*askLossHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h *askLossHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.loss != nil && h.lose.CompareAndSwap(true, false) {
			h.loss.armed.Store(true)
		}
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			if (cmd.Name() == "eval" || cmd.Name() == "evalsha") && cmd.Err() == nil {
				if !cmd.NoRetry() {
					return errors.New("ASK script lost NoRetry marker")
				}
				h.successes.Add(1)
				if h.lose.Load() {
					cmd.SetErr(io.EOF)
					return io.EOF
				}
			}
		}
		return err
	}
}

// TestClusterStaticResharding fails closed when static debt exists but its
// explicit policy key is absent on a migrating slot. The server-side error count
// detects retries even inside go-redis's node transport loop.
func TestClusterStaticResharding(t *testing.T) {
	addresses := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS")
	if addresses == "" {
		t.Skip("Cluster fixture required")
	}
	for _, retries := range []int{-1, 0, 2} {
		t.Run(fmt.Sprintf("retries=%d", retries), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			admin := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ",")})
			defer admin.Close()
			l, err := New(admin, "static-resharding", fmt.Sprint(time.Now().UnixNano()))
			if err != nil {
				t.Fatal(err)
			}
			slot, err := admin.ClusterKeySlot(ctx, l.key("q")).Result()
			if err != nil {
				t.Fatal(err)
			}
			slots, err := admin.ClusterSlots(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			var sourceAddr, targetAddr string
			for _, s := range slots {
				if int(slot) >= s.Start && int(slot) <= s.End {
					sourceAddr = s.Nodes[0].Addr
				}
			}
			for _, s := range slots {
				if s.Nodes[0].Addr != sourceAddr {
					targetAddr = s.Nodes[0].Addr
					break
				}
			}
			if sourceAddr == "" || targetAddr == "" {
				t.Fatal("need multiple masters")
			}
			source := redis.NewClient(&redis.Options{Addr: sourceAddr, MaxRetries: -1})
			defer source.Close()
			target := redis.NewClient(&redis.Options{Addr: targetAddr, MaxRetries: -1})
			defer target.Close()
			sourceID, err := source.ClusterMyID(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			targetID, err := target.ClusterMyID(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			// Keep debt on its original owner. No keys are migrated or created on target.
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				if err := source.Do(cleanup, "cluster", "setslot", slot, "stable").Err(); err != nil {
					t.Error(err)
				}
				if err := target.Do(cleanup, "cluster", "setslot", slot, "stable").Err(); err != nil {
					t.Error(err)
				}
				if err := source.Del(cleanup, l.key("q"), l.policyKey("q")).Err(); err != nil {
					t.Error(err)
				}
			}()
			r := rule("q", 2, 1)
			if d, e := l.Allow(ctx, r); e != nil || !d.Allowed {
				t.Fatalf("seed: %+v %v", d, e)
			}
			before, err := source.Get(ctx, l.key("q")).Result()
			if err != nil {
				t.Fatal(err)
			}
			if n, e := source.Exists(ctx, l.policyKey("q")).Result(); e != nil || n != 0 {
				t.Fatalf("policy must be absent: %d %v", n, e)
			}
			client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ","), MaxRetries: retries, MaxRedirects: 3, ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) { return slots, nil }})
			defer client.Close()
			l.client = safeScripter(client)
			if err := client.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error { return admissionScript.Load(ctx, node).Err() }); err != nil {
				t.Fatal(err)
			}
			if err := target.Do(ctx, "cluster", "setslot", slot, "importing", sourceID).Err(); err != nil {
				t.Fatal(err)
			}
			if err := source.Do(ctx, "cluster", "setslot", slot, "migrating", targetID).Err(); err != nil {
				t.Fatal(err)
			}
			countErrors := func() int64 {
				t.Helper()
				info, e := source.Info(ctx, "errorstats").Result()
				if e != nil {
					t.Fatal(e)
				}
				for _, line := range strings.Split(info, "\n") {
					if raw, ok := strings.CutPrefix(strings.TrimSpace(line), "errorstat_TRYAGAIN:count="); ok {
						n, e := strconv.ParseInt(raw, 10, 64)
						if e != nil {
							t.Fatal(e)
						}
						return n
					}
				}
				return 0
			}
			attemptsBefore := countErrors()
			d, err := l.Allow(ctx, r)
			if !errors.Is(err, ErrBackend) || OutcomeOf(err) != Unknown || !strings.Contains(err.Error(), "TRYAGAIN") || d.Allowed {
				t.Fatalf("resharding: %+v %v", d, err)
			}
			if attempts := countErrors() - attemptsBefore; attempts != 1 {
				t.Fatalf("hidden command replay: TRYAGAIN responses=%d, want 1", attempts)
			}
			if got, e := source.Get(ctx, l.key("q")).Result(); e != nil || got != before {
				t.Fatalf("rejected admission changed debt: %q %v", got, e)
			}
			if err := source.Do(ctx, "cluster", "setslot", slot, "stable").Err(); err != nil {
				t.Fatal(err)
			}
			if err := target.Do(ctx, "cluster", "setslot", slot, "stable").Err(); err != nil {
				t.Fatal(err)
			}
			if n, e := source.Exists(ctx, l.policyKey("q")).Result(); e != nil || n != 0 {
				t.Fatalf("rejected admission wrote policy: %d %v", n, e)
			}
			// Exactly one token remains after the seeded debit: TRYAGAIN consumed none.
			for _, want := range []bool{true, false} {
				d, e := l.Allow(ctx, r)
				if e != nil || d.Allowed != want {
					t.Fatalf("remaining quota want %t: %+v %v", want, d, e)
				}
			}
		})
	}
}

// Requires disposable masters: temporarily migrates one empty slot, then restores
// its owner. Actual ASK/ASKING, MOVED and cold-cache paths exercise node routing.
func TestClusterLiveRedirects(t *testing.T) {
	for _, retries := range []int{-1, 0, 2} {
		for _, wireLoss := range []bool{false, true} {
			t.Run(fmt.Sprintf("retries=%d/wire=%t", retries, wireLoss), func(t *testing.T) { testClusterLiveRedirects(t, retries, wireLoss) })
		}
	}
}

func testClusterLiveRedirects(t *testing.T, retries int, wireLoss bool) {
	addresses := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS")
	if addresses == "" {
		t.Skip("Cluster fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ",")})
	t.Cleanup(func() { admin.Close() })
	l, err := New(redis.NewClient(&redis.Options{Addr: "unused", MaxRetries: -1}), "redirect-test", time.Now().String())
	if err != nil {
		t.Fatal(err)
	}
	// The temporary client above is used only to derive keys.
	l.client.(noRetryScripter).Scripter.(*redis.Client).Close()
	slot, err := admin.ClusterKeySlot(ctx, l.key("q")).Result()
	if err != nil {
		t.Fatal(err)
	}
	slots, err := admin.ClusterSlots(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	var sourceAddr, targetAddr string
	for _, s := range slots {
		if int(slot) >= s.Start && int(slot) <= s.End {
			sourceAddr = s.Nodes[0].Addr
		}
	}
	for _, s := range slots {
		if s.Nodes[0].Addr != sourceAddr {
			targetAddr = s.Nodes[0].Addr
			break
		}
	}
	if sourceAddr == "" || targetAddr == "" {
		t.Fatal("need multiple masters")
	}
	source := redis.NewClient(&redis.Options{Addr: sourceAddr, MaxRetries: -1})
	t.Cleanup(func() { source.Close() })
	target := redis.NewClient(&redis.Options{Addr: targetAddr, MaxRetries: -1})
	t.Cleanup(func() { target.Close() })
	sourceID, err := source.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := target.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Target owns every test key; delete before returning the empty slot.
		target.Do(cleanup, "cluster", "setslot", slot, "node", targetID)
		keys, _ := target.ClusterGetKeysInSlot(cleanup, int(slot), 100).Result()
		if len(keys) > 0 {
			target.Del(cleanup, keys...)
		}
		if err := admin.ForEachMaster(cleanup, func(ctx context.Context, c *redis.Client) error {
			return c.Do(ctx, "cluster", "setslot", slot, "node", sourceID).Err()
		}); err != nil {
			t.Error(err)
		}
	})
	hook := &askLossHook{}
	loss := &replyLoss{}
	if wireLoss {
		hook.loss = loss
	}
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ","), MaxRetries: retries, MaxRedirects: 3, PoolSize: 1, Dialer: loss.dial,
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) { return slots, nil },
		NewClient:    func(o *redis.Options) *redis.Client { node := redis.NewClient(o); node.AddHook(hook); return node },
	})
	defer c.Close()
	l.client = safeScripter(c)
	// Warm target connections before arming a fault below the pipeline retry loop.
	if err := c.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error { return node.Command(ctx).Err() }); err != nil {
		t.Fatal(err)
	}
	// Both explicit keys must exist on the importing node for multi-key ASK.
	// An absent static policy key may instead return TRYAGAIN during resharding.
	if err := source.MSet(ctx, l.key("q"), "D3|0", l.policyKey("q"), "P3|3600000000:2:1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := target.Do(ctx, "cluster", "setslot", slot, "importing", sourceID).Err(); err != nil {
		t.Fatal(err)
	}
	if err := source.Do(ctx, "cluster", "setslot", slot, "migrating", targetID).Err(); err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(targetAddr)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{l.key("q"), l.policyKey("q")} {
		if err := source.Do(ctx, "MIGRATE", host, port, key, 0, 5000).Err(); err != nil {
			t.Fatal(err)
		}
	}
	r := managedRequest("q", 1)
	// Force NOSCRIPT on all masters to combine fallback with ASK.
	if err := admin.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error { return c.ScriptFlush(ctx).Err() }); err != nil {
		t.Fatal(err)
	}
	d, err := l.AdmitManaged(ctx, r)
	if err != nil || !d.Allowed {
		t.Fatalf("ASK: %+v %v", d, err)
	}
	if hook.successes.Load() != 1 {
		t.Fatalf("ASK successful executions=%d", hook.successes.Load())
	}
	hook.lose.Store(true)
	_, err = l.AdmitManaged(ctx, r)
	if !errors.Is(err, io.EOF) || OutcomeOf(err) != Unknown {
		t.Fatalf("ASK lost reply: %v", err)
	}
	if (!wireLoss && hook.successes.Load() != 2) || (wireLoss && loss.dropped.Load() != 1) {
		t.Fatal("lost ASK response was replayed")
	}
	hook.lose.Store(false)
	if err := admin.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error {
		return c.Do(ctx, "cluster", "setslot", slot, "node", targetID).Err()
	}); err != nil {
		t.Fatal(err)
	}
	// Static old slot map forces MOVED. Denial proves exactly two prior debits.
	d, err = l.AdmitManaged(ctx, r)
	if err != nil || d.Allowed {
		t.Fatalf("MOVED accounting: %+v %v", d, err)
	}
}
