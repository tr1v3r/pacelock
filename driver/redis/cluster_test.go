package redisdriver

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRedisCluster(t *testing.T) {
	addresses := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS")
	if addresses == "" {
		t.Skip("set PACELOCK_REDIS_CLUSTER_ADDRS for real Cluster integration")
	}
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ","), MaxRetries: -1, MaxRedirects: -1, ContextTimeoutEnabled: true})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	slots, err := c.ClusterSlots(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) < 3 {
		t.Fatalf("want >=3 slot ranges, got %d", len(slots))
	}
	prefix := fmt.Sprintf("cluster-test-%d", time.Now().UnixNano())
	var keys []string
	defer func() {
		for _, k := range keys {
			c.Del(context.Background(), k)
		}
	}()
	seen := make(map[string]bool)
	for i := 0; i < 256 && len(seen) < 3; i++ {
		l, err := newLimiter(c, "pacelock-test", fmt.Sprintf("%s-%d", prefix, i))
		if err != nil {
			t.Fatal(err)
		}
		r := []Rule{rule("rpm", 2, 1), rule("tokens", 4, 2)}
		publishRules(t, l, r...)
		quotaSlot, err := c.ClusterKeySlot(ctx, l.key("rpm")).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{l.key("rpm"), l.key("tokens"), l.policyKey("rpm"), l.policyKey("tokens")} {
			keys = append(keys, k)
			slot, err := c.ClusterKeySlot(ctx, k).Result()
			if err != nil || slot != quotaSlot {
				t.Fatalf("cross slot %d %d %v", quotaSlot, slot, err)
			}
		}
		for _, s := range slots {
			if quotaSlot >= int64(s.Start) && quotaSlot <= int64(s.End) {
				seen[s.Nodes[0].Addr] = true
			}
		}
		d, err := l.admitRules(ctx, r...)
		if err != nil || !d.Allowed {
			t.Fatalf("%+v %v", d, err)
		}
		if d, err := l.admitRules(ctx, r...); err != nil || !d.Allowed {
			t.Fatalf("%+v %v", d, err)
		}
		if d, err := l.admitRules(ctx, r...); err != nil || d.Allowed {
			t.Fatalf("%+v %v", d, err)
		}
	}
	if len(seen) < 2 {
		t.Fatalf("test did not exercise multiple masters: %v", seen)
	}
	// Different clients competing on one slot must still debit one admission.
	l, _ := newLimiter(c, "pacelock-test", prefix+"-concurrent")
	other := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ","), MaxRetries: -1, MaxRedirects: -1})
	defer other.Close()
	l2 := &limiter{client: other, prefix: l.prefix}
	keys = append(keys, l.key("q"), l.policyKey("q"))
	publishRules(t, l, rule("q", 1, 1))
	var first atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			current := l
			if i%2 == 0 {
				current = l2
			}
			d, err := current.admitRules(ctx, rule("q", 1, 1))
			if err != nil {
				t.Errorf("%+v %v", d, err)
			} else if d.Allowed {
				first.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if first.Load() != 1 {
		t.Fatalf("original admissions=%d", first.Load())
	}
}

// TestRedisClusterMinimalACL keeps the documented standalone command grants
// separate from Cluster topology discovery. It uses a fresh, namespace-scoped
// identity on every master and never modifies existing users or flushes data.
func TestRedisClusterMinimalACL(t *testing.T) {
	addresses := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS")
	if addresses == "" {
		t.Skip("set PACELOCK_REDIS_CLUSTER_ADDRS for real Cluster ACL integration")
	}
	addrs := strings.Split(addresses, ",")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addrs, MaxRetries: -1, MaxRedirects: -1, ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { admin.Close() })
	slots, err := admin.ClusterSlots(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	username := fmt.Sprintf("pacelock-acl-%d-%d", os.Getpid(), time.Now().UnixNano())
	const password = "disposable-cluster-acl-test-only"
	owner, err := newLimiter(admin, "pacelock-cluster-acl", username)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{owner.key("quota"), owner.policyKey("quota")}
	var masters []*redis.Client
	t.Cleanup(func() {
		clean, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := admin.Del(clean, keys...).Err(); err != nil {
			t.Errorf("clean test keys: %v", err)
		}
		for _, c := range masters {
			if err := c.Do(clean, "ACL", "DELUSER", username).Err(); err != nil {
				t.Errorf("clean test identity: %v", err)
			}
			c.Close()
		}
	})
	seen := make(map[string]bool)
	for _, slot := range slots {
		if len(slot.Nodes) == 0 {
			t.Fatal("slot range has no master")
		}
		addr := slot.Nodes[0].Addr
		if seen[addr] {
			continue
		}
		seen[addr] = true
		c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true})
		masters = append(masters, c)
		// SECURITY.md: admission commands plus this client's handshake grants.
		// No +cluster, +command, administrative command or broad key grant.
		if err := c.Do(ctx, "ACL", "SETUSER", username, "reset", "on", ">"+password,
			"~"+owner.prefix+"*", "+evalsha", "+eval", "+get", "+mset", "+time",
			"+hello", "+auth", "+client", "+ping", "+select").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if len(masters) == 0 {
		t.Fatal("Cluster has no masters")
	}
	connect := func(t *testing.T) *limiter {
		t.Helper()
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: addrs, Username: username, Password: password,
			MaxRetries: -1, MaxRedirects: -1, ContextTimeoutEnabled: true,
		})
		t.Cleanup(func() { client.Close() })
		l, err := newLimiter(client, "pacelock-cluster-acl", username)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	r := Rule{Name: "quota", Policy: Policy{Rate: 1, Period: time.Hour, Burst: 3}, Cost: 1}
	publishRules(t, owner, r)
	t.Run("standalone_grants_cannot_discover_cluster", func(t *testing.T) {
		d, err := connect(t).admitRules(ctx, r)
		if err == nil || !errors.Is(err, ErrBackend) || !strings.Contains(strings.ToLower(err.Error()), "cluster|slots") || d.Allowed {
			t.Fatalf("want denied CLUSTER SLOTS, got decision=%+v err=%v", d, err)
		}
		if n, err := admin.Exists(ctx, owner.key("quota")).Result(); err != nil || n != 0 {
			t.Fatalf("discovery failure wrote state: keys=%d err=%v", n, err)
		}
	})
	for _, c := range masters {
		if err := c.Do(ctx, "ACL", "SETUSER", username, "+cluster|slots").Err(); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("only_cluster_slots_restores_public_operations", func(t *testing.T) {
		l := connect(t)
		if d, err := l.admitRules(ctx, r); err != nil || !d.Allowed {
			t.Fatalf("Allow: %+v %v", d, err)
		}
		if d, err := l.admitRules(ctx, r); err != nil || !d.Allowed {
			t.Fatalf("second admission: %+v %v", d, err)
		}
		if d, err := l.admitRules(ctx, r); err != nil || !d.Allowed {
			t.Fatalf("third admission failed: %+v %v", d, err)
		}
		if d, err := l.admitRules(ctx, r); err != nil || d.Allowed {
			t.Fatalf("quota not exhausted: %+v %v", d, err)
		}
	})
	// Maintenance has separate grants; admission does not require SCAN or DEL.
	for _, c := range masters {
		if err := c.Do(ctx, "ACL", "SETUSER", username, "+scan", "+del").Err(); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("maintenance_grants_route_to_scope_primary", func(t *testing.T) {
		l := connect(t)
		cursor := PruneCursor{}
		for pages := 0; !cursor.Done(); pages++ {
			if pages > 10000 {
				t.Fatal("scan did not complete")
			}
			result, err := l.Prune(ctx, cursor, 1)
			if err != nil {
				t.Fatal(err)
			}
			if result.Examined > 1 || result.Deleted != 0 {
				t.Fatalf("maintenance changed live state: %+v", result)
			}
			cursor = result.Cursor
		}
	})

}

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

// TestClusterMissingPolicyResharding fails closed when debt exists but its
// explicit policy key is absent on a migrating slot. The server-side error count
// detects retries even inside go-redis's node transport loop.
func TestClusterMissingPolicyResharding(t *testing.T) {
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
			l, err := newLimiter(admin, "missing-policy-resharding", fmt.Sprint(time.Now().UnixNano()))
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
			publishRules(t, l, r)
			if d, e := l.admitRules(ctx, r); e != nil || !d.Allowed {
				t.Fatalf("seed: %+v %v", d, e)
			}
			if err := source.Del(ctx, l.policyKey("q")).Err(); err != nil {
				t.Fatal(err)
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
			d, err := l.admitRules(ctx, r)
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
			publishRules(t, l, r)
			// Exactly one token remains after the seeded debit: TRYAGAIN consumed none.
			for _, want := range []bool{true, false} {
				d, e := l.admitRules(ctx, r)
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
	l, err := newLimiter(redis.NewClient(&redis.Options{Addr: "unused", MaxRetries: -1}), "redirect-test", time.Now().String())
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
	// An absent policy key may instead return TRYAGAIN during resharding.
	if err := source.MSet(ctx, l.key("q"), "D1|0", l.policyKey("q"), "P1|3600000000:2:1").Err(); err != nil {
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
	d, err := l.Admit(ctx, r)
	if err != nil || !d.Allowed {
		t.Fatalf("ASK: %+v %v", d, err)
	}
	if hook.successes.Load() != 1 {
		t.Fatalf("ASK successful executions=%d", hook.successes.Load())
	}
	hook.lose.Store(true)
	_, err = l.Admit(ctx, r)
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
	d, err = l.Admit(ctx, r)
	if err != nil || d.Allowed {
		t.Fatalf("MOVED accounting: %+v %v", d, err)
	}
}
