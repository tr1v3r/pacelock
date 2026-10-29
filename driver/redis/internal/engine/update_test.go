package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

func quotaOf(r Rule) pacelock.Quota { return pacelock.Quota{Name: r.Name, Policy: r.Policy} }
func managedRequest(name string, cost int64) pacelock.Admission {
	return pacelock.Admission{Scope: pacelock.Scope{Namespace: "test", Domain: "test"}, Charges: []pacelock.Charge{{Name: name, Cost: cost}}}
}

func TestRedisManagedPolicies(t *testing.T) {
	l, c := integration(t)
	exerciseManaged(t, l, c)
}
func TestRedisClusterManagedPolicies(t *testing.T) {
	addresses := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS")
	if addresses == "" {
		t.Skip("set PACELOCK_REDIS_CLUSTER_ADDRS")
	}
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ","), MaxRetries: 3})
	defer c.Close()
	l, err := New(c, "managed-cluster", fmt.Sprint(time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.ForEachMaster(context.Background(), func(ctx context.Context, node *redis.Client) error {
		it := node.Scan(ctx, 0, l.prefix+"*", 100).Iterator()
		for it.Next(ctx) {
			node.Del(ctx, it.Val())
		}
		return it.Err()
	})
	exerciseManaged(t, l, c)
}
func exerciseManaged(t *testing.T, l *Limiter, c redis.UniversalClient) {
	t.Helper()
	ctx := context.Background()
	r := rule("quota", 4, 4)
	r.Policy.Revision = 1
	req := managedRequest(r.Name, 4)
	if _, err := l.Admit(ctx, req); !errors.Is(err, pacelock.ErrPolicyMissing) || OutcomeOf(err) != NotCharged {
		t.Fatalf("missing: %v", err)
	}
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	if c.Exists(ctx, l.key(r.Name)).Val() != 0 {
		t.Fatal("update created debt")
	}
	if d, err := l.Admit(ctx, req); err != nil || !d.Allowed {
		t.Fatalf("initial: %+v %v", d, err)
	}
	debt := c.Get(ctx, l.key(r.Name)).Val()
	if !strings.HasPrefix(debt, "D3|") {
		t.Fatal(debt)
	}
	r.Policy.Revision = 2
	r.Policy.Burst = 1
	r.Policy.Period = time.Minute
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	if c.Get(ctx, l.key(r.Name)).Val() != debt {
		t.Fatal("update changed debt")
	}
	if _, err := l.Admit(ctx, req); !errors.Is(err, ErrInvalid) {
		t.Fatalf("over-burst: %v", err)
	}
	req.Charges[0].Cost = 1
	if d, err := l.Admit(ctx, req); err != nil || d.Allowed || d.RetryAfter < 3*time.Hour {
		t.Fatalf("shrink: %+v %v", d, err)
	}
	equivalent := r
	equivalent.Policy.Rate = 2
	equivalent.Policy.Period = 2 * time.Minute
	// A same-revision update must not rewrite even TTL (test sentinel only).
	if err := c.PExpire(ctx, l.policyKey(r.Name), time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if err := l.UpdateQuotas(ctx, quotaOf(equivalent)); err != nil {
		t.Fatal(err)
	}
	if c.PTTL(ctx, l.policyKey(r.Name)).Val() <= 0 {
		t.Fatal("same revision rewrote policy")
	}
	c.Persist(ctx, l.policyKey(r.Name))
	fresh := rule("fresh", 1, 1)
	fresh.Policy.Revision = 1
	bad := r
	bad.Policy.Burst = 2
	if err := l.UpdateQuotas(ctx, quotaOf(fresh), quotaOf(bad)); !errors.Is(err, ErrPolicyMismatch) {
		t.Fatal(err)
	}
	if c.Exists(ctx, l.policyKey(fresh.Name)).Val() != 0 {
		t.Fatal("partial update")
	}
	r.Policy.Revision = 3
	r.Policy.Rate = 0
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	req.Charges[0].Cost = 4
	if _, err := l.Admit(ctx, req); !errors.Is(err, pacelock.ErrPaused) || OutcomeOf(err) != NotCharged {
		t.Fatalf("paused: %v", err)
	}
	if c.Get(ctx, l.key(r.Name)).Val() != debt {
		t.Fatal("pause mutated state")
	}
	// Expired debt is reclaimable, policy/paused state remains.
	c.Set(ctx, l.key(r.Name), "D3|0", 0)
	var cursor PruneCursor
	for {
		page, err := l.Prune(ctx, cursor, 64)
		if err != nil {
			t.Fatal(err)
		}
		cursor = page.Cursor
		if cursor.Done() {
			break
		}
	}
	if c.Exists(ctx, l.key(r.Name)).Val() != 0 || c.Get(ctx, l.policyKey(r.Name)).Val() != "P3|0:1:3" {
		t.Fatal("prune failed separation")
	}
	if err := l.UpdateQuotas(ctx, quotaOf(equivalent)); !errors.Is(err, ErrPolicyMismatch) {
		t.Fatalf("stale after prune: %v", err)
	}
	r.Policy.Revision = 4
	r.Policy.Rate = 1
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	req.Charges[0].Cost = 1
	if d, err := l.Admit(ctx, req); err != nil || !d.Allowed {
		t.Fatalf("resume: %+v %v", d, err)
	}
	r.Policy.Revision = 5
	r.Policy.Rate = 0
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Admit(ctx, req); !errors.Is(err, pacelock.ErrPaused) || OutcomeOf(err) != NotCharged {
		t.Fatalf("latest policy: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := l.UpdateQuotas(canceled, quotaOf(fresh)); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
}

func TestRedisManagedLegacyTupleDebt(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("legacy", 2, 1)
	now, err := c.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	debt := fmt.Sprintf("%d:3600000000:2", now.UnixMicro()+int64(time.Hour/time.Microsecond))
	if err := c.Set(ctx, l.key(r.Name), debt, 0).Err(); err != nil {
		t.Fatal(err)
	}
	r.Policy.Revision = 1
	r.Policy.Rate = 0
	if e := l.UpdateQuotas(ctx, quotaOf(r)); e != nil {
		t.Fatal(e)
	}
	if c.Get(ctx, l.key(r.Name)).Val() != debt {
		t.Fatal("legacy debt touched")
	}
	req := managedRequest(r.Name, 1)
	if _, e := l.Admit(ctx, req); !errors.Is(e, pacelock.ErrPaused) {
		t.Fatal(e)
	}
	r.Policy.Revision = 2
	r.Policy.Rate = 1
	r.Policy.Burst = 1
	if e := l.UpdateQuotas(ctx, quotaOf(r)); e != nil {
		t.Fatal(e)
	}
	if d, e := l.Admit(ctx, req); e != nil || d.Allowed {
		t.Fatalf("legacy debt reset: %+v %v", d, e)
	}
}

func TestRedisManagedMalformed(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("bad", 1, 1)
	r.Policy.Revision = 1
	for _, raw := range []string{"P3|0:1:0", "P3|0:1:9007199254740992", "P3|0:86400000001:1", "P3|1:0:1", "P3|0:1:1:extra", "D3|0"} {
		c.Set(ctx, l.policyKey(r.Name), raw, 0)
		if err := l.UpdateQuotas(ctx, quotaOf(r)); !errors.Is(err, ErrState) {
			t.Fatalf("%s update: %v", raw, err)
		}
		if _, err := l.Admit(ctx, managedRequest(r.Name, 1)); !errors.Is(err, ErrState) {
			t.Fatalf("%s admit: %v", raw, err)
		}
		// SCAN may return an empty first page even with matching keys present.
		cursor := PruneCursor{}
		for {
			result, err := l.Prune(ctx, cursor, MaxPruneKeys)
			if errors.Is(err, ErrState) {
				break
			}
			if err != nil || result.Cursor.Done() {
				t.Fatalf("%s prune: %v", raw, err)
			}
			cursor = result.Cursor
		}
	}
	c.Del(ctx, l.policyKey(r.Name))
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"D3|-1", "D3|9007199254740991", "D3|bad", "Q2|1:1:1", "P3|1:1:1", "E2|1|C1|removed"} {
		c.Set(ctx, l.key(r.Name), raw, 0)
		if _, err := l.Admit(ctx, managedRequest(r.Name, 1)); !errors.Is(err, ErrState) {
			t.Fatalf("%s debt: %v", raw, err)
		}
	}
	// Configuration writes must not read malformed debt.
	r.Policy.Revision = 2
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
}
func TestRedisManagedConcurrentUpdates(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for rev := int64(1); rev <= 50; rev++ {
		wg.Add(1)
		go func(rev int64) {
			defer wg.Done()
			r := rule("concurrent", 1, 1)
			r.Policy.Revision = rev
			if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil && !errors.Is(err, ErrPolicyMismatch) {
				t.Error(err)
			}
		}(rev)
	}
	wg.Wait()
	if got := c.Get(ctx, l.policyKey("concurrent")).Val(); got != "P3|3600000000:1:50" {
		t.Fatal(got)
	}
}
