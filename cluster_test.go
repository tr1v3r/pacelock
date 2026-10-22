package pacelock

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisCluster(t *testing.T) {
	addresses := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS")
	if addresses == "" {
		t.Skip("set PACELOCK_REDIS_CLUSTER_ADDRS for real Cluster integration")
	}
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ","), MaxRetries: -1, ContextTimeoutEnabled: true})
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
		l, err := New(c, "pacelock-test", fmt.Sprintf("%s-%d", prefix, i))
		if err != nil {
			t.Fatal(err)
		}
		r := []Rule{rule("rpm", 2, 1), rule("tokens", 4, 2)}
		quotaSlot, err := c.ClusterKeySlot(ctx, l.key("rpm")).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{l.key("rpm"), l.key("tokens"), l.attemptKey("id")} {
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
		d, err := l.AllowOnce(ctx, "id", time.Minute, r...)
		if err != nil || !d.Allowed || d.Replayed {
			t.Fatalf("%+v %v", d, err)
		}
		replay, err := l.AllowOnce(ctx, "id", time.Minute, r...)
		if err != nil || !replay.Replayed {
			t.Fatalf("%+v %v", replay, err)
		}
		if found, ok, err := l.Lookup(ctx, "id"); err != nil || !ok || !found.Allowed {
			t.Fatalf("%+v %v %v", found, ok, err)
		}
		if d, err := l.Allow(ctx, r...); err != nil || !d.Allowed {
			t.Fatalf("%+v %v", d, err)
		}
		if d, err := l.Allow(ctx, r...); err != nil || d.Allowed {
			t.Fatalf("%+v %v", d, err)
		}
	}
	if len(seen) < 2 {
		t.Fatalf("test did not exercise multiple masters: %v", seen)
	}
	// Different clients competing on one slot must still debit one admission.
	l, _ := New(c, "pacelock-test", prefix+"-concurrent")
	other := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ","), MaxRetries: -1})
	defer other.Close()
	l2 := &Limiter{client: other, prefix: l.prefix}
	keys = append(keys, l.key("q"), l.attemptKey("same"))
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
			d, err := current.AllowOnce(ctx, "same", time.Minute, rule("q", 1, 1))
			if err != nil || !d.Allowed {
				t.Errorf("%+v %v", d, err)
			} else if !d.Replayed {
				first.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if first.Load() != 1 {
		t.Fatalf("original admissions=%d", first.Load())
	}
}
