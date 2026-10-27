package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

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
	owner, err := New(admin, "pacelock-cluster-acl", username)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{owner.key("quota"), owner.attemptKey("admission")}
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
			"~"+owner.prefix+"*", "+evalsha", "+eval", "+get", "+set", "+time",
			"+hello", "+auth", "+client", "+ping", "+select").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if len(masters) == 0 {
		t.Fatal("Cluster has no masters")
	}
	connect := func(t *testing.T) *Limiter {
		t.Helper()
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: addrs, Username: username, Password: password,
			MaxRetries: -1, MaxRedirects: -1, ContextTimeoutEnabled: true,
		})
		t.Cleanup(func() { client.Close() })
		l, err := New(client, "pacelock-cluster-acl", username)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	r := Rule{Name: "quota", Rate: 1, Period: time.Hour, Burst: 3, Cost: 1}
	t.Run("standalone_grants_cannot_discover_cluster", func(t *testing.T) {
		d, err := connect(t).Allow(ctx, r)
		if err == nil || !errors.Is(err, ErrBackend) || !strings.Contains(strings.ToLower(err.Error()), "cluster|slots") || d.Allowed {
			t.Fatalf("want denied CLUSTER SLOTS, got decision=%+v err=%v", d, err)
		}
		if n, err := admin.Exists(ctx, keys...).Result(); err != nil || n != 0 {
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
		if d, err := l.Allow(ctx, r); err != nil || !d.Allowed {
			t.Fatalf("Allow: %+v %v", d, err)
		}
		if d, err := l.AllowOnce(ctx, "admission", time.Minute, r); err != nil || !d.Allowed || d.Replayed {
			t.Fatalf("AllowOnce: %+v %v", d, err)
		}
		if d, err := l.AllowOnce(ctx, "admission", time.Minute, r); err != nil || !d.Allowed || !d.Replayed {
			t.Fatalf("replay: %+v %v", d, err)
		}
		if d, found, err := l.Lookup(ctx, "admission"); err != nil || !found || !d.Allowed {
			t.Fatalf("Lookup: %+v found=%v err=%v", d, found, err)
		}
		if d, err := l.Allow(ctx, r); err != nil || !d.Allowed {
			t.Fatalf("replay consumed extra quota: %+v %v", d, err)
		}
		if d, err := l.Allow(ctx, r); err != nil || d.Allowed {
			t.Fatalf("quota not exhausted: %+v %v", d, err)
		}
	})
}
