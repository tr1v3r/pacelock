package contracts_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	redisdriver "github.com/tr1v3r/pacelock/driver/redis"
)

func TestRedisPublicPruneCycle(t *testing.T) {
	address := os.Getenv("PACELOCK_REDIS_ADDR")
	if address == "" {
		t.Skip("set PACELOCK_REDIS_ADDR")
	}
	client := redis.NewClient(&redis.Options{Addr: address, ContextTimeoutEnabled: true})
	defer client.Close()
	driver, err := redisdriver.New(client)
	if err != nil {
		t.Fatal(err)
	}
	scope := pacelock.Scope{Namespace: "pacelock-public-prune", Domain: fmt.Sprint(time.Now().UnixNano())}
	quota := pacelock.Quota{Name: "q", Policy: pacelock.Policy{Revision: 1, Rate: 1, Period: 20 * time.Millisecond, Burst: 1}}
	limiter, err := pacelock.New(driver, scope.Namespace, scope.Domain)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := limiter.UpdateQuotas(ctx, quota); err != nil {
		t.Fatal(err)
	}
	if r, err := limiter.Allow(ctx, pacelock.Charge{Name: quota.Name, Cost: 1}); err != nil || !r.Allowed {
		t.Fatal(r, err)
	}
	time.Sleep(30 * time.Millisecond)
	cursor := redisdriver.PruneCursor{}
	var deleted int64
	for steps := 0; !cursor.Done(); steps++ {
		if steps > 10000 {
			t.Fatal("cleanup did not complete")
		}
		r, err := driver.Prune(ctx, scope, cursor, 32)
		if err != nil {
			t.Fatal(err)
		}
		if r.Examined > 32 {
			t.Fatal("cleanup exceeded budget")
		}
		deleted += r.Deleted
		cursor = r.Cursor
	}
	if deleted != 1 {
		t.Fatalf("deleted %d, want expired quota", deleted)
	}
}

// Seed the retired wire format directly: no current static writer is needed.
func TestRedisLegacyDebtUpgrade(t *testing.T) {
	address := os.Getenv("PACELOCK_REDIS_ADDR")
	if address == "" {
		t.Skip("set PACELOCK_REDIS_ADDR")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, ContextTimeoutEnabled: true})
	defer client.Close()
	driver, err := redisdriver.New(client)
	if err != nil {
		t.Fatal(err)
	}
	scope := pacelock.Scope{Namespace: "legacy-upgrade", Domain: freshDomain()}
	l, err := pacelock.New(driver, scope.Namespace, scope.Domain)
	if err != nil {
		t.Fatal(err)
	}
	now, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	prefix := "pacelock:{" + pgScopeID(scope) + "}:"
	debtKey := prefix + pgItemID("q")
	receiptKey := prefix + "attempt:" + pgItemID("legacy")
	raw := fmt.Sprintf("%d:%d:1", now.Add(time.Hour).UnixMicro(), time.Hour.Microseconds())
	if err := client.MSet(ctx, debtKey, raw, receiptKey, "retained legacy receipt").Err(); err != nil {
		t.Fatal(err)
	}
	charge := pacelock.Charge{Name: "q", Cost: 1}
	if _, err := l.Allow(ctx, charge); !errors.Is(err, pacelock.ErrPolicyMissing) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatalf("legacy debt implied policy: %v", err)
	}
	q := pacelock.Quota{Name: "q", Policy: pacelock.Policy{Revision: 1, Rate: 1, Period: time.Hour, Burst: 1}}
	if err := l.UpdateQuotas(ctx, q); err != nil {
		t.Fatal(err)
	}
	if got, err := client.Get(ctx, debtKey).Result(); err != nil || got != raw {
		t.Fatalf("publication changed debt: %q %v", got, err)
	}
	if decision, err := l.Allow(ctx, charge); err != nil || decision.Allowed || decision.RetryAfter < 50*time.Minute {
		t.Fatalf("legacy debt lost: %+v %v", decision, err)
	}
	if got, err := client.Get(ctx, receiptKey).Result(); err != nil || got != "retained legacy receipt" {
		t.Fatalf("legacy receipt changed: %q %v", got, err)
	}
}
