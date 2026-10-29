package contracts_test

import (
	"context"
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
	quota := pacelock.Quota{Name: "q", Policy: pacelock.Policy{Rate: 1, Period: 20 * time.Millisecond, Burst: 1}}
	limiter, err := pacelock.NewStatic(driver, scope.Namespace, scope.Domain, quota)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if r, err := limiter.AllowOnce(ctx, "id", 20*time.Millisecond, pacelock.Charge{Name: quota.Name, Cost: 1}); err != nil || !r.Allowed {
		t.Fatal(r, err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, found, err := limiter.Lookup(ctx, "id"); err != nil || found {
		t.Fatal(found, err)
	}
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
	if deleted != 2 {
		t.Fatalf("deleted %d, want quota and receipt", deleted)
	}
}
