package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func integration(t *testing.T) (*Limiter, *redis.Client) {
	t.Helper()
	addr := os.Getenv("PACELOCK_REDIS_ADDR")
	if addr == "" {
		t.Skip("set PACELOCK_REDIS_ADDR to run real Redis integration tests")
	}
	c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		t.Fatal(err)
	}
	l, err := New(c, "pacelock-test", fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		it := c.Scan(ctx, 0, l.prefix+"*", 100).Iterator()
		for it.Next(ctx) {
			c.Del(ctx, it.Val())
		}
		c.Close()
	})
	return l, c
}

func rule(name string, burst, cost int64) Rule {
	return Rule{Name: name, Policy: Policy{Rate: 1, Period: time.Hour, Burst: burst}, Cost: cost}
}

func TestRedisWeightedAndAtomic(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	rpm := rule("rpm", 2, 1)
	tokens := rule("tokens", 5, 4)
	d, err := l.Allow(ctx, rpm, tokens)
	if err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	before := c.Get(ctx, l.key("rpm")).Val()
	d, err = l.Allow(ctx, rpm, tokens)
	if err != nil || d.Allowed || d.RetryAfter <= 0 || len(d.Limited) != 1 || d.Limited[0] != "tokens" {
		t.Fatalf("%+v %v", d, err)
	}
	if c.Get(ctx, l.key("rpm")).Val() != before {
		t.Fatal("denial partially charged RPM")
	}
	tokens.Cost = 1
	if d, err = l.Allow(ctx, rpm, tokens); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	d, err = l.Allow(ctx, rpm, tokens)
	if err != nil || d.Allowed || len(d.Limited) != 2 {
		t.Fatalf("%+v %v", d, err)
	}
	ttl := c.PTTL(ctx, l.key("tokens")).Val()
	if ttl != -1 {
		t.Fatalf("ttl=%s", ttl)
	}
}

func TestRedisConcurrentInstances(t *testing.T) {
	l, c := integration(t)
	second := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer second.Close()
	l2 := &Limiter{client: second, prefix: l.prefix}
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			current := l
			if i%2 == 0 {
				current = l2
			}
			d, err := current.Allow(context.Background(), rule("shared", 20, 2))
			if err != nil {
				t.Error(err)
				return
			}
			if d.Allowed {
				accepted.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if got := accepted.Load(); got != 10 {
		t.Fatalf("admitted %d, want exactly 10 across clients", got)
	}
}

func TestRedisPolicyAndCorruptionDoNotPartiallyWrite(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("existing", 3, 1)
	if _, err := l.Allow(ctx, r); err != nil {
		t.Fatal(err)
	}
	before := c.Get(ctx, l.key(r.Name)).Val()
	for _, field := range []string{"step", "burst"} {
		t.Run(field, func(t *testing.T) {
			changed := r
			if field == "step" {
				changed.Policy.Period = 2 * time.Hour
			} else {
				changed.Policy.Burst = 4
			}
			if _, err := l.Allow(ctx, rule("fresh", 1, 1), changed); !errors.Is(err, ErrPolicyMismatch) || OutcomeOf(err) != NotCharged {
				t.Fatalf("err=%v", err)
			}
			if c.Exists(ctx, l.key("fresh")).Val() != 0 || c.Get(ctx, l.key(r.Name)).Val() != before {
				t.Fatal("policy error mutated quota state")
			}
		})
	}
	for _, raw := range []string{"broken", "Q2|1:1:1", "Q2|" + before} {
		t.Run(raw, func(t *testing.T) {
			if err := c.Set(ctx, l.key(r.Name), raw, 0).Err(); err != nil {
				t.Fatal(err)
			}
			if _, err := l.Allow(ctx, rule("fresh", 1, 1), r); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
				t.Fatalf("err=%v", err)
			}
			if c.Exists(ctx, l.key("fresh")).Val() != 0 || c.Get(ctx, l.key(r.Name)).Val() != raw {
				t.Fatal("state error mutated quota state")
			}
			if _, err := l.AllowOnce(ctx, "invalid-quota", time.Minute, rule("fresh-once", 1, 1), r); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
				t.Fatalf("AllowOnce state error: %v", err)
			}
			if c.Exists(ctx, l.key("fresh-once"), l.attemptKey("invalid-quota")).Val() != 0 || c.Get(ctx, l.key(r.Name)).Val() != raw {
				t.Fatal("AllowOnce state error wrote a quota or receipt")
			}
		})
	}
}

func TestRedisRefillAndExpiry(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := Rule{Name: "short", Policy: Policy{Rate: 1, Period: 200 * time.Millisecond, Burst: 1}, Cost: 1}
	if d, err := l.Allow(ctx, r); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	if d, err := l.Allow(ctx, r); err != nil || d.Allowed || d.RetryAfter <= 0 || d.RetryAfter > r.Policy.Period {
		t.Fatalf("%+v %v", d, err)
	}
	time.Sleep(220 * time.Millisecond)
	if c.Exists(ctx, l.key(r.Name)).Val() != 1 {
		t.Fatal("test must retain physically expired logical state")
	}
	r.Policy.Burst = 2 // A logically expired policy may change without physical cleanup.
	if d, err := l.Allow(ctx, r); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}

}

func TestRedisIndependentDomainsAndCanceledCall(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("quota", 1, 1)
	if _, err := l.Allow(ctx, r); err != nil {
		t.Fatal(err)
	}
	other, _ := New(c, "another-app", l.prefix)
	t.Cleanup(func() { c.Del(ctx, other.key(r.Name)) })
	if d, err := other.Allow(ctx, r); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.Allow(canceled, rule("untouched", 1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if c.Exists(ctx, l.key("untouched")).Val() != 0 {
		t.Fatal("pre-canceled call wrote state")
	}
}
