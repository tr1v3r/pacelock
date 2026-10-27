package contracts_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	redisdriver "github.com/tr1v3r/pacelock/drivers/redis"
)

func bind(t *testing.T, l *pacelock.Limiter, quotas ...pacelock.Quota) *pacelock.BoundLimiter {
	t.Helper()
	b, err := l.Bind(quotas...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func boundQuotas() []pacelock.Quota {
	return []pacelock.Quota{
		{Name: "rpm", Policy: pacelock.Policy{Rate: 1, Period: time.Minute, Burst: 2}},
		{Name: "tokens", Policy: pacelock.Policy{Rate: 1, Period: time.Minute, Burst: 5}},
	}
}

func boundBackends(t *testing.T) []backend {
	t.Helper()
	result := backends(t)
	if address := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS"); address != "" {
		b := backend{name: "redis-cluster"}
		for i := range b.driver {
			client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(address, ","), ContextTimeoutEnabled: true})
			t.Cleanup(func() { client.Close() })
			d, err := redisdriver.New(client)
			if err != nil {
				t.Fatal(err)
			}
			b.driver[i] = d
		}
		result = append(result, b)
	}
	return result
}

func TestBoundDriverContract(t *testing.T) {
	for _, backend := range boundBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			t.Run("atomic copied policies and compatibility", func(t *testing.T) { testBoundAtomic(t, backend) })
			t.Run("idempotency and recovery", func(t *testing.T) { testBoundIdempotency(t, backend) })
			t.Run("concurrent admission and reordered retries", func(t *testing.T) { testBoundConcurrency(t, backend) })
		})
	}
}

func testBoundAtomic(t *testing.T, backend backend) {
	l := limiter(t, backend, 0, freshDomain())
	quotas := boundQuotas()
	b := bind(t, l, quotas...)
	quotas[0].Name = "changed"
	quotas[0].Policy.Burst = 20
	quotas[1].Policy.Rate = 99
	ctx := context.Background()
	charges := []pacelock.Charge{{Name: "tokens", Cost: 4}, {Name: "rpm", Cost: 1}}
	if d, err := b.Allow(ctx, charges...); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	if d, err := b.Allow(ctx, charges...); err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"tokens"}) || d.RetryAfter <= 0 {
		t.Fatalf("%+v %v", d, err)
	}
	// Consuming the exact remainder proves a denial did not partially debit RPM.
	mustAllow(t, l, rule("rpm", 2, 1), rule("tokens", 5, 1))
	if d, err := b.Allow(ctx, charges...); err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"rpm", "tokens"}) {
		t.Fatalf("binding order: %+v %v", d, err)
	}
	changed := bind(t, l, pacelock.Quota{Name: "fresh", Policy: boundQuotas()[0].Policy}, quotas[1])
	if _, err := changed.Allow(ctx, pacelock.Charge{Name: "fresh", Cost: 1}, pacelock.Charge{Name: "tokens", Cost: 1}); !errors.Is(err, pacelock.ErrPolicyMismatch) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatal(err)
	}
	mustAllow(t, l, rule("fresh", 2, 2))
}

func testBoundIdempotency(t *testing.T, backend backend) {
	l := limiter(t, backend, 0, freshDomain())
	b := bind(t, l, boundQuotas()...)
	ctx := context.Background()
	first, err := b.AllowOnce(ctx, "id", time.Minute, pacelock.Charge{Name: "tokens", Cost: 3}, pacelock.Charge{Name: "rpm", Cost: 1})
	if err != nil || !first.Allowed || first.Replayed {
		t.Fatalf("%+v %v", first, err)
	}
	replay, err := b.AllowOnce(ctx, "id", time.Minute, pacelock.Charge{Name: "rpm", Cost: 1}, pacelock.Charge{Name: "tokens", Cost: 3})
	if err != nil || !replay.Replayed || !reflect.DeepEqual(first.Decision, replay.Decision) {
		t.Fatalf("charge reordering changed attempt: %+v %v", replay, err)
	}
	quotas := boundQuotas()
	quotas[0], quotas[1] = quotas[1], quotas[0]
	reordered := bind(t, l, quotas...)
	if _, err := reordered.AllowOnce(ctx, "id", time.Minute, pacelock.Charge{Name: "rpm", Cost: 1}, pacelock.Charge{Name: "tokens", Cost: 3}); !errors.Is(err, pacelock.ErrConflict) {
		t.Fatalf("changed binding order: %v", err)
	}
	quotas = boundQuotas()
	quotas[0].Policy.Burst++
	changedPolicy := bind(t, l, quotas...)
	if _, err := changedPolicy.AllowOnce(ctx, "id", time.Minute, pacelock.Charge{Name: "rpm", Cost: 1}, pacelock.Charge{Name: "tokens", Cost: 3}); !errors.Is(err, pacelock.ErrConflict) {
		t.Fatalf("changed effective policy: %v", err)
	}
	if _, err := b.AllowOnce(ctx, "id", time.Minute, pacelock.Charge{Name: "rpm", Cost: 1}, pacelock.Charge{Name: "tokens", Cost: 2}); !errors.Is(err, pacelock.ErrConflict) {
		t.Fatalf("changed cost: %v", err)
	}
	if _, err := b.AllowOnce(ctx, "id", 2*time.Minute, pacelock.Charge{Name: "rpm", Cost: 1}, pacelock.Charge{Name: "tokens", Cost: 3}); !errors.Is(err, pacelock.ErrConflict) {
		t.Fatalf("changed retention: %v", err)
	}
	// Lookup is domain-scoped and does not require the original bound quota set.
	if d, found, err := reordered.Lookup(ctx, "id"); err != nil || !found || !reflect.DeepEqual(d, first.Decision) {
		t.Fatalf("lookup: %+v %t %v", d, found, err)
	}
	if _, found, err := b.Lookup(ctx, "missing"); err != nil || found {
		t.Fatalf("missing lookup: %t %v", found, err)
	}
	// Low-level replay proves the binding used the existing ordered rule protocol.
	if r, err := l.AllowOnce(ctx, "id", time.Minute, rule("rpm", 2, 1), rule("tokens", 5, 3)); err != nil || !r.Replayed {
		t.Fatalf("low-level replay: %+v %v", r, err)
	}
	denied, err := b.AllowOnce(ctx, "denied", time.Minute, pacelock.Charge{Name: "rpm", Cost: 1}, pacelock.Charge{Name: "tokens", Cost: 3})
	if err != nil || denied.Allowed || denied.Replayed {
		t.Fatalf("denied receipt: %+v %v", denied, err)
	}
	if r, err := b.AllowOnce(ctx, "denied", time.Minute, pacelock.Charge{Name: "tokens", Cost: 3}, pacelock.Charge{Name: "rpm", Cost: 1}); err != nil || r.Allowed || !r.Replayed || !reflect.DeepEqual(r.Decision, denied.Decision) {
		t.Fatalf("denial replay: %+v %v", r, err)
	}
	// A replay must not have consumed the one remaining request and two tokens.
	if d, err := b.Allow(ctx, pacelock.Charge{Name: "rpm", Cost: 1}, pacelock.Charge{Name: "tokens", Cost: 2}); err != nil || !d.Allowed {
		t.Fatalf("replay debited quotas: %+v %v", d, err)
	}
}

func testBoundConcurrency(t *testing.T, backend backend) {
	quotas := []pacelock.Quota{
		{Name: "rpm", Policy: pacelock.Policy{Rate: 1, Period: time.Minute, Burst: 20}},
		{Name: "tokens", Policy: pacelock.Policy{Rate: 1, Period: time.Minute, Burst: 60}},
	}
	domain := freshDomain()
	bounds := [2]*pacelock.BoundLimiter{
		bind(t, limiter(t, backend, 0, domain), quotas...),
		bind(t, limiter(t, backend, 1, domain), quotas...),
	}
	charges := []pacelock.Charge{{Name: "tokens", Cost: 3}, {Name: "rpm", Cost: 1}}
	wave := func(call func(context.Context, int)) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range 100 {
			wg.Add(1)
			go func(i int) { defer wg.Done(); <-start; call(ctx, i) }(i)
		}
		close(start)
		wg.Wait()
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
	}
	var accepted atomic.Int64
	wave(func(ctx context.Context, i int) {
		d, err := bounds[i%2].Allow(ctx, charges...)
		if err != nil {
			t.Error(err)
			return
		}
		if d.Allowed {
			accepted.Add(1)
		}
	})
	if accepted.Load() != 20 {
		t.Fatalf("admitted %d, want 20", accepted.Load())
	}
	shared := bind(t, limiter(t, backend, 0, freshDomain()), quotas...)
	var originals atomic.Int64
	wave(func(ctx context.Context, i int) {
		ordered := charges
		if i%2 == 0 {
			ordered = []pacelock.Charge{charges[1], charges[0]}
		}
		r, err := shared.AllowOnce(ctx, "shared-id", time.Minute, ordered...)
		if err != nil || !r.Allowed {
			t.Errorf("receipt=%+v err=%v", r, err)
			return
		}
		if !r.Replayed {
			originals.Add(1)
		}
	})
	if originals.Load() != 1 {
		t.Fatalf("original receipts=%d, want 1", originals.Load())
	}
	if d, err := shared.Allow(context.Background(), pacelock.Charge{Name: "rpm", Cost: 19}, pacelock.Charge{Name: "tokens", Cost: 57}); err != nil || !d.Allowed {
		t.Fatalf("concurrent replays debited more than once: %+v %v", d, err)
	}
}
