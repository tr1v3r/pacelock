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
	redisdriver "github.com/tr1v3r/pacelock/driver/redis"
)

func bind(t *testing.T, l *contractLimiter, quotas ...pacelock.Quota) *pacelock.StaticLimiter {
	t.Helper()
	b, err := pacelock.NewStatic(l.driver, l.scope.Namespace, l.scope.Domain, quotas...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func quotaCharge(q pacelock.Quota, cost int64) pacelock.Charge {
	return pacelock.Charge{Name: q.Name, Cost: cost}
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

func TestStaticDriverContract(t *testing.T) {
	for _, backend := range boundBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			t.Run("atomic copied policies and shared static state", func(t *testing.T) { testBoundAtomic(t, backend) })
			t.Run("independent subset admissions", func(t *testing.T) { testBoundSubsets(t, backend) })
			t.Run("invalid subset is uncharged", func(t *testing.T) { testBoundInvalidCharges(t, backend) })
			t.Run("constructor validation", func(t *testing.T) { testStaticConstructorValidation(t, backend) })
			t.Run("blocking wait", func(t *testing.T) { testStaticWait(t, backend) })
			t.Run("equivalent effective policies", func(t *testing.T) { testBoundEquivalentPolicies(t, backend) })
			t.Run("concurrent admission", func(t *testing.T) { testBoundConcurrency(t, backend) })
		})
	}
}

func testBoundSubsets(t *testing.T, backend backend) {
	ctx := context.Background()
	quotas := boundQuotas()
	b := bind(t, limiter(t, backend, 0, freshDomain()), quotas...)
	call := func(charges ...pacelock.Charge) (pacelock.Decision, error) {
		return b.Allow(ctx, charges...)
	}
	if d, err := call(quotaCharge(quotas[0], 2)); err != nil || !d.Allowed {
		t.Fatalf("exhaust RPM: %+v %v", d, err)
	}
	if d, err := call(quotaCharge(quotas[1], 4)); err != nil || !d.Allowed {
		t.Fatalf("omitted exhausted RPM blocked tokens: %+v %v", d, err)
	}
	if d, err := call(quotaCharge(quotas[0], 1)); err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"rpm"}) {
		t.Fatalf("selected denial: %+v %v", d, err)
	}
	if d, err := call(quotaCharge(quotas[1], 2)); err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"tokens"}) {
		t.Fatalf("later denial rolled back prior token debit: %+v %v", d, err)
	}
	if d, err := call(quotaCharge(quotas[1], 1)); err != nil || !d.Allowed {
		t.Fatalf("omitted token was debited or denied call charged: %+v %v", d, err)
	}
}

func testBoundAtomic(t *testing.T, backend backend) {
	l := limiter(t, backend, 0, freshDomain())
	quotas := boundQuotas()
	b := bind(t, l, quotas...)
	charges := []pacelock.Charge{quotaCharge(quotas[1], 4), quotaCharge(quotas[0], 1)}
	quotas[0].Name = "changed"
	quotas[0].Policy.Burst = 20
	quotas[1].Policy.Rate = 99
	ctx := context.Background()
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
	fresh := pacelock.Quota{Name: "fresh", Policy: boundQuotas()[0].Policy}
	changed := bind(t, l, fresh, quotas[1])
	if _, err := changed.Allow(ctx, quotaCharge(fresh, 1), quotaCharge(quotas[1], 1)); !errors.Is(err, pacelock.ErrPolicyMismatch) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatal(err)
	}
	mustAllow(t, l, rule("fresh", 2, 2))
}

func testBoundInvalidCharges(t *testing.T, backend backend) {
	cases := []struct {
		name    string
		charges []pacelock.Charge
	}{
		{"empty", nil},
		{"unknown name", []pacelock.Charge{{Name: "tokens", Cost: 1}, {Name: "other", Cost: 1}}},
		{"zero charge", []pacelock.Charge{{Name: "tokens", Cost: 1}, {}}},
		{"zero cost", []pacelock.Charge{{Name: "tokens", Cost: 1}, {Name: "rpm"}}},
		{"negative cost", []pacelock.Charge{{Name: "tokens", Cost: 1}, {Name: "rpm", Cost: -1}}},
		{"cost exceeds burst", []pacelock.Charge{{Name: "tokens", Cost: 1}, {Name: "rpm", Cost: 3}}},
		{"duplicate name", []pacelock.Charge{{Name: "tokens", Cost: 1}, {Name: "tokens", Cost: 1}}},
	}
	for _, tc := range cases {
		for _, operation := range []string{"allow_static", "wait_static"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				ctx := context.Background()
				quotas := boundQuotas()
				b := bind(t, limiter(t, backend, 0, freshDomain()), quotas...)
				// Put a valid charge first: rejection must not partially debit it,
				// even when no policy has yet been registered in the backend.
				charges := tc.charges
				var err error
				if operation == "wait_static" {
					err = b.Wait(ctx, charges...)
				} else {
					_, err = b.Allow(ctx, charges...)
				}
				if !errors.Is(err, pacelock.ErrInvalid) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatalf("local rejection: %v (outcome %v)", err, pacelock.OutcomeOf(err))
				}
				full := []pacelock.Charge{quotaCharge(quotas[0], 2), quotaCharge(quotas[1], 5)}
				if d, err := b.Allow(ctx, full...); err != nil || !d.Allowed {
					t.Fatalf("invalid admission debited quotas: %+v %v", d, err)
				}
				if d, err := b.Allow(ctx, quotaCharge(quotas[0], 1), quotaCharge(quotas[1], 1)); err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"rpm", "tokens"}) {
					t.Fatalf("full debit after local rejection: %+v %v", d, err)
				}
			})
		}
	}
}

func testBoundEquivalentPolicies(t *testing.T, backend backend) {
	for _, period := range []time.Duration{2*time.Minute + time.Nanosecond, 2*time.Minute + 2*time.Microsecond} {
		t.Run(period.String(), func(t *testing.T) {
			ctx := context.Background()
			domain := freshDomain()
			quota := pacelock.Quota{Name: "q", Policy: pacelock.Policy{Rate: 1, Period: time.Minute + time.Nanosecond, Burst: 3}}
			equivalent := quota
			equivalent.Policy.Rate = 2
			equivalent.Policy.Period = period
			b := bind(t, limiter(t, backend, 0, domain), quota)
			other := bind(t, limiter(t, backend, 1, domain), equivalent)
			charge := quotaCharge(quota, 1)
			// Construction snapshots policy values; charges carry only names and costs.
			quota.Name = "mutated"
			quota.Policy = pacelock.Policy{}
			if d, err := b.Allow(ctx, charge); err != nil || !d.Allowed {
				t.Fatalf("charge snapshot: %+v %v", d, err)
			}
			if d, err := other.Allow(ctx, quotaCharge(equivalent, 2)); err != nil || !d.Allowed {
				t.Fatalf("equivalent policy rejected: %+v %v", d, err)
			}
			if d, err := other.Allow(ctx, quotaCharge(equivalent, 1)); err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"q"}) || d.RetryAfter <= 0 {
				t.Fatalf("equivalent policies did not share quota state: %+v %v", d, err)
			}
		})
	}
}

func testBoundConcurrency(t *testing.T, backend backend) {
	quotas := []pacelock.Quota{
		{Name: "rpm", Policy: pacelock.Policy{Rate: 1, Period: time.Minute, Burst: 20}},
		{Name: "tokens", Policy: pacelock.Policy{Rate: 1, Period: time.Minute, Burst: 60}},
	}
	domain := freshDomain()
	bounds := [2]*pacelock.StaticLimiter{
		bind(t, limiter(t, backend, 0, domain), quotas...),
		bind(t, limiter(t, backend, 1, domain), quotas...),
	}
	charges := []pacelock.Charge{quotaCharge(quotas[1], 3), quotaCharge(quotas[0], 1)}
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
		ordered := charges
		if i%2 == 0 {
			ordered = []pacelock.Charge{charges[1], charges[0]}
		}
		d, err := bounds[i%2].Allow(ctx, ordered...)
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
}

func testStaticConstructorValidation(t *testing.T, backend backend) {
	valid := boundQuotas()[0]
	cases := []struct {
		name   string
		mutate func(*pacelock.Quota)
	}{
		{"invalid rate", func(q *pacelock.Quota) { q.Policy.Rate = 0 }},
		{"invalid period", func(q *pacelock.Quota) { q.Policy.Period = 0 }},
		{"excessive period", func(q *pacelock.Quota) { q.Policy.Period = pacelock.MaxHorizon + 1 }},
		{"invalid burst", func(q *pacelock.Quota) { q.Policy.Burst = 0 }},
		{"excessive burst horizon", func(q *pacelock.Quota) { q.Policy.Burst = 1441 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := valid
			tc.mutate(&q)
			domain := freshDomain()
			if _, err := pacelock.NewStatic(backend.driver[0], "contract-"+domain, domain, q); !errors.Is(err, pacelock.ErrInvalid) {
				t.Fatalf("invalid constructor: %v", err)
			}
			// Invalid construction must not register backend policy or consume capacity.
			b := bind(t, limiter(t, backend, 0, domain), valid)
			if d, err := b.Allow(context.Background(), quotaCharge(valid, valid.Policy.Burst)); err != nil || !d.Allowed {
				t.Fatalf("constructor changed state: %+v %v", d, err)
			}
		})
	}
}

func testStaticWait(t *testing.T, backend backend) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	quotas := boundQuotas()
	quotas[0].Policy.Period = 20 * time.Millisecond
	b := bind(t, limiter(t, backend, 0, freshDomain()), quotas...)
	if d, err := b.Allow(ctx, quotaCharge(quotas[0], 2)); err != nil || !d.Allowed {
		t.Fatal(d, err)
	}
	if err := b.Wait(ctx, quotaCharge(quotas[1], 3), quotaCharge(quotas[0], 2)); err != nil {
		t.Fatal(err)
	}
	// Denied retries neither debit the fresh dimension nor require all quotas.
	if d, err := b.Allow(ctx, quotaCharge(quotas[1], 2)); err != nil || !d.Allowed {
		t.Fatalf("wait partially or repeatedly charged: %+v %v", d, err)
	}
	if d, err := b.Allow(ctx, quotaCharge(quotas[1], 1)); err != nil || d.Allowed {
		t.Fatalf("wait did not debit selected quota: %+v %v", d, err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err := b.Wait(canceled, quotaCharge(quotas[0], 1)); !errors.Is(err, context.Canceled) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatal(err)
	}
}
