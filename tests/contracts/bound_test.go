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
			t.Run("idempotency and recovery", func(t *testing.T) { testBoundIdempotency(t, backend) })
			t.Run("independent subset admissions", func(t *testing.T) { testBoundSubsets(t, backend) })
			t.Run("subset idempotency", func(t *testing.T) { testBoundSubsetIdempotency(t, backend) })
			t.Run("invalid subset is uncharged", func(t *testing.T) { testBoundInvalidCharges(t, backend) })
			t.Run("constructor validation", func(t *testing.T) { testStaticConstructorValidation(t, backend) })
			t.Run("blocking wait", func(t *testing.T) { testStaticWait(t, backend) })
			t.Run("equivalent effective policies", func(t *testing.T) { testBoundEquivalentPolicies(t, backend) })
			t.Run("concurrent admission and reordered retries", func(t *testing.T) { testBoundConcurrency(t, backend) })
		})
	}
}

func testBoundSubsets(t *testing.T, backend backend) {
	for _, once := range []bool{false, true} {
		ctx := context.Background()
		quotas := boundQuotas()
		b := bind(t, limiter(t, backend, 0, freshDomain()), quotas...)
		call := func(id string, charges ...pacelock.Charge) (pacelock.Decision, error) {
			if once {
				r, err := b.AllowOnce(ctx, id, time.Minute, charges...)
				return r.Decision, err
			}
			return b.Allow(ctx, charges...)
		}
		if d, err := call("exhaust-rpm", quotaCharge(quotas[0], 2)); err != nil || !d.Allowed {
			t.Fatalf("once=%t exhaust RPM: %+v %v", once, d, err)
		}
		if d, err := call("tokens-only", quotaCharge(quotas[1], 4)); err != nil || !d.Allowed {
			t.Fatalf("omitted exhausted RPM blocked tokens: %+v %v", d, err)
		}
		if d, err := call("deny-rpm", quotaCharge(quotas[0], 1)); err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"rpm"}) {
			t.Fatalf("selected denial: %+v %v", d, err)
		}
		if d, err := call("tokens-still-debited", quotaCharge(quotas[1], 2)); err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"tokens"}) {
			t.Fatalf("later denial rolled back prior token debit: %+v %v", d, err)
		}
		if d, err := call("remaining-token", quotaCharge(quotas[1], 1)); err != nil || !d.Allowed {
			t.Fatalf("omitted token was debited or denied call charged: %+v %v", d, err)
		}
	}
}

func testBoundSubsetIdempotency(t *testing.T, backend backend) {
	ctx := context.Background()
	l := limiter(t, backend, 0, freshDomain())
	quotas := boundQuotas()
	omitted := pacelock.Quota{Name: "omitted", Policy: quotas[0].Policy}
	b := bind(t, l, quotas[0], omitted, quotas[1])
	// An exhausted omitted quota must neither block admission nor enter its receipt.
	if d, err := b.Allow(ctx, quotaCharge(omitted, 2)); err != nil || !d.Allowed {
		t.Fatalf("exhaust omitted: %+v %v", d, err)
	}
	first, err := b.AllowOnce(ctx, "subset", time.Minute, quotaCharge(quotas[1], 3), quotaCharge(quotas[0], 1))
	if err != nil || !first.Allowed || first.Replayed {
		t.Fatalf("subset admission: %+v %v", first, err)
	}
	// Removing or changing only unselected binding entries cannot change identity.
	omitted.Policy.Burst++
	for _, other := range []*pacelock.StaticLimiter{b, bind(t, l, quotas...), bind(t, l, omitted, quotas[0], quotas[1])} {
		r, err := other.AllowOnce(ctx, "subset", time.Minute, quotaCharge(quotas[0], 1), quotaCharge(quotas[1], 3))
		if err != nil || !r.Replayed || !reflect.DeepEqual(r.Decision, first.Decision) {
			t.Fatalf("canonical subset replay: %+v %v", r, err)
		}
	}
	for _, selected := range [][]pacelock.Charge{
		{quotaCharge(quotas[0], 1)},
		{quotaCharge(quotas[1], 3)},
		{quotaCharge(quotas[0], 1), quotaCharge((pacelock.Quota{Name: "omitted", Policy: quotas[0].Policy}), 1), quotaCharge(quotas[1], 3)},
	} {
		if _, err := b.AllowOnce(ctx, "subset", time.Minute, selected...); !errors.Is(err, pacelock.ErrConflict) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
			t.Fatalf("changed subset must conflict without debit: %v", err)
		}
	}
	if r, err := l.allowRulesOnce(ctx, "subset", time.Minute, rule("rpm", 2, 1), rule("tokens", 5, 3)); err != nil || !r.Replayed {
		t.Fatalf("selected-only fingerprint: %+v %v", r, err)
	}
	if d, err := b.Allow(ctx, quotaCharge(quotas[1], 2), quotaCharge(quotas[0], 1)); err != nil || !d.Allowed {
		t.Fatalf("replay/conflict debited subset: %+v %v", d, err)
	}
	denied, err := b.AllowOnce(ctx, "denied-subset", time.Minute, quotaCharge(quotas[1], 1))
	if err != nil || denied.Allowed || denied.Replayed || !reflect.DeepEqual(denied.Limited, []string{"tokens"}) {
		t.Fatalf("subset denial: %+v %v", denied, err)
	}
	if replay, err := b.AllowOnce(ctx, "denied-subset", time.Minute, quotaCharge(quotas[1], 1)); err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Decision, denied.Decision) {
		t.Fatalf("subset denial replay: %+v %v", replay, err)
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

func testBoundIdempotency(t *testing.T, backend backend) {
	l := limiter(t, backend, 0, freshDomain())
	originalQuotas := boundQuotas()
	b := bind(t, l, originalQuotas...)
	ctx := context.Background()
	first, err := b.AllowOnce(ctx, "id", time.Minute, quotaCharge(originalQuotas[1], 3), quotaCharge(originalQuotas[0], 1))
	if err != nil || !first.Allowed || first.Replayed {
		t.Fatalf("%+v %v", first, err)
	}
	replay, err := b.AllowOnce(ctx, "id", time.Minute, quotaCharge(originalQuotas[0], 1), quotaCharge(originalQuotas[1], 3))
	if err != nil || !replay.Replayed || !reflect.DeepEqual(first.Decision, replay.Decision) {
		t.Fatalf("charge reordering changed attempt: %+v %v", replay, err)
	}
	quotas := boundQuotas()
	quotas[0], quotas[1] = quotas[1], quotas[0]
	reordered := bind(t, l, quotas...)
	if _, err := reordered.AllowOnce(ctx, "id", time.Minute, quotaCharge(originalQuotas[0], 1), quotaCharge(originalQuotas[1], 3)); !errors.Is(err, pacelock.ErrConflict) {
		t.Fatalf("changed binding order: %v", err)
	}
	quotas = boundQuotas()
	quotas[0].Policy.Burst++
	changedPolicy := bind(t, l, quotas...)
	if _, err := changedPolicy.AllowOnce(ctx, "id", time.Minute, quotaCharge(quotas[0], 1), quotaCharge(quotas[1], 3)); !errors.Is(err, pacelock.ErrConflict) {
		t.Fatalf("changed effective policy: %v", err)
	}
	if _, err := b.AllowOnce(ctx, "id", time.Minute, quotaCharge(originalQuotas[0], 1), quotaCharge(originalQuotas[1], 2)); !errors.Is(err, pacelock.ErrConflict) {
		t.Fatalf("changed cost: %v", err)
	}
	if _, err := b.AllowOnce(ctx, "id", 2*time.Minute, quotaCharge(originalQuotas[0], 1), quotaCharge(originalQuotas[1], 3)); !errors.Is(err, pacelock.ErrConflict) {
		t.Fatalf("changed retention: %v", err)
	}
	// Lookup is domain-scoped and does not require the original bound quota set.
	if d, found, err := reordered.Lookup(ctx, "id"); err != nil || !found || !reflect.DeepEqual(d, first.Decision) {
		t.Fatalf("lookup: %+v %t %v", d, found, err)
	}
	if _, found, err := b.Lookup(ctx, "missing"); err != nil || found {
		t.Fatalf("missing lookup: %t %v", found, err)
	}
	// A fresh static instance uses the existing ordered rule fingerprint protocol.
	if r, err := l.allowRulesOnce(ctx, "id", time.Minute, rule("rpm", 2, 1), rule("tokens", 5, 3)); err != nil || !r.Replayed {
		t.Fatalf("low-level replay: %+v %v", r, err)
	}
	denied, err := b.AllowOnce(ctx, "denied", time.Minute, quotaCharge(originalQuotas[0], 1), quotaCharge(originalQuotas[1], 3))
	if err != nil || denied.Allowed || denied.Replayed {
		t.Fatalf("denied receipt: %+v %v", denied, err)
	}
	if r, err := b.AllowOnce(ctx, "denied", time.Minute, quotaCharge(originalQuotas[1], 3), quotaCharge(originalQuotas[0], 1)); err != nil || r.Allowed || !r.Replayed || !reflect.DeepEqual(r.Decision, denied.Decision) {
		t.Fatalf("denial replay: %+v %v", r, err)
	}
	// A replay must not have consumed the one remaining request and two tokens.
	if d, err := b.Allow(ctx, quotaCharge(originalQuotas[0], 1), quotaCharge(originalQuotas[1], 2)); err != nil || !d.Allowed {
		t.Fatalf("replay debited quotas: %+v %v", d, err)
	}
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
		for _, operation := range []string{"allow_static", "allow_static_once", "wait_static"} {
			once := operation == "allow_static_once"
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				ctx := context.Background()
				quotas := boundQuotas()
				b := bind(t, limiter(t, backend, 0, freshDomain()), quotas...)
				// Put a valid charge first: rejection must not partially debit it,
				// even when no policy has yet been registered in the backend.
				charges := tc.charges
				var err error
				if once {
					_, err = b.AllowOnce(ctx, "rejected", time.Minute, charges...)
				} else if operation == "wait_static" {
					err = b.Wait(ctx, charges...)
				} else {
					_, err = b.Allow(ctx, charges...)
				}
				if !errors.Is(err, pacelock.ErrInvalid) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatalf("local rejection: %v (outcome %v)", err, pacelock.OutcomeOf(err))
				}
				full := []pacelock.Charge{quotaCharge(quotas[0], 2), quotaCharge(quotas[1], 5)}
				if once {
					if _, found, err := b.Lookup(ctx, "rejected"); err != nil || found {
						t.Fatalf("invalid admission retained a receipt: found=%t err=%v", found, err)
					}
					r, err := b.AllowOnce(ctx, "rejected", time.Minute, full...)
					if err != nil || !r.Allowed || r.Replayed {
						t.Fatalf("invalid admission debited or reserved its ID: %+v %v", r, err)
					}
				} else if d, err := b.Allow(ctx, full...); err != nil || !d.Allowed {
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
			first, err := b.AllowOnce(ctx, "equivalent", time.Minute, charge)
			if err != nil || !first.Allowed || first.Replayed {
				t.Fatalf("charge snapshot: %+v %v", first, err)
			}
			for _, bound := range []*pacelock.StaticLimiter{b, other} {
				replay, err := bound.AllowOnce(ctx, "equivalent", time.Minute, quotaCharge(equivalent, 1))
				if err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Decision, first.Decision) {
					t.Fatalf("equivalent policy replay: %+v %v", replay, err)
				}
			}
			if d, err := b.Allow(ctx, quotaCharge(equivalent, 2)); err != nil || !d.Allowed {
				t.Fatalf("equivalent policy rejected or replay debited: %+v %v", d, err)
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
	if d, err := shared.Allow(context.Background(), quotaCharge(quotas[0], 19), quotaCharge(quotas[1], 57)); err != nil || !d.Allowed {
		t.Fatalf("concurrent replays debited more than once: %+v %v", d, err)
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
