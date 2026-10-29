package contracts_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/pacelock"
	"github.com/tr1v3r/pacelock/driver/postgres"
	redisdriver "github.com/tr1v3r/pacelock/driver/redis"
)

func dynamicQuota(rev, rate, burst int64) pacelock.Quota {
	return pacelock.Quota{Name: "dynamic", Policy: pacelock.Policy{Revision: rev, Rate: rate, Period: time.Minute, Burst: burst}}
}
func updateQuota(t *testing.T, l *contractLimiter, q pacelock.Quota) {
	t.Helper()
	if err := l.UpdateQuotas(context.Background(), q); err != nil {
		t.Fatal(err)
	}
}
func managedCharge() pacelock.Charge { return pacelock.Charge{Name: "dynamic", Cost: 1} }
func pruneManagedScope(t *testing.T, b backend, domain string) {
	t.Helper()
	ctx := context.Background()
	scope := pacelock.Scope{Namespace: "contract-" + domain, Domain: domain}
	switch d := b.driver[0].(type) {
	case *postgres.Driver:
		if _, err := d.Prune(ctx, scope, 100); err != nil {
			t.Fatal(err)
		}
	case *redisdriver.Driver:
		cursor := redisdriver.PruneCursor{}
		for {
			res, err := d.Prune(ctx, scope, cursor, redisdriver.MaxPruneKeys)
			if err != nil {
				t.Fatal(err)
			}
			if res.Cursor.Done() {
				break
			}
			cursor = res.Cursor
		}
	}
}

// Admission fails closed until policies are explicitly published.
func TestDefaultDynamicFailClosed(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			l := limiter(t, b, 0, freshDomain())
			c := managedCharge()
			if _, err := l.Allow(ctx, c); !errors.Is(err, pacelock.ErrPolicyMissing) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
				t.Fatalf("unpublished policy admitted: %v", err)
			}
			updateQuota(t, l, dynamicQuota(1, 1, 3))
			if d, err := l.Allow(ctx, pacelock.Charge{Name: c.Name, Cost: 3}); err != nil || !d.Allowed {
				t.Fatalf("missing policy call charged debt: %+v %v", d, err)
			}
		})
	}
}

func TestDynamicPolicies(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			domain := freshDomain()
			l := limiter(t, b, 0, domain)
			peer := limiter(t, b, 1, domain)
			c := managedCharge()
			if _, err := l.Allow(ctx, c); !errors.Is(err, pacelock.ErrPolicyMissing) {
				t.Fatal("unpublished", err)
			}
			updateQuota(t, l, dynamicQuota(1, 1, 2))
			admitted, err := l.Allow(ctx, c)
			if err != nil || !admitted.Allowed {
				t.Fatal(admitted, err)
			}
			// Same caller charge, no refresh: faster policy must not forgive old one-minute debt.
			updateQuota(t, l, dynamicQuota(2, 2, 1))
			decision, err := peer.Allow(ctx, c)
			if err != nil || decision.Allowed || decision.RetryAfter < 50*time.Second {
				t.Fatal("debt lost", decision, err)
			}
			updateQuota(t, l, dynamicQuota(3, 0, 1))
			if _, err = peer.Allow(ctx, c); !errors.Is(err, pacelock.ErrPaused) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
				t.Fatal("pause", err)
			}
			if err = l.UpdateQuotas(ctx, dynamicQuota(1, 1, 2)); !errors.Is(err, pacelock.ErrPolicyMismatch) {
				t.Fatal("stale update", err)
			}
			updateQuota(t, l, dynamicQuota(3, 0, 1))
			updateQuota(t, l, dynamicQuota(4, 1, 2))
			decision, err = peer.Allow(ctx, c)
			if err != nil || !decision.Allowed {
				t.Fatal(decision, err)
			}
			decision, err = peer.Allow(ctx, c)
			if err != nil || decision.Allowed {
				t.Fatal("resume reset debt", decision, err)
			}
		})
	}
}

func TestDynamicIdlePruneAndAtomicUpdate(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			domain := freshDomain()
			l := limiter(t, b, 0, domain)
			c := managedCharge()
			updateQuota(t, l, dynamicQuota(1, 0, 2))
			pruneManagedScope(t, b, domain)
			if _, err := l.Allow(ctx, c); !errors.Is(err, pacelock.ErrPaused) {
				t.Fatal("prune lost pause", err)
			}
			other := dynamicQuota(3, 1, 2)
			other.Name = "other"
			updateQuota(t, l, other)
			other.Policy.Revision = 2
			if err := l.UpdateQuotas(ctx, dynamicQuota(2, 1, 2), other); !errors.Is(err, pacelock.ErrPolicyMismatch) {
				t.Fatal(err)
			}
			if _, err := l.Allow(ctx, c); !errors.Is(err, pacelock.ErrPaused) {
				t.Fatal("partial update", err)
			}
		})
	}
}

func TestDynamicDebtExpiryDoesNotDeletePolicy(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			domain := freshDomain()
			l := limiter(t, b, 0, domain)
			c := managedCharge()
			q := dynamicQuota(2, 1, 1)
			q.Policy.Period = time.Millisecond
			updateQuota(t, l, q)
			if d, err := l.Allow(ctx, c); err != nil || !d.Allowed {
				t.Fatal(d, err)
			}
			// Test waits for actual debt expiry before exercising physical maintenance.
			<-time.After(20 * time.Millisecond)
			pruneManagedScope(t, b, domain)
			old := q
			old.Policy.Revision = 1
			if err := l.UpdateQuotas(ctx, old); !errors.Is(err, pacelock.ErrPolicyMismatch) {
				t.Fatal("lost fence", err)
			}
			if d, err := l.Allow(ctx, c); err != nil || !d.Allowed {
				t.Fatal("lost config", d, err)
			}
		})
	}
}

func TestDynamicAtomicAdmission(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			l := limiter(t, b, 0, freshDomain())
			q := dynamicQuota(1, 1, 1)
			other := q
			other.Name = "other"
			if err := l.UpdateQuotas(ctx, q, other); err != nil {
				t.Fatal(err)
			}
			c := managedCharge()
			o := pacelock.Charge{Name: other.Name, Cost: 1}
			if d, err := l.Allow(ctx, c); err != nil || !d.Allowed {
				t.Fatal(d, err)
			}
			deny, err := l.Allow(ctx, c, o)
			if err != nil || deny.Allowed {
				t.Fatal(deny, err)
			}
			if d, err := l.Allow(ctx, o); err != nil || !d.Allowed {
				t.Fatal("partial debit", d, err)
			}

		})
	}
}

func TestDynamicConcurrentUpdates(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			domain := freshDomain()
			ls := []*contractLimiter{limiter(t, b, 0, domain), limiter(t, b, 1, domain)}
			c := managedCharge()
			updateQuota(t, ls[0], dynamicQuota(1, 1, 100))
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					err := ls[i%2].UpdateQuotas(ctx, dynamicQuota(int64(i+2), 1, 100))
					if err != nil && !errors.Is(err, pacelock.ErrPolicyMismatch) {
						t.Errorf("update: %v", err)
					}
					if d, err := ls[i%2].Allow(ctx, c); err != nil || !d.Allowed {
						t.Errorf("latest config: %+v %v", d, err)
					}
				}(i)
			}
			wg.Wait()
			if err := ls[1].UpdateQuotas(ctx, dynamicQuota(16, 1, 100)); !errors.Is(err, pacelock.ErrPolicyMismatch) {
				t.Fatal(err)
			}
			updateQuota(t, ls[0], dynamicQuota(17, 1, 100))
			if d, err := ls[0].Allow(ctx, c); err != nil || !d.Allowed {
				t.Fatal(d, err)
			}
		})
	}
}
