package contracts_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	redisdriver "github.com/tr1v3r/pacelock/drivers/redis"
)

// cancelWaitOnceOnDenialDriver cancels only after a confirmed atomic denial.
type cancelWaitOnceOnDenialDriver struct {
	pacelock.Driver
	cancel context.CancelFunc
}

func (d cancelWaitOnceOnDenialDriver) Admit(ctx context.Context, a pacelock.Admission) (pacelock.Receipt, error) {
	r, err := d.Driver.Admit(ctx, a)
	if err == nil && !r.Allowed {
		d.cancel()
	}
	return r, err
}

func TestWaitOnceContract(t *testing.T) {
	bs := backends(t)
	if addresses := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS"); addresses != "" {
		b := backend{name: "redis-cluster"}
		for i := range b.driver {
			client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ","), ContextTimeoutEnabled: true})
			t.Cleanup(func() { client.Close() })
			d, err := redisdriver.New(client)
			if err != nil {
				t.Fatal(err)
			}
			b.driver[i] = d
		}
		bs = append(bs, b)
	}
	for _, b := range bs {
		t.Run(b.name, func(t *testing.T) {
			t.Run("denial creates no record or binding", func(t *testing.T) {
				domain := freshDomain()
				l := limiter(t, b, 0, domain)
				r := rule("q", 1, 1)
				mustAllow(t, l, r)
				a := pacelock.Admission{Scope: pacelock.Scope{Namespace: "contract-" + domain, Domain: domain}, ID: "id", Retention: time.Minute, SuccessOnly: true, Rules: []pacelock.Rule{r}}
				d := b.driver[1]
				for range 2 {
					receipt, err := d.Admit(context.Background(), a)
					if err != nil || receipt.Allowed || receipt.Replayed || receipt.RetryAfter <= 0 {
						t.Fatal(receipt, err)
					}
					if _, found, err := l.Lookup(context.Background(), "id"); err != nil || found {
						t.Fatal("denial retained a record", found, err)
					}
				}
				// Without a pending/success record, parameters have not become bound.
				a.Rules = []pacelock.Rule{rule("fresh", 2, 1)}
				a.Retention = 2 * time.Minute
				if receipt, err := d.Admit(context.Background(), a); err != nil || !receipt.Allowed || receipt.Replayed {
					t.Fatal(receipt, err)
				}
				a.ID = ""
				a.Retention = 0
				if _, err := d.Admit(context.Background(), a); !errors.Is(err, pacelock.ErrInvalid) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
			})
			t.Run("weighted recovery and replay across APIs", func(t *testing.T) {
				domain := freshDomain()
				l := limiter(t, b, 0, domain)
				other := limiter(t, b, 1, domain)
				slow := rule("slow", 2, 2)
				slow.Period = 50 * time.Millisecond
				mustAllow(t, l, slow)
				fresh := rule("fresh", 5, 3)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				receipt, err := l.WaitOnce(ctx, "waited", time.Minute, fresh, slow)
				if err != nil || !receipt.Allowed || receipt.Replayed {
					t.Fatal(receipt, err)
				}
				if decision, found, err := other.Lookup(ctx, "waited"); err != nil || !found || !decision.Allowed {
					t.Fatal(decision, found, err)
				}
				for _, admit := range []func(context.Context, string, time.Duration, ...pacelock.Rule) (pacelock.Receipt, error){other.WaitOnce, other.AllowOnce} {
					if replay, err := admit(ctx, "waited", time.Minute, fresh, slow); err != nil || !replay.Allowed || !replay.Replayed {
						t.Fatal(replay, err)
					}
				}
				if _, err := other.WaitOnce(ctx, "waited", 2*time.Minute, fresh, slow); !errors.Is(err, pacelock.ErrConflict) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
				if _, err := other.WaitOnce(ctx, "waited", time.Minute, slow, fresh); !errors.Is(err, pacelock.ErrConflict) {
					t.Fatal(err)
				}
				fresh.Cost = 2
				mustAllow(t, l, fresh)
				fresh.Cost = 1
				if decision, err := l.Allow(ctx, fresh); err != nil || decision.Allowed {
					t.Fatal("duplicate debit or missing debit", decision, err)
				}
			})
			t.Run("AllowOnce decisions stay immutable", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				r := rule("q", 1, 1)
				if receipt, err := l.AllowOnce(context.Background(), "allowed", time.Minute, r); err != nil || !receipt.Allowed {
					t.Fatal(receipt, err)
				}
				if receipt, err := l.WaitOnce(context.Background(), "allowed", time.Minute, r); err != nil || !receipt.Replayed || !receipt.Allowed {
					t.Fatal(receipt, err)
				}
				if receipt, err := l.AllowOnce(context.Background(), "denied", time.Minute, r); err != nil || receipt.Allowed {
					t.Fatal(receipt, err)
				}
				if _, err := l.WaitOnce(context.Background(), "denied", time.Minute, r); !errors.Is(err, pacelock.ErrConflict) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
				if receipt, err := l.AllowOnce(context.Background(), "denied", time.Minute, r); err != nil || receipt.Allowed || !receipt.Replayed {
					t.Fatal("retained denial changed", receipt, err)
				}
			})
			t.Run("cancellation leaves no receipt or debit", func(t *testing.T) {
				domain := freshDomain()
				l := limiter(t, b, 0, domain)
				r := rule("blocked", 1, 1)
				mustAllow(t, l, r)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				d := cancelWaitOnceOnDenialDriver{b.driver[1], cancel}
				waiting, err := pacelock.New(d, "contract-"+domain, domain)
				if err != nil {
					t.Fatal(err)
				}
				fresh := rule("fresh", 5, 3)
				if _, err := waiting.WaitOnce(ctx, "canceled", time.Minute, fresh, r); !errors.Is(err, context.Canceled) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
				if _, found, err := l.Lookup(context.Background(), "canceled"); err != nil || found {
					t.Fatal(found, err)
				}
				fresh.Cost = fresh.Burst
				mustAllow(t, l, fresh)
			})
			t.Run("same ID concurrent waiters debit once", func(t *testing.T) {
				domain := freshDomain()
				ls := []*pacelock.Limiter{limiter(t, b, 0, domain), limiter(t, b, 1, domain)}
				slow := rule("slow", 1, 1)
				slow.Period = 30 * time.Millisecond
				mustAllow(t, ls[0], slow)
				fresh := rule("fresh", 2, 1)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var first atomic.Int64
				var wg sync.WaitGroup
				start := make(chan struct{})
				for i := range 32 {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-start
						r, err := ls[i%len(ls)].WaitOnce(ctx, "shared", time.Minute, fresh, slow)
						if err != nil || !r.Allowed {
							t.Errorf("wait: %+v %v", r, err)
						} else if !r.Replayed {
							first.Add(1)
						}
					}(i)
				}
				close(start)
				wg.Wait()
				if first.Load() != 1 {
					t.Fatal("new admissions", first.Load())
				}
				mustAllow(t, ls[0], fresh)
				if decision, err := ls[0].Allow(ctx, fresh); err != nil || decision.Allowed {
					t.Fatal(decision, err)
				}
			})
		})
	}
}
