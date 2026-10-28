package contracts_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	"github.com/tr1v3r/pacelock/driver/postgres"
	redisdriver "github.com/tr1v3r/pacelock/driver/redis"
)

type backend struct {
	name   string
	driver [2]pacelock.Driver
}

func backends(t *testing.T) []backend {
	t.Helper()
	var result []backend
	required := "," + os.Getenv("PACELOCK_REQUIRED_DRIVER_BACKENDS") + ","
	for _, name := range []string{"redis", "postgres"} {
		env := "PACELOCK_REDIS_ADDR"
		if name == "postgres" {
			env = "PACELOCK_POSTGRES_DSN"
		}
		address := os.Getenv(env)
		if address == "" {
			if strings.Contains(required, ","+name+",") {
				t.Fatalf("required %s backend is unconfigured", name)
			}
			continue
		}
		b := backend{name: name}
		for i := range b.driver {
			if name == "redis" {
				client := redis.NewClient(&redis.Options{Addr: address, ContextTimeoutEnabled: true})
				t.Cleanup(func() { client.Close() })
				d, err := redisdriver.New(client)
				if err != nil {
					t.Fatal(err)
				}
				b.driver[i] = d
			} else {
				db, err := sql.Open("pgx", address)
				if err != nil {
					t.Fatal(err)
				}
				db.SetMaxOpenConns(8)
				db.SetMaxIdleConns(8)
				t.Cleanup(func() { db.Close() })
				d, err := postgres.New(db)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err = d.Migrate(ctx)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				b.driver[i] = d
			}
		}
		result = append(result, b)
	}
	if len(result) == 0 {
		t.Skip("set PACELOCK_REDIS_ADDR and/or PACELOCK_POSTGRES_DSN")
	}
	return result
}

var sequence atomic.Int64

func limiter(t *testing.T, b backend, index int, domain string) *pacelock.Limiter {
	t.Helper()
	l, err := pacelock.New(b.driver[index], "contract-"+domain, domain)
	if err != nil {
		t.Fatal(err)
	}
	return l
}
func freshDomain() string { return fmt.Sprintf("%d-%d", time.Now().UnixNano(), sequence.Add(1)) }
func rule(name string, burst, cost int64) pacelock.Rule {
	return pacelock.Rule{Name: name, Policy: pacelock.Policy{Rate: 1, Period: time.Minute, Burst: burst}, Cost: cost}
}
func mustAllow(t *testing.T, l *pacelock.Limiter, rules ...pacelock.Rule) {
	t.Helper()
	d, err := l.AllowStatic(context.Background(), rules...)
	if err != nil || !d.Allowed {
		t.Fatalf("allow: %+v %v", d, err)
	}
}

func TestDriverContract(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			t.Run("joint denial is uncharged", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				mustAllow(t, l, rule("blocked", 1, 1))
				d, err := l.AllowStatic(context.Background(), rule("fresh", 5, 2), rule("blocked", 1, 1))
				if err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"blocked"}) || d.RetryAfter <= 0 {
					t.Fatal(d, err)
				}
				mustAllow(t, l, rule("fresh", 5, 5))
			})
			t.Run("multiple blockers and maximum retry", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				slow := rule("slow", 1, 1)
				fast := slow
				fast.Name = "fast"
				fast.Policy.Period = 10 * time.Second
				mustAllow(t, l, slow, fast)
				d, err := l.AllowStatic(context.Background(), fast, slow)
				if err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"fast", "slow"}) || d.RetryAfter < 50*time.Second || d.RetryAfter > time.Minute {
					t.Fatal(d, err)
				}
			})
			t.Run("scope isolation", func(t *testing.T) {
				domain := freshDomain()
				a := limiter(t, b, 0, domain)
				mustAllow(t, a, rule("q", 1, 1))
				other, _ := pacelock.New(b.driver[1], "other-app", domain)
				mustAllow(t, other, rule("q", 1, 1))
				mustAllow(t, limiter(t, b, 1, domain+"-other"), rule("q", 1, 1))
			})
			t.Run("policy conflict is uncharged", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				mustAllow(t, l, rule("q", 5, 1))
				changed := rule("q", 6, 1)
				if _, err := l.AllowStatic(context.Background(), rule("fresh", 1, 1), changed); !errors.Is(err, pacelock.ErrPolicyMismatch) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
				mustAllow(t, l, rule("fresh", 1, 1))
			})
			t.Run("weighted cross client contention", func(t *testing.T) {
				domain := freshDomain()
				limiters := []*pacelock.Limiter{limiter(t, b, 0, domain), limiter(t, b, 1, domain)}
				var allowed atomic.Int64
				var wg sync.WaitGroup
				start := make(chan struct{})
				for i := range 128 {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-start
						ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
						defer cancel()
						d, err := limiters[i%2].AllowStatic(ctx, rule("q", 31, 3))
						if err != nil {
							t.Error(err)
						} else if d.Allowed {
							allowed.Add(1)
						}
					}(i)
				}
				close(start)
				wg.Wait()
				if allowed.Load() != 10 {
					t.Fatal("allowed", allowed.Load())
				}
				mustAllow(t, limiters[0], rule("q", 31, 1))
			})
			t.Run("retained same ID contention and conflict", func(t *testing.T) {
				domain := freshDomain()
				ls := []*pacelock.Limiter{limiter(t, b, 0, domain), limiter(t, b, 1, domain)}
				var first atomic.Int64
				var wg sync.WaitGroup
				for i := range 64 {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						r, err := ls[i%2].AllowStaticOnce(context.Background(), "same", time.Minute, rule("q", 1, 1))
						if err != nil || !r.Allowed {
							t.Errorf("%+v %v", r, err)
						} else if !r.Replayed {
							first.Add(1)
						}
					}(i)
				}
				wg.Wait()
				if first.Load() != 1 {
					t.Fatal("first", first.Load())
				}
				d, found, err := ls[1].Lookup(context.Background(), "same")
				if err != nil || !found || !d.Allowed {
					t.Fatal(d, found, err)
				}
				if _, err := ls[1].AllowStaticOnce(context.Background(), "same", 2*time.Minute, rule("q", 1, 1)); !errors.Is(err, pacelock.ErrConflict) {
					t.Fatal(err)
				}
				d, err = ls[0].AllowStatic(context.Background(), rule("q", 1, 1))
				if err != nil || d.Allowed {
					t.Fatal(d, err)
				}
			})
			t.Run("denial replay preserves original wait", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				mustAllow(t, l, rule("q", 1, 1))
				first, err := l.AllowStaticOnce(context.Background(), "denied", time.Minute, rule("q", 1, 1))
				if err != nil || first.Allowed {
					t.Fatal(first, err)
				}
				again, err := l.AllowStaticOnce(context.Background(), "denied", time.Minute, rule("q", 1, 1))
				if err != nil || !again.Replayed || !reflect.DeepEqual(first.Decision, again.Decision) {
					t.Fatal(first, again, err)
				}
				d, found, err := l.Lookup(context.Background(), "denied")
				if err != nil || !found || !reflect.DeepEqual(d, first.Decision) {
					t.Fatal(d, found, err)
				}
			})
			t.Run("ordered and equivalent policy fingerprints", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				rules := []pacelock.Rule{rule("a", 3, 1), rule("b", 3, 1)}
				if _, err := l.AllowStaticOnce(context.Background(), "id", time.Minute, rules...); err != nil {
					t.Fatal(err)
				}
				rules[0].Policy.Rate = 60
				rules[0].Policy.Period = time.Hour
				if r, err := l.AllowStaticOnce(context.Background(), "id", time.Minute, rules...); err != nil || !r.Replayed {
					t.Fatal(r, err)
				}
				rules[0], rules[1] = rules[1], rules[0]
				if _, err := l.AllowStaticOnce(context.Background(), "id", time.Minute, rules...); !errors.Is(err, pacelock.ErrConflict) {
					t.Fatal(err)
				}
			})
			t.Run("expiry recovery and no retention extension", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				r := rule("q", 1, 1)
				r.Policy.Period = 20 * time.Millisecond
				if _, err := l.AllowStaticOnce(context.Background(), "short", time.Second, r); err != nil {
					t.Fatal(err)
				}
				time.Sleep(600 * time.Millisecond)
				replay, err := l.AllowStaticOnce(context.Background(), "short", time.Second, r)
				if err != nil || !replay.Replayed {
					t.Fatal(replay, err)
				}
				time.Sleep(500 * time.Millisecond)
				if _, found, err := l.Lookup(context.Background(), "short"); err != nil || found {
					t.Fatal(found, err)
				}
				r.Policy.Burst = 2
				mustAllow(t, l, r)
			})
			t.Run("32 dimensions and cancellation", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				rules := make([]pacelock.Rule, pacelock.MaxRules)
				for i := range rules {
					rules[i] = rule(fmt.Sprint(i), 1, 1)
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := l.AllowStatic(ctx, rules...); !errors.Is(err, context.Canceled) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
				mustAllow(t, l, rules...)
			})
		})
	}
}

// cancelOnDenialDriver cancels only after a real backend confirms no debit.
// This keeps cancellation between attempts even on slow race-test hosts.
type cancelOnDenialDriver struct {
	pacelock.Driver
	cancel context.CancelFunc
}

func (d cancelOnDenialDriver) AdmitStatic(ctx context.Context, a pacelock.StaticAdmission) (pacelock.Receipt, error) {
	r, err := d.Driver.(pacelock.StaticDriver).AdmitStatic(ctx, a)
	if err == nil && !r.Allowed {
		d.cancel()
	}
	return r, err
}

// TestWaitDriverContract exercises the shared wrapper through real backends.
func TestWaitDriverContract(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			t.Run("weighted recovery charges all quotas once", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				slow := rule("slow", 2, 2)
				slow.Policy.Period = 50 * time.Millisecond
				mustAllow(t, l, slow)
				fresh := rule("fresh", 5, 3)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := l.WaitStatic(ctx, fresh, slow); err != nil {
					t.Fatal(err)
				}
				fresh.Cost = 2
				mustAllow(t, l, fresh)
				fresh.Cost = 1
				decision, err := l.AllowStatic(ctx, fresh)
				if err != nil || decision.Allowed {
					t.Fatal("wait must charge fresh quota exactly once", decision, err)
				}
			})
			t.Run("cancellation leaves every quota uncharged", func(t *testing.T) {
				domain := freshDomain()
				first := limiter(t, b, 0, domain)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				second, err := pacelock.New(cancelOnDenialDriver{b.driver[1], cancel}, "contract-"+domain, domain)
				if err != nil {
					t.Fatal(err)
				}
				blocked := rule("blocked", 1, 1)
				mustAllow(t, first, blocked)
				fresh := rule("fresh", 5, 3)
				if err := second.WaitStatic(ctx, fresh, blocked); !errors.Is(err, context.Canceled) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
				fresh.Cost = fresh.Policy.Burst
				mustAllow(t, first, fresh)
			})
			t.Run("concurrent cross client waits", func(t *testing.T) {
				domain := freshDomain()
				ls := []*pacelock.Limiter{limiter(t, b, 0, domain), limiter(t, b, 1, domain)}
				r := rule("weighted", 2, 2)
				r.Policy.Period = 10 * time.Millisecond
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				const callers = 12
				errs := make(chan error, callers)
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range callers {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-start
						errs <- ls[i%len(ls)].WaitStatic(ctx, r)
					}(i)
				}
				close(start)
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		})
	}
}
