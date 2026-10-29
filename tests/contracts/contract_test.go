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

// contractLimiter preserves a dynamic scope while low-level contracts vary fixed
// policy sets between calls. Each static call uses the public constructor; main
// StaticLimiter contracts below retain one instance for repeated admissions.
type contractLimiter struct {
	*pacelock.Limiter
	driver pacelock.Driver
	scope  pacelock.Scope
}

func newContractLimiter(driver pacelock.Driver, namespace, domain string) (*contractLimiter, error) {
	l, err := pacelock.New(driver, namespace, domain)
	if err != nil {
		return nil, err
	}
	return &contractLimiter{Limiter: l, driver: driver, scope: pacelock.Scope{Namespace: namespace, Domain: domain}}, nil
}

func (l *contractLimiter) staticRules(rules ...pacelock.Rule) (*pacelock.StaticLimiter, []pacelock.Charge, error) {
	quotas := make([]pacelock.Quota, len(rules))
	charges := make([]pacelock.Charge, len(rules))
	for i, r := range rules {
		quotas[i] = pacelock.Quota{Name: r.Name, Policy: r.Policy}
		charges[i] = pacelock.Charge{Name: r.Name, Cost: r.Cost}
	}
	static, err := pacelock.NewStatic(l.driver, l.scope.Namespace, l.scope.Domain, quotas...)
	return static, charges, err
}
func (l *contractLimiter) allowRules(ctx context.Context, rules ...pacelock.Rule) (pacelock.Decision, error) {
	static, charges, err := l.staticRules(rules...)
	if err != nil {
		return pacelock.Decision{}, err
	}
	return static.Allow(ctx, charges...)
}
func (l *contractLimiter) waitRules(ctx context.Context, rules ...pacelock.Rule) error {
	static, charges, err := l.staticRules(rules...)
	if err != nil {
		return err
	}
	return static.Wait(ctx, charges...)
}

func limiter(t *testing.T, b backend, index int, domain string) *contractLimiter {
	t.Helper()
	l, err := newContractLimiter(b.driver[index], "contract-"+domain, domain)
	if err != nil {
		t.Fatal(err)
	}
	return l
}
func freshDomain() string { return fmt.Sprintf("%d-%d", time.Now().UnixNano(), sequence.Add(1)) }
func rule(name string, burst, cost int64) pacelock.Rule {
	return pacelock.Rule{Name: name, Policy: pacelock.Policy{Rate: 1, Period: time.Minute, Burst: burst}, Cost: cost}
}
func mustAllow(t *testing.T, l *contractLimiter, rules ...pacelock.Rule) {
	t.Helper()
	d, err := l.allowRules(context.Background(), rules...)
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
				d, err := l.allowRules(context.Background(), rule("fresh", 5, 2), rule("blocked", 1, 1))
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
				d, err := l.allowRules(context.Background(), fast, slow)
				if err != nil || d.Allowed || !reflect.DeepEqual(d.Limited, []string{"fast", "slow"}) || d.RetryAfter < 50*time.Second || d.RetryAfter > time.Minute {
					t.Fatal(d, err)
				}
			})
			t.Run("scope isolation", func(t *testing.T) {
				domain := freshDomain()
				a := limiter(t, b, 0, domain)
				mustAllow(t, a, rule("q", 1, 1))
				other, _ := newContractLimiter(b.driver[1], "other-app", domain)
				mustAllow(t, other, rule("q", 1, 1))
				mustAllow(t, limiter(t, b, 1, domain+"-other"), rule("q", 1, 1))
			})
			t.Run("policy conflict is uncharged", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				mustAllow(t, l, rule("q", 5, 1))
				changed := rule("q", 6, 1)
				if _, err := l.allowRules(context.Background(), rule("fresh", 1, 1), changed); !errors.Is(err, pacelock.ErrPolicyMismatch) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
				mustAllow(t, l, rule("fresh", 1, 1))
			})
			t.Run("weighted cross client contention", func(t *testing.T) {
				domain := freshDomain()
				limiters := []*contractLimiter{limiter(t, b, 0, domain), limiter(t, b, 1, domain)}
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
						d, err := limiters[i%2].allowRules(ctx, rule("q", 31, 3))
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
			t.Run("32 dimensions and cancellation", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				rules := make([]pacelock.Rule, pacelock.MaxRules)
				for i := range rules {
					rules[i] = rule(fmt.Sprint(i), 1, 1)
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := l.allowRules(ctx, rules...); !errors.Is(err, context.Canceled) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
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

func (d cancelOnDenialDriver) AdmitStatic(ctx context.Context, a pacelock.StaticAdmission) (pacelock.Decision, error) {
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
				if err := l.waitRules(ctx, fresh, slow); err != nil {
					t.Fatal(err)
				}
				fresh.Cost = 2
				mustAllow(t, l, fresh)
				fresh.Cost = 1
				decision, err := l.allowRules(ctx, fresh)
				if err != nil || decision.Allowed {
					t.Fatal("wait must charge fresh quota exactly once", decision, err)
				}
			})
			t.Run("cancellation leaves every quota uncharged", func(t *testing.T) {
				domain := freshDomain()
				first := limiter(t, b, 0, domain)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				second, err := newContractLimiter(cancelOnDenialDriver{b.driver[1], cancel}, "contract-"+domain, domain)
				if err != nil {
					t.Fatal(err)
				}
				blocked := rule("blocked", 1, 1)
				mustAllow(t, first, blocked)
				fresh := rule("fresh", 5, 3)
				if err := second.waitRules(ctx, fresh, blocked); !errors.Is(err, context.Canceled) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
				fresh.Cost = fresh.Policy.Burst
				mustAllow(t, first, fresh)
			})
			t.Run("concurrent cross client waits", func(t *testing.T) {
				domain := freshDomain()
				ls := []*contractLimiter{limiter(t, b, 0, domain), limiter(t, b, 1, domain)}
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
						errs <- ls[i%len(ls)].waitRules(ctx, r)
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
