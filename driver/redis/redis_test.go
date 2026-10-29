package redisdriver

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUpdateQuotasPublicValidation(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer client.Close()
	driver, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	request := pacelock.QuotaBatch{Scope: pacelock.Scope{Namespace: "test", Domain: "q"}, Quotas: []pacelock.Quota{{Name: "q", Policy: pacelock.Policy{Rate: 1, Period: time.Second, Burst: 1, Revision: 1}}}}
	for _, current := range []*Driver{nil, {}, driver} {
		if err := current.UpdateQuotas(nil, request); !errors.Is(err, pacelock.ErrInvalid) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := driver.UpdateQuotas(ctx, request); !errors.Is(err, context.Canceled) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatal(err)
	}
	request.Quotas[0].Policy.Revision = 0
	if err := driver.UpdateQuotas(context.Background(), request); !errors.Is(err, pacelock.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestAdmissionOperationNames(t *testing.T) {
	request := pacelock.Admission{Scope: pacelock.Scope{Namespace: "test", Domain: "q"}, Charges: []pacelock.Charge{{Name: "q", Cost: 1}}}
	var driver *Driver
	_, err := driver.Admit(context.Background(), request)
	var sdk *pacelock.Error
	if !errors.As(err, &sdk) || sdk.Op != "allow" || sdk.Outcome != pacelock.NotCharged || !errors.Is(err, pacelock.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestPrunePublicValidation(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer client.Close()
	driver, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range []*Driver{nil, {}, driver} {
		if _, err := current.Prune(nil, pacelock.Scope{Namespace: "test", Domain: "q"}, PruneCursor{}, 1); !errors.Is(err, pacelock.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := driver.Prune(context.Background(), pacelock.Scope{}, PruneCursor{}, 1); !errors.Is(err, pacelock.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := driver.Prune(context.Background(), pacelock.Scope{Namespace: "test", Domain: "q"}, PruneCursor{}, MaxPruneKeys+1); !errors.Is(err, pacelock.ErrInvalid) {
		t.Fatal(err)
	}
}

func integration(t *testing.T) (*limiter, *redis.Client) {
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
	l, err := newLimiter(c, "pacelock-test", fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()))
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
	publishRules(t, l, rpm, tokens)
	d, err := l.admitRules(ctx, rpm, tokens)
	if err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	before := c.Get(ctx, l.key("rpm")).Val()
	d, err = l.admitRules(ctx, rpm, tokens)
	if err != nil || d.Allowed || d.RetryAfter <= 0 || len(d.Limited) != 1 || d.Limited[0] != "tokens" {
		t.Fatalf("%+v %v", d, err)
	}
	if c.Get(ctx, l.key("rpm")).Val() != before {
		t.Fatal("denial partially charged RPM")
	}
	tokens.Cost = 1
	if d, err = l.admitRules(ctx, rpm, tokens); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	d, err = l.admitRules(ctx, rpm, tokens)
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
	l2 := &limiter{client: second, prefix: l.prefix}
	publishRules(t, l, rule("shared", 20, 2))
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
			d, err := current.admitRules(context.Background(), rule("shared", 20, 2))
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
	publishRules(t, l, r, rule("fresh", 1, 1))
	if _, err := l.admitRules(ctx, r); err != nil {
		t.Fatal(err)
	}
	before := c.Get(ctx, l.key(r.Name)).Val()
	for _, raw := range []string{"broken", "Q2|1:1:1", "Q2|" + before} {
		t.Run(raw, func(t *testing.T) {
			if err := c.Set(ctx, l.key(r.Name), raw, 0).Err(); err != nil {
				t.Fatal(err)
			}
			if _, err := l.admitRules(ctx, rule("fresh", 1, 1), r); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
				t.Fatalf("err=%v", err)
			}
			if c.Exists(ctx, l.key("fresh")).Val() != 0 || c.Get(ctx, l.key(r.Name)).Val() != raw {
				t.Fatal("state error mutated quota state")
			}

		})
	}
}

func TestRedisRefillAndExpiry(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := Rule{Name: "short", Policy: Policy{Rate: 1, Period: 200 * time.Millisecond, Burst: 1}, Cost: 1}
	publishRules(t, l, r)
	if d, err := l.admitRules(ctx, r); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	if d, err := l.admitRules(ctx, r); err != nil || d.Allowed || d.RetryAfter <= 0 || d.RetryAfter > r.Policy.Period {
		t.Fatalf("%+v %v", d, err)
	}
	time.Sleep(220 * time.Millisecond)
	if c.Exists(ctx, l.key(r.Name)).Val() != 1 {
		t.Fatal("test must retain physically expired logical state")
	}
	r.Policy.Burst = 2
	r.Policy.Revision = 2
	publishRules(t, l, r)
	if d, err := l.admitRules(ctx, r); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}

}

func TestRedisIndependentDomainsAndCanceledCall(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("quota", 1, 1)
	publishRules(t, l, r)
	if _, err := l.admitRules(ctx, r); err != nil {
		t.Fatal(err)
	}
	other, _ := newLimiter(c, "another-app", l.prefix)
	t.Cleanup(func() { c.Del(ctx, other.key(r.Name), other.policyKey(r.Name)) })
	publishRules(t, other, r)
	if d, err := other.admitRules(ctx, r); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.admitRules(canceled, rule("untouched", 1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if c.Exists(ctx, l.key("untouched")).Val() != 0 {
		t.Fatal("pre-canceled call wrote state")
	}
}

// Observation belongs to the public limiter, including errors from the Redis driver.
type observationScripter struct {
	redis.Scripter
	reply interface{}
	err   error
	calls atomic.Int64
}

func (s *observationScripter) EvalSha(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	s.calls.Add(1)
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(s.reply)
	cmd.SetErr(s.err)
	return cmd
}

func TestPublicObserver(t *testing.T) {
	charge := pacelock.Charge{Name: "q", Cost: 1}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name       string
		ctx        context.Context
		charges    []pacelock.Charge
		reply      interface{}
		backendErr error
		wantErr    error
		outcome    pacelock.Outcome
		result     string
		calls      int64
	}{
		{name: "allowed", ctx: context.Background(), charges: []pacelock.Charge{charge}, reply: []interface{}{int64(1), int64(0), int64(0)}, result: "allowed", calls: 1},
		{name: "denied", ctx: context.Background(), charges: []pacelock.Charge{charge}, reply: []interface{}{int64(0), int64(5), int64(1)}, result: "denied", calls: 1},
		{name: "empty", ctx: context.Background(), wantErr: pacelock.ErrInvalid, outcome: pacelock.NotCharged, result: "error"},
		{name: "nil context", charges: []pacelock.Charge{charge}, wantErr: pacelock.ErrInvalid, outcome: pacelock.NotCharged, result: "error"},
		{name: "canceled", ctx: canceled, charges: []pacelock.Charge{charge}, wantErr: context.Canceled, outcome: pacelock.NotCharged, result: "error"},
		{name: "redis nil", ctx: context.Background(), charges: []pacelock.Charge{charge}, backendErr: redis.Nil, wantErr: pacelock.ErrBackend, outcome: pacelock.Unknown, result: "error", calls: 1},
		{name: "lost reply", ctx: context.Background(), charges: []pacelock.Charge{charge}, backendErr: io.ErrUnexpectedEOF, wantErr: pacelock.ErrBackend, outcome: pacelock.Unknown, result: "error", calls: 1},
		{name: "malformed reply", ctx: context.Background(), charges: []pacelock.Charge{charge}, reply: int64(1), wantErr: pacelock.ErrState, outcome: pacelock.Unknown, result: "error", calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &observationScripter{reply: tc.reply, err: tc.backendErr}
			driver, err := New(s)
			if err != nil {
				t.Fatal(err)
			}
			var events []pacelock.Event
			l, err := pacelock.NewWithOptions(driver, "test", "observer", pacelock.Options{Observer: func(ctx context.Context, e pacelock.Event) {
				if ctx == nil {
					t.Error("nil observer context")
				}
				if tc.ctx != nil && ctx != tc.ctx {
					t.Error("observer lost caller context")
				}
				events = append(events, e)
			}})
			if err != nil {
				t.Fatal(err)
			}
			d, err := l.Allow(tc.ctx, tc.charges...)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
			if err != nil && pacelock.OutcomeOf(err) != tc.outcome {
				t.Fatalf("outcome: %v", err)
			}
			if tc.backendErr != nil && !errors.Is(err, tc.backendErr) {
				t.Fatalf("lost backend error: %v", err)
			}
			if s.calls.Load() != tc.calls {
				t.Fatalf("dispatches=%d want %d", s.calls.Load(), tc.calls)
			}
			if len(events) != 1 {
				t.Fatalf("events=%+v", events)
			}
			e := events[0]
			if e.Operation != "allow" || e.Result != tc.result || e.RuleCount != len(tc.charges) || e.Duration < 0 || e.Err != err || e.RetryAfter != d.RetryAfter {
				t.Fatalf("event=%+v decision=%+v err=%v", e, d, err)
			}
			if err == nil && d.Allowed != (tc.result == "allowed") {
				t.Fatal(d)
			}
		})
	}
}

func TestPublicObserverConcurrentCancellation(t *testing.T) {
	s := &observationScripter{reply: []interface{}{int64(1), int64(0), int64(0)}}
	driver, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	events := make(map[string]int)
	l, err := pacelock.NewWithOptions(driver, "test", "observer", pacelock.Options{Observer: func(ctx context.Context, e pacelock.Event) {
		mu.Lock()
		defer mu.Unlock()
		events[e.Operation+"/"+e.Result]++
		if ctx == nil || e.Duration < 0 {
			t.Error("invalid observation")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 600; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			charges := []pacelock.Charge{{Name: "q", Cost: 1}}
			var want error
			switch i % 6 {
			case 1, 5:
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx, want = canceled, context.Canceled
			case 2:
				expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				ctx, want = expired, context.DeadlineExceeded
			case 3:
				charges = append(charges, charges[0])
				want = pacelock.ErrInvalid
			}
			d, err := l.Allow(ctx, charges...)
			if !errors.Is(err, want) || (err != nil && pacelock.OutcomeOf(err) != pacelock.NotCharged) || (err == nil && !d.Allowed) {
				t.Errorf("decision=%+v err=%v want=%v", d, err, want)
			}
		}(i)
	}
	wg.Wait()
	if len(events) != 2 || events["allow/allowed"] != 200 || events["allow/error"] != 400 || s.calls.Load() != 200 {
		t.Fatalf("events=%v dispatches=%d", events, s.calls.Load())
	}
}

func BenchmarkValidate(b *testing.B) {
	for _, n := range []int{1, 4, 32} {
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			rules := make([]Rule, n)
			for i := range rules {
				rules[i] = Rule{Name: fmt.Sprint(i), Policy: Policy{Rate: 1000, Period: time.Second, Burst: 1000}, Cost: 1}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := pacelock.Validate(rules...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRedis(b *testing.B) {
	addr := os.Getenv("PACELOCK_REDIS_ADDR")
	if addr == "" {
		b.Skip("set PACELOCK_REDIS_ADDR")
	}
	for _, n := range []int{1, 4, 32} {
		for _, mode := range []string{"allow", "deny"} {
			b.Run(fmt.Sprintf("%s/rules=%d", mode, n), func(b *testing.B) {
				c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
				defer c.Close()
				l, _ := newLimiter(c, "pacelock-bench", fmt.Sprintf("%s-%d", b.Name(), time.Now().UnixNano()))
				ctx := context.Background()
				b.Cleanup(func() {
					clean := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
					defer clean.Close()
					it := clean.Scan(ctx, 0, l.prefix+"*", 1000).Iterator()
					for it.Next(ctx) {
						clean.Del(ctx, it.Val())
					}
					if err := it.Err(); err != nil {
						b.Error(err)
					}
				})
				rules := make([]Rule, n)
				for i := range rules {
					rules[i] = Rule{Name: fmt.Sprint(i), Policy: Policy{Rate: 1000000, Period: time.Second, Burst: 1000000000}, Cost: 1}
					if mode == "deny" {
						rules[i] = rule(fmt.Sprint(i), 1, 1)
					}
				}
				publishRules(b, l, rules...)
				if mode == "deny" {
					if _, err := l.admitRules(ctx, rules...); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					d, err := l.admitRules(ctx, rules...)
					if err != nil || (mode == "allow" && !d.Allowed) || (mode == "deny" && d.Allowed) {
						b.Fatalf("%+v %v", d, err)
					}
				}
				b.StopTimer()
			})
		}
	}
}

// BenchmarkPruneCandidates measures candidate recheck/deletion without discovery.
// Expired-state setup is outside timing; it is not a SCAN or end-to-end cleanup SLO.
func BenchmarkPruneCandidates(b *testing.B) {
	address := os.Getenv("PACELOCK_REDIS_ADDR")
	if address == "" {
		b.Skip("set PACELOCK_REDIS_ADDR")
	}
	for _, n := range []int{1, 32, MaxPruneKeys} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			c := redis.NewClient(&redis.Options{Addr: address, ContextTimeoutEnabled: true})
			defer c.Close()
			l, err := newLimiter(c, "pacelock-prune-bench", fmt.Sprint(time.Now().UnixNano()))
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			keys := make([]string, n)
			pairs := make([]interface{}, 0, 2*n)
			for i := range keys {
				keys[i] = l.key(fmt.Sprint(i))
				pairs = append(pairs, keys[i], "D1|1")
			}
			defer c.Del(ctx, keys...)
			cursor := PruneCursor{scope: l.prefix, position: "0", pending: keys}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if err := c.MSet(ctx, pairs...).Err(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				r, err := l.Prune(ctx, cursor, n)
				if err != nil || r.Deleted != int64(n) {
					b.Fatalf("%+v %v", r, err)
				}
			}
			b.ReportMetric(float64(n), "candidates/op")
		})
	}
}
