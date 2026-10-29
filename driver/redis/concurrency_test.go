package redisdriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These are bounded contention/correctness tests, not throughput or latency SLOs.
// A minute per unit prevents refill during each 20-second synchronized wave.
func contentionRule(name string, burst, cost int64) Rule {
	return Rule{Name: name, Policy: Policy{Rate: 1, Period: time.Minute, Burst: burst}, Cost: cost}
}

type contentionFixture struct {
	clients []redis.UniversalClient
	cluster bool
}

func contentionClient(cluster bool, pool int) redis.UniversalClient {
	if cluster {
		return redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS"), ","), MaxRetries: -1, MaxRedirects: -1, ContextTimeoutEnabled: true, PoolSize: pool, MaxActiveConns: pool, PoolTimeout: 15 * time.Second})
	}
	return redis.NewClient(&redis.Options{Addr: os.Getenv("PACELOCK_REDIS_ADDR"), MaxRetries: -1, ContextTimeoutEnabled: true, PoolSize: pool, MaxActiveConns: pool, PoolTimeout: 15 * time.Second})
}

func newContentionFixture(t *testing.T, cluster bool) *contentionFixture {
	t.Helper()
	env := "PACELOCK_REDIS_ADDR"
	if cluster {
		env = "PACELOCK_REDIS_CLUSTER_ADDRS"
	}
	if os.Getenv(env) == "" {
		backend := "standalone"
		if cluster {
			backend = "cluster"
		}
		for _, required := range strings.Split(os.Getenv("PACELOCK_REQUIRED_REDIS_BACKENDS"), ",") {
			if required == backend {
				t.Fatalf("required %s fixture missing: set %s", backend, env)
			}
		}
		t.Skip("set " + env + " for real Redis contention tests")
	}
	f := &contentionFixture{cluster: cluster}
	for i := 0; i < 4; i++ {
		c := contentionClient(cluster, 8)
		t.Cleanup(func() {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := c.Ping(ctx).Err()
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		f.clients = append(f.clients, c)
	}
	return f
}

var contentionSequence atomic.Int64

func (f *contentionFixture) limiter(t *testing.T) *limiter {
	t.Helper()
	l, err := newLimiter(f.clients[0], "pacelock-contention", fmt.Sprintf("%d-%d-%d", os.Getpid(), time.Now().UnixNano(), contentionSequence.Add(1)))
	if err != nil {
		t.Fatal(err)
	}
	f.cleanup(t, l)
	return l
}

func (f *contentionFixture) cleanup(t *testing.T, l *limiter) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		clean := func(ctx context.Context, c redis.Cmdable) error {
			it := c.Scan(ctx, 0, l.prefix+"*", 100).Iterator()
			var keys []string
			for it.Next(ctx) {
				keys = append(keys, it.Val())
			}
			if err := it.Err(); err != nil {
				return err
			}
			// One key per DEL also works across Cluster slots.
			for _, key := range keys {
				if err := c.Del(ctx, key).Err(); err != nil {
					return err
				}
			}
			return nil
		}
		var err error
		if c, ok := f.clients[0].(*redis.ClusterClient); ok {
			err = c.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error { return clean(ctx, c) })
		} else {
			err = clean(ctx, f.clients[0])
		}
		if err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
}

func (f *contentionFixture) caller(l *limiter, i int) *limiter {
	return &limiter{client: f.clients[i%len(f.clients)], prefix: l.prefix}
}

func contentionWave(t *testing.T, n int, call func(context.Context, int)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(n)
	done.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) { defer done.Done(); ready.Done(); <-start; call(ctx, i) }(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
	if err := ctx.Err(); err != nil {
		t.Fatalf("contention wave exceeded deadline: %v", err)
	}
}

func requireAllowed(t *testing.T, l *limiter, rules ...Rule) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if d, err := l.admitRules(ctx, rules...); err != nil || !d.Allowed {
		t.Fatalf("want allowed: %+v %v", d, err)
	}
}

func requireDenied(t *testing.T, l *limiter, rules ...Rule) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if d, err := l.admitRules(ctx, rules...); err != nil || d.Allowed || d.RetryAfter <= 0 || len(d.Limited) == 0 {
		t.Fatalf("want denied: %+v %v", d, err)
	}
}

func quotaState(t *testing.T, f *contentionFixture, l *limiter, name string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := f.clients[0].Get(ctx, l.key(name)).Result()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRedisConcurrency(t *testing.T) {
	for _, backend := range []struct {
		name    string
		cluster bool
	}{{"standalone", false}, {"cluster", true}} {
		t.Run(backend.name, func(t *testing.T) {
			f := newContentionFixture(t, backend.cluster)
			seed := time.Now().UnixNano()
			if value := os.Getenv("PACELOCK_CONCURRENCY_SEED"); value != "" {
				var err error
				seed, err = strconv.ParseInt(value, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("scenario order seed: %d (reproduce with PACELOCK_CONCURRENCY_SEED)", seed)
			scenarios := []struct {
				name string
				run  func(*testing.T, *contentionFixture)
			}{
				{"burst", testContentionBurst},
				{"mixed_costs", testContentionCosts},
				{"repeated_workers", testContentionWorkers},
				{"overlapping_quotas", testContentionOverlapping},
				{"isolation", testContentionIsolation},
				{"policy_updates", testContentionPolicyUpdates},
				{"invalid_and_canceled", testContentionCancellation},
				{"malformed_quotas", testContentionBadState},
				{"lost_responses", testContentionLostResponses},
				{"small_pool", testContentionSmallPool},
				{"pool_exhaustion", testContentionPoolExhaustion},
				{"cold_script_cache", testContentionColdScripts},
				{"refill_waves", testContentionRefill},
				{"processes", testContentionProcesses},
			}
			rand.New(rand.NewSource(seed)).Shuffle(len(scenarios), func(i, j int) { scenarios[i], scenarios[j] = scenarios[j], scenarios[i] })
			for _, scenario := range scenarios {
				t.Run(scenario.name, func(t *testing.T) { scenario.run(t, f) })
			}

		})
	}
}

func testContentionBurst(t *testing.T, f *contentionFixture) {
	for _, n := range []int{100, 500, 1000} {
		for _, dimensions := range []int{1, 4, MaxRules} {
			t.Run(fmt.Sprintf("callers_%d/rules_%d", n, dimensions), func(t *testing.T) {
				l := f.limiter(t)
				rules := make([]Rule, dimensions)
				for j := range rules {
					cost := int64(j%3 + 1)
					rules[j] = contentionRule(fmt.Sprintf("q%d", j), int64(37+j%3)*cost, cost)
				}
				publishRules(t, l, rules...)
				var admitted atomic.Int64
				contentionWave(t, n, func(ctx context.Context, i int) {
					// Different argument orders must not affect atomic admission.
					r := append([]Rule(nil), rules...)
					if i%2 == 0 {
						for a, b := 0, len(r)-1; a < b; a, b = a+1, b-1 {
							r[a], r[b] = r[b], r[a]
						}
					}
					d, err := f.caller(l, i).admitRules(ctx, r...)
					if err != nil {
						t.Error(err)
						return
					}
					if d.Allowed {
						admitted.Add(1)
						return
					}
					var want []string
					for j := range r {
						if r[j].Policy.Burst/r[j].Cost == 37 {
							want = append(want, r[j].Name)
						}
					}
					if d.RetryAfter <= 0 || !reflect.DeepEqual(d.Limited, want) {
						t.Errorf("denied decision: %+v want limited %v", d, want)
					}
				})
				if admitted.Load() != 37 {
					t.Fatalf("admitted %d, want 37", admitted.Load())
				}
				// Fill exactly the unused capacity of non-bottleneck dimensions. Any
				// partial debit by rejected requests makes these checks fail.
				for j, r := range rules {
					remaining := int64(j%3) * r.Cost
					if remaining > 0 {
						r.Cost = remaining
						requireAllowed(t, l, r)
					}
					r.Cost = 1
					requireDenied(t, l, r)
				}
			})
		}
	}
}

func testContentionCosts(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	r := contentionRule("units", 1000, 1)
	publishRules(t, l, r)
	requireAllowed(t, l, r) // Establish a common TAT before competing weighted calls.
	var charged atomic.Int64
	charged.Store(1)
	contentionWave(t, 500, func(ctx context.Context, i int) {
		r := r
		r.Cost = int64(i%7 + 1)
		d, err := f.caller(l, i).admitRules(ctx, r)
		if err != nil {
			t.Error(err)
		} else if d.Allowed {
			charged.Add(r.Cost)
		}
	})
	got := charged.Load()
	if got > 1000 || got < 994 {
		t.Fatalf("charged %d, want 994..1000", got)
	}
	if got < 1000 {
		r.Cost = 1000 - got
		requireAllowed(t, l, r)
	}
	r.Cost = 1
	requireDenied(t, l, r)
}

func testContentionOverlapping(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	publishRules(t, l, contentionRule("global", 122, 2))
	for i := 0; i < 4; i++ {
		publishRules(t, l, contentionRule(fmt.Sprintf("tenant%d", i), 17, 1))
	}
	var admitted [4]atomic.Int64
	contentionWave(t, 400, func(ctx context.Context, i int) {
		tenant := i % 4
		d, err := f.caller(l, i).admitRules(ctx, contentionRule("global", 122, 2), contentionRule(fmt.Sprintf("tenant%d", tenant), 17, 1))
		if err != nil {
			t.Error(err)
		} else if d.Allowed {
			admitted[tenant].Add(1)
		}
	})
	var total int64
	for i := range admitted {
		got := admitted[i].Load()
		total += got
		if got > 17 {
			t.Fatalf("tenant %d admitted %d > 17", i, got)
		}
		r := contentionRule(fmt.Sprintf("tenant%d", i), 17, 1)
		if got < 17 {
			r.Cost = 17 - got
			requireAllowed(t, l, r)
		}
		r.Cost = 1
		requireDenied(t, l, r)
	}
	if total != 61 {
		t.Fatalf("global admissions %d, want 61", total)
	}
	requireDenied(t, l, contentionRule("global", 122, 1))
}

func testContentionIsolation(t *testing.T, f *contentionFixture) {
	// Same quota name; vary namespace alone, domain alone, and both.
	seed := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	limiters := make([]*limiter, 32)
	for i := range limiters {
		ns, domain := "ns-"+seed, "domain-"+seed
		if i%2 == 0 {
			ns = fmt.Sprintf("%s-%d", ns, i)
		} else {
			domain = fmt.Sprintf("%s-%d", domain, i)
		}
		l, err := newLimiter(f.clients[0], ns, domain)
		if err != nil {
			t.Fatal(err)
		}
		limiters[i] = l
		publishRules(t, l, contentionRule("q", 1, 1))
		f.cleanup(t, l)
	}
	var originals [32]atomic.Int64
	contentionWave(t, 512, func(ctx context.Context, i int) {
		domain := i % len(limiters)
		d, err := f.caller(limiters[domain], i).admitRules(ctx, contentionRule("q", 1, 1))
		if err != nil {
			t.Errorf("domain %d: %+v %v", domain, d, err)
		} else if d.Allowed {
			originals[domain].Add(1)
		}
	})
	for i, l := range limiters {
		if originals[i].Load() != 1 {
			t.Fatalf("domain %d originals=%d", i, originals[i].Load())
		}
		requireDenied(t, l, contentionRule("q", 1, 1))
	}
}

func testContentionWorkers(t *testing.T, f *contentionFixture) {
	// Keep each worker active for multiple sequential calls. Half hit one hot
	// domain; half spread over eight other domains. All workers contend
	// on the same quotas. This is a bounded workload, not a long-duration soak.
	limiters := make([]*limiter, 9)
	for i := range limiters {
		limiters[i] = f.limiter(t)
		publishRules(t, limiters[i], contentionRule("q", 200, 2))
	}
	var admitted [9]atomic.Int64
	contentionWave(t, 64, func(ctx context.Context, worker int) {
		domain := 0
		if worker%2 != 0 {
			domain = 1 + (worker/2)%8
		}
		current := f.caller(limiters[domain], worker)
		for iteration := 0; iteration < 40; iteration++ {
			r := contentionRule("q", 200, 2)
			d, err := current.admitRules(ctx, r)
			if err != nil {
				t.Error(err)
				return
			}
			if d.Allowed {
				admitted[domain].Add(1)
			}
		}
	})
	for i, l := range limiters {
		if got := admitted[i].Load(); got != 100 {
			t.Fatalf("domain %d admitted %d, want 100", i, got)
		}
		requireDenied(t, l, contentionRule("q", 200, 1))
	}
}

func testContentionCancellation(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	publishRules(t, l, contentionRule("q", 200, 1))
	var allowed atomic.Int64
	contentionWave(t, 600, func(ctx context.Context, i int) {
		current := f.caller(l, i)
		r := contentionRule("q", 200, 1)
		var err error
		var want error
		switch i % 6 {
		case 0:
			var d Decision
			d, err = current.admitRules(ctx, r)
			if err == nil && d.Allowed {
				allowed.Add(1)
			}
		case 1:
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = current.admitRules(canceled, contentionRule("untouched", 1, 1))
			want = context.Canceled
		case 2:
			expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
			defer cancel()
			_, err = current.admitRules(expired, r)
			want = context.DeadlineExceeded
		case 3:
			_, err = current.admitRules(ctx, r, r)
			want = ErrInvalid
		case 4:
			var d Decision
			d, err = current.admitRules(ctx, r)
			if err == nil && d.Allowed {
				allowed.Add(1)
			}
		case 5:
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = current.admitRules(canceled, r)
			want = context.Canceled
		}
		if want == nil {
			if err != nil {
				t.Error(err)
			}
		} else if !errors.Is(err, want) || OutcomeOf(err) != NotCharged {
			t.Errorf("want %v/not_charged, got %v", want, err)
		}
	})
	if allowed.Load() != 200 {
		t.Fatalf("admitted=%d want 200", allowed.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n, err := f.clients[0].Exists(ctx, l.key("untouched")).Result(); err != nil || n != 0 {
		t.Fatalf("pre-dispatch errors wrote %d keys: %v", n, err)
	}
	requireDenied(t, l, contentionRule("q", 200, 1))
}

func testContentionBadState(t *testing.T, f *contentionFixture) {
	for _, kind := range []string{"quota", "wrong_type"} {
		t.Run(kind, func(t *testing.T) {
			l := f.limiter(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r := contentionRule("q", 10, 1)
			publishRules(t, l, r, contentionRule("side", 10, 1))
			requireAllowed(t, l, r)
			before := quotaState(t, f, l, "q")
			rules := []Rule{r, contentionRule("side", 10, 1)}
			var seedErr error
			want, outcome := ErrState, Unknown
			switch kind {
			case "quota":
				seedErr = f.clients[0].Set(ctx, l.key("side"), "broken", time.Minute).Err()
				outcome = NotCharged
			case "wrong_type":
				seedErr = f.clients[0].LPush(ctx, l.key("side"), "wrongtype").Err()
				outcome = NotCharged
			}
			if seedErr != nil {
				t.Fatal(seedErr)
			}
			contentionWave(t, 300, func(ctx context.Context, i int) {
				current := f.caller(l, i)
				_, err := current.admitRules(ctx, rules...)
				if !errors.Is(err, want) || OutcomeOf(err) != outcome {
					t.Errorf("want %v/%s, got %v", want, outcome, err)
				}
			})
			if quotaState(t, f, l, "q") != before {
				t.Fatal("failed concurrent requests debited healthy quota")
			}
		})
	}
}

// Inject loss only after the real server has executed successfully. The wrapper
// is safe to share and counts dispatched successful commands (including denials).
type concurrentResponseLoss struct {
	redis.Scripter
	calls atomic.Int64
	loss  error
}

func (s *concurrentResponseLoss) lose(cmd *redis.Cmd) *redis.Cmd {
	if cmd.Err() == nil {
		s.calls.Add(1)
		cmd.SetErr(s.loss)
	}
	return cmd
}
func (s *concurrentResponseLoss) Eval(ctx context.Context, src string, keys []string, args ...interface{}) *redis.Cmd {
	return s.lose(s.Scripter.Eval(ctx, src, keys, args...))
}
func (s *concurrentResponseLoss) EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) *redis.Cmd {
	return s.lose(s.Scripter.EvalSha(ctx, sha, keys, args...))
}

func testContentionLostResponses(t *testing.T, f *contentionFixture) {
	for _, loss := range []struct {
		name string
		err  error
	}{
		{"eof", io.ErrUnexpectedEOF}, {"canceled_after_execution", context.Canceled}, {"deadline_after_execution", context.DeadlineExceeded},
	} {
		t.Run(loss.name, func(t *testing.T) {
			l := f.limiter(t)
			r := contentionRule("q", 100, 1)
			publishRules(t, l, r)
			wrapped := &concurrentResponseLoss{Scripter: f.clients[0], loss: loss.err}
			broken := &limiter{client: wrapped, prefix: l.prefix}
			contentionWave(t, 400, func(ctx context.Context, i int) {
				_, err := broken.admitRules(ctx, r)
				if !errors.Is(err, loss.err) || OutcomeOf(err) != Unknown {
					t.Errorf("want unknown lost response, got %v", err)
				}
			})
			if wrapped.calls.Load() != 400 {
				t.Fatalf("unexpected internal retries: %d executions", wrapped.calls.Load())
			}
			requireDenied(t, l, r)
		})
	}
}

func testContentionSmallPool(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	small := contentionClient(f.cluster, 1)
	defer small.Close()
	current := &limiter{client: small, prefix: l.prefix}
	publishRules(t, l, contentionRule("q", 73, 1), contentionRule("after", 1, 1))
	var admitted atomic.Int64
	contentionWave(t, 500, func(ctx context.Context, i int) {
		d, err := current.admitRules(ctx, contentionRule("q", 73, 1))
		if err != nil {
			t.Error(err)
		} else if d.Allowed {
			admitted.Add(1)
		}
	})
	if admitted.Load() != 73 {
		t.Fatalf("small pool admitted %d", admitted.Load())
	}
	// The same client remains usable after contention drains.
	requireAllowed(t, current, contentionRule("after", 1, 1))
}

func testContentionColdScripts(t *testing.T, f *contentionFixture) {
	// Dedicated disposable fixtures only: SCRIPT FLUSH affects the script cache
	// of the whole server, not quota data. Never run against shared/production Redis.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if c, ok := f.clients[0].(*redis.ClusterClient); ok {
		err = c.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error { return c.ScriptFlush(ctx).Err() })
	} else {
		err = f.clients[0].ScriptFlush(ctx).Err()
	}
	if err != nil {
		t.Fatal(err)
	}
	l := f.limiter(t)
	publishRules(t, l, contentionRule("q", 1, 1))
	var originals atomic.Int64
	contentionWave(t, 300, func(ctx context.Context, i int) {
		current := f.caller(l, i)
		d, err := current.admitRules(ctx, contentionRule("q", 1, 1))
		if err != nil {
			t.Errorf("cold script %+v %v", d, err)
		} else if d.Allowed {
			originals.Add(1)
		}
	})
	if originals.Load() != 1 {
		t.Fatalf("cold script originals=%d", originals.Load())
	}
	requireDenied(t, l, contentionRule("q", 1, 1))
}

func testContentionRefill(t *testing.T, f *contentionFixture) {
	t.Run("natural_refill", func(t *testing.T) {
		l := f.limiter(t)
		// With a fast real clock, don't assert an exact burst count: Redis service
		// time can span replenishment. Bound accepted cost by server elapsed time.
		r := Rule{Name: "q", Policy: Policy{Rate: 1, Period: 10 * time.Millisecond, Burst: 20}, Cost: 1}
		publishRules(t, l, r)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for wave := 0; wave < 3; wave++ {
			before, err := f.clients[0].Time(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			var admitted atomic.Int64
			contentionWave(t, 200, func(ctx context.Context, i int) {
				d, err := f.caller(l, i).admitRules(ctx, r)
				if err != nil {
					t.Error(err)
				} else if d.Allowed {
					admitted.Add(1)
				}
			})
			after, err := f.clients[0].Time(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			bound := r.Policy.Burst + int64(after.Sub(before)/r.Policy.Period)
			if got := admitted.Load(); got < r.Policy.Burst || got > bound {
				t.Fatalf("wave %d admitted %d outside [%d,%d]", wave, got, r.Policy.Burst, bound)
			}
			time.Sleep(time.Duration(r.Policy.Burst)*r.Policy.Period + 10*time.Millisecond) // Logical recovery requires no physical deletion.
		}
	})
}

func testContentionPoolExhaustion(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	publishRules(t, l, contentionRule("q", 1, 1))
	small := contentionClient(f.cluster, 1)
	defer small.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Pin the only connection on every master, so no admission can reach Redis.
	var mu sync.Mutex
	var held []*redis.Conn
	hold := func(ctx context.Context, c *redis.Client) error {
		conn := c.Conn()
		if err := conn.Ping(ctx).Err(); err != nil {
			conn.Close()
			return err
		}
		mu.Lock()
		held = append(held, conn)
		mu.Unlock()
		return nil
	}
	release := func() {
		for _, c := range held {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}
		held = nil
	}
	defer release()
	var err error
	if c, ok := small.(*redis.ClusterClient); ok {
		// Populate go-redis routing metadata before deliberately exhausting its pools.
		if err := c.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error { return node.Command(ctx).Err() }); err != nil {
			t.Fatal(err)
		}
		if err := c.Exists(ctx, l.key("warm-metadata")).Err(); err != nil {
			t.Fatal(err)
		}
		err = c.ForEachMaster(ctx, hold)
	} else {
		err = hold(ctx, small.(*redis.Client))
	}
	if err != nil {
		t.Fatal(err)
	}
	current := &limiter{client: small, prefix: l.prefix}
	contentionWave(t, 100, func(parent context.Context, i int) {
		deadline, cancel := context.WithTimeout(parent, 100*time.Millisecond)
		defer cancel()
		_, err := current.admitRules(deadline, contentionRule("q", 1, 1))
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrBackend) || OutcomeOf(err) != Unknown {
			t.Errorf("pool wait deadline: %v", err)
		}
	})
	// The SDK conservatively reports Unknown after dispatch; the fixture can
	// independently prove that these particular requests never got a connection.
	if n, err := f.clients[0].Exists(ctx, l.key("q")).Result(); err != nil || n != 0 {
		t.Fatalf("blocked pool wrote %d keys: %v", n, err)
	}
	release()
	if d, err := current.admitRules(ctx, contentionRule("q", 1, 1)); err != nil || !d.Allowed {
		t.Fatalf("pool did not recover: %+v %v", d, err)
	}
}

// Launch the same race-instrumented test executable as four independent SDK
// processes. A file barrier ensures all clients are connected before admission.
func testContentionProcesses(t *testing.T, f *contentionFixture) {
	for _, mode := range []string{"allow"} {
		t.Run(mode, func(t *testing.T) {
			l := f.limiter(t)
			publishRules(t, l, contentionRule("q", 127, 1))
			dir := t.TempDir()
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			type processResult struct {
				index  int
				output []byte
				err    error
			}
			done := make(chan processResult, 4)
			for i := 0; i < 4; i++ {
				cmd := exec.CommandContext(ctx, exe, "-test.run=^TestConcurrencyProcessWorker$", "-test.timeout=35s")
				cmd.Env = append(os.Environ(), "PACELOCK_CONTENTION_WORKER="+dir, "PACELOCK_CONTENTION_INDEX="+strconv.Itoa(i), "PACELOCK_CONTENTION_PREFIX="+l.prefix, "PACELOCK_CONTENTION_CLUSTER="+strconv.FormatBool(f.cluster), "PACELOCK_CONTENTION_MODE="+mode)
				go func(i int) { out, err := cmd.CombinedOutput(); done <- processResult{i, out, err} }(i)
			}
			// Always join children before fixture and temporary-file cleanup, even if
			// startup fails. Early exit is reported rather than silently timing out.
			finished := 0
			defer func() {
				cancel()
				for finished < 4 {
					<-done
					finished++
				}
			}()
			for {
				ready := 0
				for i := 0; i < 4; i++ {
					if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("ready%d", i))); err == nil {
						ready++
					}
				}
				if ready == 4 {
					break
				}
				select {
				case r := <-done:
					finished++
					t.Fatalf("worker %d exited before barrier: %v\n%s", r.index, r.err, r.output)
				case <-ctx.Done():
					t.Fatal("workers did not reach start barrier")
				case <-time.After(10 * time.Millisecond):
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "start"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			var total int64
			for finished < 4 {
				r := <-done
				finished++
				if r.err != nil {
					t.Errorf("worker %d: %v\n%s", r.index, r.err, r.output)
					continue
				}
				b, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("result%d", r.index)))
				if err != nil {
					t.Fatal(err)
				}
				var count int64
				if err := json.Unmarshal(b, &count); err != nil {
					t.Fatal(err)
				}
				total += count
			}
			want := int64(127)
			if total != want {
				t.Fatalf("cross-process admissions=%d, want %d", total, want)
			}
			burst := int64(127)
			requireDenied(t, l, contentionRule("q", burst, 1))
		})
	}
}

func TestConcurrencyProcessWorker(t *testing.T) {
	dir := os.Getenv("PACELOCK_CONTENTION_WORKER")
	if dir == "" {
		t.Skip("subprocess helper for TestRedisConcurrency")
	}
	cluster, err := strconv.ParseBool(os.Getenv("PACELOCK_CONTENTION_CLUSTER"))
	if err != nil {
		t.Fatal(err)
	}
	c := contentionClient(cluster, 8)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	l := &limiter{client: c, prefix: os.Getenv("PACELOCK_CONTENTION_PREFIX")}
	index := os.Getenv("PACELOCK_CONTENTION_INDEX")
	if err := os.WriteFile(filepath.Join(dir, "ready"+index), nil, 0600); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "start")); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	var admitted atomic.Int64
	contentionWave(t, 64, func(ctx context.Context, i int) {
		d, err := l.admitRules(ctx, contentionRule("q", 127, 1))
		if err != nil {
			t.Error(err)
		} else if d.Allowed {
			admitted.Add(1)
		}
	})
	b, err := json.Marshal(admitted.Load())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result"+index), b, 0600); err != nil {
		t.Fatal(err)
	}
}
