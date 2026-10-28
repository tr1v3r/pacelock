package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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

func (f *contentionFixture) limiter(t *testing.T) *Limiter {
	t.Helper()
	l, err := New(f.clients[0], "pacelock-contention", fmt.Sprintf("%d-%d-%d", os.Getpid(), time.Now().UnixNano(), contentionSequence.Add(1)))
	if err != nil {
		t.Fatal(err)
	}
	f.cleanup(t, l)
	return l
}

func (f *contentionFixture) cleanup(t *testing.T, l *Limiter) {
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

func (f *contentionFixture) caller(l *Limiter, i int) *Limiter {
	return &Limiter{client: f.clients[i%len(f.clients)], prefix: l.prefix, observer: l.observer}
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

func requireAllowed(t *testing.T, l *Limiter, rules ...Rule) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if d, err := l.Allow(ctx, rules...); err != nil || !d.Allowed {
		t.Fatalf("want allowed: %+v %v", d, err)
	}
}

func requireDenied(t *testing.T, l *Limiter, rules ...Rule) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if d, err := l.Allow(ctx, rules...); err != nil || d.Allowed || d.RetryAfter <= 0 || len(d.Limited) == 0 {
		t.Fatalf("want denied: %+v %v", d, err)
	}
}

func quotaState(t *testing.T, f *contentionFixture, l *Limiter, name string) string {
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
				{"same_id", testContentionSameID},
				{"mixed_apis_and_ids", testContentionMixedAPIs},
				{"id_conflict", testContentionConflict},
				{"policy_race", testContentionPolicy},
				{"invalid_and_canceled", testContentionCancellation},
				{"lookup_and_replay", testContentionLookup},
				{"malformed_records_and_quotas", testContentionBadState},
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
				var admitted atomic.Int64
				contentionWave(t, n, func(ctx context.Context, i int) {
					// Different argument orders must not affect atomic admission.
					r := append([]Rule(nil), rules...)
					if i%2 == 0 {
						for a, b := 0, len(r)-1; a < b; a, b = a+1, b-1 {
							r[a], r[b] = r[b], r[a]
						}
					}
					d, err := f.caller(l, i).Allow(ctx, r...)
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
	requireAllowed(t, l, r) // Establish a common TAT before competing weighted calls.
	var charged atomic.Int64
	charged.Store(1)
	contentionWave(t, 500, func(ctx context.Context, i int) {
		r := r
		r.Cost = int64(i%7 + 1)
		d, err := f.caller(l, i).Allow(ctx, r)
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
	var admitted [4]atomic.Int64
	contentionWave(t, 400, func(ctx context.Context, i int) {
		tenant := i % 4
		d, err := f.caller(l, i).Allow(ctx, contentionRule("global", 122, 2), contentionRule(fmt.Sprintf("tenant%d", tenant), 17, 1))
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
	// Same ID/quota name; vary namespace alone, domain alone, and both.
	seed := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	limiters := make([]*Limiter, 32)
	for i := range limiters {
		ns, domain := "ns-"+seed, "domain-"+seed
		if i%2 == 0 {
			ns = fmt.Sprintf("%s-%d", ns, i)
		} else {
			domain = fmt.Sprintf("%s-%d", domain, i)
		}
		l, err := New(f.clients[0], ns, domain)
		if err != nil {
			t.Fatal(err)
		}
		limiters[i] = l
		f.cleanup(t, l)
	}
	var originals [32]atomic.Int64
	contentionWave(t, 512, func(ctx context.Context, i int) {
		domain := i % len(limiters)
		d, err := f.caller(limiters[domain], i).AllowOnce(ctx, "same", time.Minute, contentionRule("q", 1, 1))
		if err != nil || !d.Allowed {
			t.Errorf("domain %d: %+v %v", domain, d, err)
		} else if !d.Replayed {
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

func testContentionSameID(t *testing.T, f *contentionFixture) {
	for _, allow := range []bool{true, false} {
		t.Run(fmt.Sprintf("allowed_%t", allow), func(t *testing.T) {
			l := f.limiter(t)
			r := contentionRule("q", 1, 1)
			if !allow {
				requireAllowed(t, l, r)
			}
			receipts := make([]Receipt, 1000)
			var originals atomic.Int64
			contentionWave(t, len(receipts), func(ctx context.Context, i int) {
				d, err := f.caller(l, i).AllowOnce(ctx, "same", time.Minute, r)
				if err != nil {
					t.Error(err)
					return
				}
				receipts[i] = d
				if !d.Replayed {
					originals.Add(1)
				}
			})
			if originals.Load() != 1 {
				t.Fatalf("originals=%d", originals.Load())
			}
			for _, d := range receipts {
				if d.Allowed != allow || !reflect.DeepEqual(d.Decision, receipts[0].Decision) {
					t.Fatalf("inconsistent retained result: %+v", d)
				}
			}
			requireDenied(t, l, r)
		})
	}
}

func testContentionMixedAPIs(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	r := contentionRule("q", 73, 1)
	receipts := make([]Receipt, 400)
	var admitted, originals atomic.Int64
	contentionWave(t, 500, func(ctx context.Context, i int) {
		current := f.caller(l, i)
		if i >= 400 {
			d, err := current.Allow(ctx, r)
			if err != nil {
				t.Error(err)
			} else if d.Allowed {
				admitted.Add(1)
			}
			return
		}
		d, err := current.AllowOnce(ctx, fmt.Sprintf("id%d", i%100), time.Minute, r)
		if err != nil {
			t.Error(err)
			return
		}
		receipts[i] = d
		if !d.Replayed {
			originals.Add(1)
			if d.Allowed {
				admitted.Add(1)
			}
		}
	})
	if originals.Load() != 100 || admitted.Load() != 73 {
		t.Fatalf("originals=%d admitted=%d", originals.Load(), admitted.Load())
	}
	contentionWave(t, 100, func(ctx context.Context, i int) {
		for j := 1; j < 4; j++ {
			if !reflect.DeepEqual(receipts[i].Decision, receipts[i+j*100].Decision) {
				t.Errorf("ID %d disagrees", i)
			}
		}
		d, found, err := f.caller(l, i).Lookup(ctx, fmt.Sprintf("id%d", i))
		if err != nil || !found || !reflect.DeepEqual(d, receipts[i].Decision) {
			t.Errorf("lookup ID %d: %+v %t %v", i, d, found, err)
		}
	})
	requireDenied(t, l, r)
}

func testContentionConflict(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	var originals, conflicts, winningCost atomic.Int64
	contentionWave(t, 500, func(ctx context.Context, i int) {
		cost := int64(i%2 + 1)
		d, err := f.caller(l, i).AllowOnce(ctx, "same", time.Minute, contentionRule("q", 10, cost))
		if errors.Is(err, ErrConflict) && OutcomeOf(err) == NotCharged {
			conflicts.Add(1)
			return
		}
		if err != nil || !d.Allowed {
			t.Errorf("%+v %v", d, err)
			return
		}
		if !d.Replayed {
			originals.Add(1)
			winningCost.Store(cost)
		}
	})
	if originals.Load() != 1 || conflicts.Load() != 250 {
		t.Fatalf("originals=%d conflicts=%d", originals.Load(), conflicts.Load())
	}
	requireAllowed(t, l, contentionRule("q", 10, 10-winningCost.Load()))
	requireDenied(t, l, contentionRule("q", 10, 1))
}

func testContentionPolicy(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	var mismatches, admitted, winningBurst atomic.Int64
	contentionWave(t, 200, func(ctx context.Context, i int) {
		burst := int64(10 * (i%2 + 1))
		d, err := f.caller(l, i).Allow(ctx, contentionRule("q", burst, 1), contentionRule(fmt.Sprintf("side%d", i), 1, 1))
		if errors.Is(err, ErrPolicyMismatch) && OutcomeOf(err) == NotCharged {
			mismatches.Add(1)
		} else if err != nil {
			t.Error(err)
		} else if d.Allowed {
			admitted.Add(1)
			winningBurst.Store(burst)
			return
		}
		if n, err := f.clients[0].Exists(ctx, l.key(fmt.Sprintf("side%d", i))).Result(); err != nil || n != 0 {
			t.Errorf("failed request wrote side quota: %d %v", n, err)
		}
	})
	if mismatches.Load() != 100 || admitted.Load() != winningBurst.Load() || admitted.Load() == 0 {
		t.Fatalf("mismatches=%d admitted=%d burst=%d", mismatches.Load(), admitted.Load(), winningBurst.Load())
	}
}

func testContentionWorkers(t *testing.T, f *contentionFixture) {
	// Keep each worker active for multiple sequential calls. Half hit one hot
	// domain; half spread over eight other domains. Both admission APIs contend
	// on the same quotas. This is a bounded workload, not a long-duration soak.
	limiters := make([]*Limiter, 9)
	for i := range limiters {
		limiters[i] = f.limiter(t)
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
			var d Decision
			var err error
			if iteration%2 == 0 {
				d, err = current.Allow(ctx, r)
			} else {
				var receipt Receipt
				receipt, err = current.AllowOnce(ctx, fmt.Sprintf("w%d-i%d", worker, iteration), time.Minute, r)
				if receipt.Replayed {
					t.Error("unique worker ID replayed")
				}
				d = receipt.Decision
			}
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
