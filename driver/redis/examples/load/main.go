// load is a bounded measurement tool for disposable Redis deployments.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	redisdriver "github.com/tr1v3r/pacelock/driver/redis"
)

// metrics aggregates calls and a bounded uniform latency sample across concurrent workers.
type metrics struct {
	// Mutex protects all counters, the latency reservoir and its random source.
	sync.Mutex
	// Calls counts completions; Allowed/Denied count successful decisions (Denied includes missing lookups).
	// Errors counts failures; Unknown and ContextErrors count their outcome/context subsets.
	// Found counts successful lookups; Replayed counts retained AllowOnce decisions.
	Calls, Allowed, Denied, Errors, Unknown, Found, Replayed, ContextErrors int64
	// Samples holds up to 100000 uniformly sampled call durations in microseconds, including errors.
	Samples []int64
	// random is the deterministic source used for reservoir sampling, protected by Mutex.
	random *rand.Rand
}

// add records one completed call, including failed-call latency, under the metrics mutex.
func (m *metrics) add(d pacelock.Decision, found, replayed bool, err error, elapsed time.Duration) {
	m.Lock()
	defer m.Unlock()
	m.Calls++
	if err != nil {
		m.Errors++
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			m.ContextErrors++
		}
		if pacelock.OutcomeOf(err) == pacelock.Unknown {
			m.Unknown++
		}
	} else {
		if d.Allowed {
			m.Allowed++
		} else {
			m.Denied++
		}
		if found {
			m.Found++
		}
		if replayed {
			m.Replayed++
		}
	}
	value := elapsed.Microseconds()
	if len(m.Samples) < 100000 {
		m.Samples = append(m.Samples, value)
	} else if i := m.random.Int63n(m.Calls); i < int64(len(m.Samples)) {
		m.Samples[i] = value
	}
}

// info collects selected Redis diagnostics from a standalone server or each Cluster primary.
func info(ctx context.Context, c redis.UniversalClient) map[string]string {
	if cluster, ok := c.(*redis.ClusterClient); ok {
		all := map[string]string{}
		var mu sync.Mutex
		err := cluster.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			data := info(ctx, node)
			mu.Lock()
			defer mu.Unlock()
			for k, v := range data {
				all[node.Options().Addr+"/"+k] = v
			}
			return nil
		})
		if err != nil {
			all["cluster_error"] = err.Error()
		}
		return all
	}
	out := map[string]string{}
	for _, section := range []string{"server", "memory", "stats", "cpu", "replication"} {
		s, err := c.Info(ctx, section).Result()
		if err != nil {
			out[section+"_error"] = err.Error()
			continue
		}
		for _, line := range strings.Split(s, "\r\n") {
			k, v, ok := strings.Cut(line, ":")
			if ok && keepInfo(k) {
				out[k] = v
			}
		}
	}
	return out
}

// keepInfo limits captured Redis INFO fields to deployment and resource diagnostics.
func keepInfo(k string) bool {
	switch k {
	case "redis_version", "redis_mode", "os", "arch_bits", "uptime_in_seconds", "used_memory", "used_memory_peak", "used_memory_rss", "maxmemory", "maxmemory_policy", "connected_clients", "total_commands_processed", "total_error_replies", "rejected_connections", "evicted_keys", "expired_keys", "used_cpu_sys", "used_cpu_user", "role", "connected_slaves", "master_repl_offset":
		return true
	}
	return false
}

// main reports bounded load-tool errors and exits unsuccessfully.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run validates disposable-backend settings, measures bounded load and cleans up its own namespace.
func run() error {
	addr := flag.String("addr", "127.0.0.1:16379", "disposable standalone/primary endpoint")
	clusterMode := flag.Bool("cluster", false, "addr is comma-separated disposable Cluster seeds")
	disposable := flag.Bool("disposable", false, "confirm this is a dedicated disposable backend")
	duration := flag.Duration("duration", 30*time.Second, "measurement interval")
	warmup := flag.Duration("warmup", time.Second, "warmup interval")
	retention := flag.Duration("retention", 10*time.Second, "idempotency retention")
	workers := flag.Int("workers", 16, "concurrent workers")
	domains := flag.Int("domains", 1, "1 is a hot domain; larger values spread independent quotas")
	dimensions := flag.Int("rules", 4, "quota dimensions 1..32")
	rps := flag.Int("rps", 0, "approximate aggregate offered rate; 0 means closed-loop saturation")
	mode := flag.String("mode", "allow", "allow|deny|once|once-deny|replay|lookup")
	expiry := flag.Bool("expiry", false, "wait retention+2s and measure memory/keys again")
	flag.Parse()
	if !*disposable {
		return fmt.Errorf("requires -disposable; never use shared or production Redis")
	}
	if *duration <= 0 || *warmup < 0 || *workers < 1 || *workers > 1024 || *domains < 1 || *domains > 1024 || *dimensions < 1 || *dimensions > 32 || *rps < 0 || *rps > 1000000 || *retention < time.Millisecond || *retention > 24*time.Hour || *retention%time.Millisecond != 0 {
		return fmt.Errorf("invalid bounded load configuration")
	}
	switch *mode {
	case "allow", "deny", "once", "once-deny", "replay", "lookup":
	default:
		return fmt.Errorf("invalid mode")
	}
	var c redis.UniversalClient
	if *clusterMode {
		c = redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(*addr, ","), MaxRetries: -1, MaxRedirects: -1, ContextTimeoutEnabled: true, PoolSize: *workers, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second})
	} else {
		c = redis.NewClient(&redis.Options{Addr: *addr, MaxRetries: -1, ContextTimeoutEnabled: true, PoolSize: *workers, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second})
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		return err
	}
	namespace := fmt.Sprintf("pacelock-load-%d", time.Now().UnixNano())
	limiters := make([]*pacelock.Limiter, *domains)
	prefixes := make([]string, *domains)
	rules := make([]pacelock.Rule, *dimensions)
	for i := range rules {
		rules[i] = pacelock.Rule{Name: fmt.Sprintf("quota-%d", i), Policy: pacelock.Policy{Rate: 1000000, Period: time.Second, Burst: 1000000}, Cost: 1}
		if *mode == "deny" || *mode == "once-deny" {
			rules[i].Policy.Rate = 1
			rules[i].Policy.Period = 24 * time.Hour
			rules[i].Policy.Burst = 1
		}
	}
	for i := range limiters {
		domain := fmt.Sprint(i)
		l, err := newLimiter(c, namespace, domain)
		if err != nil {
			return err
		}
		limiters[i] = l
		digest := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(namespace), namespace, domain)))
		prefixes[i] = fmt.Sprintf("pacelock:v1:{%x}:*", digest)
		if *mode == "deny" || *mode == "once-deny" {
			if _, err := l.Allow(ctx, rules...); err != nil {
				return err
			}
		}
		if *mode == "replay" || *mode == "lookup" {
			if _, err := l.AllowOnce(ctx, "seed", 24*time.Hour, rules...); err != nil {
				return err
			}
		}
	}
	keys := func(remove bool) (int64, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var count int64
		var mu sync.Mutex
		scan := func(ctx context.Context, node *redis.Client) error {
			var local int64
			for _, prefix := range prefixes {
				it := node.Scan(ctx, 0, prefix, 1000).Iterator()
				for it.Next(ctx) {
					local++
					if remove {
						if err := node.Del(ctx, it.Val()).Err(); err != nil {
							return err
						}
					}
				}
				if err := it.Err(); err != nil {
					return err
				}
			}
			mu.Lock()
			count += local
			mu.Unlock()
			return nil
		}
		var err error
		if cluster, ok := c.(*redis.ClusterClient); ok {
			err = cluster.ForEachMaster(ctx, scan)
		} else {
			err = scan(ctx, c.(*redis.Client))
		}
		if err != nil {
			return count, err
		}
		return count, nil
	}
	defer func() {
		if _, err := keys(true); err != nil {
			fmt.Fprintln(os.Stderr, "namespace cleanup:", err)
		}
	}()
	phase := func(length time.Duration, label string) *metrics {
		m := &metrics{random: rand.New(rand.NewSource(1))}
		ctx, cancel := context.WithTimeout(context.Background(), length)
		defer cancel()
		var wg sync.WaitGroup
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				var ticker *time.Ticker
				if *rps > 0 {
					ticker = time.NewTicker(time.Duration(float64(time.Second) * float64(*workers) / float64(*rps)))
					defer ticker.Stop()
				}
				for n := 0; ctx.Err() == nil; n++ {
					if ticker != nil {
						select {
						case <-ctx.Done():
							return
						case <-ticker.C:
						}
					}
					l := limiters[(w+n)%len(limiters)]
					id := fmt.Sprintf("%s-%d-%d", label, w, n)
					start := time.Now()
					// Stop scheduling at the phase deadline, but drain in-flight calls.
					call, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					var d pacelock.Decision
					var found, replayed bool
					var err error
					switch *mode {
					case "allow", "deny":
						d, err = l.Allow(call, rules...)
					case "once", "once-deny", "replay":
						if *mode == "replay" {
							id = "seed"
						}
						var receipt pacelock.Receipt
						receipt, err = l.AllowOnce(call, id, func() time.Duration {
							if *mode == "replay" {
								return 24 * time.Hour
							}
							return *retention
						}(), rules...)
						d = receipt.Decision
						replayed = receipt.Replayed
					case "lookup":
						d, found, err = l.Lookup(call, "seed")
					}
					cancel()
					m.add(d, found, replayed, err, time.Since(start))
				}
			}(w)
		}
		wg.Wait()
		return m
	}
	if *warmup > 0 {
		phase(*warmup, "warmup")
	}
	snap := func() map[string]string {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return info(ctx, c)
	}
	before := snap()
	start := time.Now()
	m := phase(*duration, "measure")
	elapsed := time.Since(start)
	after := snap()
	count, countErr := keys(false)
	sort.Slice(m.Samples, func(i, j int) bool { return m.Samples[i] < m.Samples[j] })
	percentile := func(p float64) int64 {
		if len(m.Samples) == 0 {
			return 0
		}
		return m.Samples[int(float64(len(m.Samples)-1)*p)]
	}
	build, _ := debug.ReadBuildInfo()
	result := map[string]interface{}{"mode": *mode, "domains": *domains, "rules": *dimensions, "workers": *workers, "offered_rps": *rps, "duration_seconds": elapsed.Seconds(), "warmup_seconds": warmup.Seconds(), "retention_seconds": retention.Seconds(), "calls": m.Calls, "allowed": m.Allowed, "denied_or_missing": m.Denied, "found": m.Found, "replayed": m.Replayed, "errors": m.Errors, "context_errors": m.ContextErrors, "cluster": *clusterMode, "unknown": m.Unknown, "throughput": float64(m.Calls) / elapsed.Seconds(), "latency_us": map[string]int64{"p50": percentile(.50), "p95": percentile(.95), "p99": percentile(.99)}, "latency_sample_size": len(m.Samples), "latency_sampling": "uniform reservoir, max 100000; includes failures", "keys_after": count, "redis_before": before, "redis_after": after, "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "cpus": runtime.NumCPU(), "pool_stats": c.PoolStats(), "build": build}
	if countErr != nil {
		result["keys_error"] = countErr.Error()
	}
	if *expiry {
		time.Sleep(*retention + 2*time.Second)
		n, err := keys(false)
		result["keys_after_retention"] = n
		result["redis_after_retention"] = snap()
		if err != nil {
			result["expiry_keys_error"] = err.Error()
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

// newLimiter constructs a Redis driver and the shared admission facade.
func newLimiter(client redis.Scripter, namespace, domain string) (*pacelock.Limiter, error) {
	return newLimiterWithOptions(client, namespace, domain, pacelock.Options{})
}

// newLimiterWithOptions attaches application observation to the common facade.
func newLimiterWithOptions(client redis.Scripter, namespace, domain string, options pacelock.Options) (*pacelock.Limiter, error) {
	driver, err := redisdriver.New(client)
	if err != nil {
		return nil, err
	}
	return pacelock.NewWithOptions(driver, namespace, domain, options)
}
