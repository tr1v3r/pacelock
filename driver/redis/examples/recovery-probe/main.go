// recovery-probe is destructive only within a synthetic namespace; use the
// accompanying isolated deployment harness, never production credentials.
// It explicitly uses static admission so existing failover measurements stay comparable.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	redisdriver "github.com/tr1v3r/pacelock/driver/redis"
	"os"
	"time"
)

// main seeds or inspects synthetic retained admissions for an isolated failover fixture.
func main() {
	addr := flag.String("addr", "127.0.0.1:17379", "fixture address")
	domain := flag.String("domain", "", "unique fixture run ID")
	action := flag.String("action", "seed", "seed|check")
	flag.Parse()
	if *domain == "" || (*action != "seed" && *action != "check") {
		fmt.Fprintln(os.Stderr, "domain and seed/check action required")
		os.Exit(1)
	}
	c := redis.NewClient(&redis.Options{Addr: *addr, MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second})
	defer c.Close()
	l, err := newLimiter(c, "deployment-probe", *domain)
	if err != nil {
		panic(err)
	}
	r := pacelock.Rule{Name: "quota", Policy: pacelock.Policy{Rate: 1, Period: time.Hour, Burst: 10}, Cost: 1}
	out := map[string]int{"allowed": 0, "denied": 0, "unknown": 0, "errors": 0, "found": 0, "missing": 0}
	if *action == "check" {
		for i := 0; i < 10; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, found, err := l.Lookup(ctx, fmt.Sprintf("seed-%d", i))
			cancel()
			if err != nil {
				out["errors"]++
			} else if found {
				out["found"]++
			} else {
				out["missing"]++
			}
		}
	}
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		d, err := l.AllowStaticOnce(ctx, fmt.Sprintf("%s-%d", *action, i), time.Hour, r)
		cancel()
		if err != nil {
			out["errors"]++
			if pacelock.OutcomeOf(err) == pacelock.Unknown {
				out["unknown"]++
			}
		} else if d.Allowed {
			out["allowed"]++
		} else {
			out["denied"]++
		}
	}
	json.NewEncoder(os.Stdout).Encode(out)
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
