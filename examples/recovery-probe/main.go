// recovery-probe is destructive only within a synthetic namespace; use the
// accompanying isolated deployment harness, never production credentials.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	"os"
	"time"
)

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
	l, err := pacelock.New(c, "deployment-probe", *domain)
	if err != nil {
		panic(err)
	}
	r := pacelock.Rule{Name: "quota", Rate: 1, Period: time.Hour, Burst: 10, Cost: 1}
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
		d, err := l.AllowOnce(ctx, fmt.Sprintf("%s-%d", *action, i), time.Hour, r)
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
