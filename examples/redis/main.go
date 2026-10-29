// Demo: an LLM gateway admitting one request against two shared quotas
// (requests-per-minute and input tokens-per-minute) in one namespace/domain.
//
// Start a disposable Redis and run:
//
//	docker run -d -p 127.0.0.1:16379:6379 redis:7-alpine
//	go run . -redis 127.0.0.1:16379
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	redisdriver "github.com/tr1v3r/pacelock/driver/redis"
)

func main() {
	addr := flag.String("redis", "127.0.0.1:6379", "Redis address")
	flag.Parse()

	client := redis.NewClient(&redis.Options{Addr: *addr, ContextTimeoutEnabled: true})
	defer client.Close()

	// The driver owns atomic admission; the limiter is scoped to one
	// provider account. All instances sharing this namespace/domain
	// draw from the same quota state.
	driver, err := redisdriver.New(client)
	if err != nil {
		log.Fatal(err)
	}
	// Configuration may live in code; publish it explicitly before requests.
	// Backend-authoritative execution lets rolling pods share the latest policy.
	rpm := pacelock.Quota{Name: "rpm", Policy: pacelock.Policy{
		Revision: 1, Rate: 6, Period: time.Minute, Burst: 6,
	}}
	tpm := pacelock.Quota{Name: "input-tpm", Policy: pacelock.Policy{
		Revision: 1, Rate: 1000, Period: time.Minute, Burst: 1000,
	}}
	limiter, err := pacelock.New(driver, "llm-gateway", "provider-account-a")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	// Revisions come from one authority, not per-pod counters. Handle stale
	// publication explicitly; rollback uses old parameters at a higher revision.
	if err := limiter.UpdateQuotas(ctx, rpm, tpm); err != nil {
		log.Fatalf("publish policies (stale=%v): %v", errors.Is(err, pacelock.ErrPolicyMismatch), err)
	}

	// 1. Immediate admission: charge 1 request and 200 input tokens.
	// Charges may select any nonempty subset, in any order.
	d, err := limiter.Allow(ctx, pacelock.Charge{Name: tpm.Name, Cost: 200}, pacelock.Charge{Name: rpm.Name, Cost: 1})
	if err != nil {
		log.Fatalf("Allow failed (outcome=%v); do not blindly retry Unknown: %v", pacelock.OutcomeOf(err), err)
	}
	fmt.Printf("Allow:     allowed=%v\n", d.Allowed)

	// 2. Denial is atomic and free: the remaining 800 tokens fit two heavy calls.
	for i := 1; i <= 4; i++ {
		d, err := limiter.Allow(ctx, pacelock.Charge{Name: rpm.Name, Cost: 1}, pacelock.Charge{Name: tpm.Name, Cost: 400})
		if err != nil {
			log.Fatalf("heavy call failed (outcome=%v); stop without retry: %v", pacelock.OutcomeOf(err), err)
		}
		var retryAfter time.Duration
		if err == nil && !d.Allowed {
			retryAfter = d.RetryAfter
		}
		fmt.Printf("heavy %d:   allowed=%v retry_after=%v limited=%v err=%v\n",
			i, d.Allowed, retryAfter, d.Limited, err)
	}

	// 3. Blocking wait: retry after the advisory RetryAfter until every
	// selected quota admits atomically or the deadline hits.
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	start := time.Now()
	err = limiter.Wait(waitCtx,
		pacelock.Charge{Name: rpm.Name, Cost: 1},
		pacelock.Charge{Name: tpm.Name, Cost: 200})
	fmt.Printf("Wait:      err=%v elapsed=%v\n", err, time.Since(start).Round(time.Millisecond))

	// Unknown means a debit cannot be ruled out. Do not blindly retry it.
	// Wait retries confirmed capacity denials only; it stops on every error.
	if _, err := limiter.Allow(ctx, pacelock.Charge{Name: rpm.Name, Cost: 0}); err != nil {
		fmt.Printf("Invalid: outcome=%v is_invalid=%v\n", pacelock.OutcomeOf(err), errors.Is(err, pacelock.ErrInvalid))
	}
}
