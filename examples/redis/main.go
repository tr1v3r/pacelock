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
	// Construct fixed policies once; the static limiter snapshots them and is
	// safe for concurrent use. Construct another instance to change policies.
	rpm := pacelock.Quota{Name: "rpm", Policy: pacelock.Policy{
		Rate: 6, Period: time.Minute, Burst: 6,
	}}
	tpm := pacelock.Quota{Name: "input-tpm", Policy: pacelock.Policy{
		Rate: 1000, Period: time.Minute, Burst: 1000,
	}}
	limiter, err := pacelock.NewStatic(driver, "llm-gateway", "provider-account-a", rpm, tpm)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// 1. Immediate admission: charge 1 request and 200 input tokens.
	// Charges may select any nonempty subset, in any order.
	d, err := limiter.Allow(ctx, pacelock.Charge{Name: tpm.Name, Cost: 200}, pacelock.Charge{Name: rpm.Name, Cost: 1})
	fmt.Printf("Allow:     allowed=%v err=%v\n", d.Allowed, err)

	// 2. Denial is atomic and free: this burst only fits three heavy calls.
	for i := 1; i <= 4; i++ {
		d, err := limiter.Allow(ctx, pacelock.Charge{Name: rpm.Name, Cost: 1}, pacelock.Charge{Name: tpm.Name, Cost: 400})
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

	// 4. Idempotent admission: persist the attempt ID *before* dispatch.
	// If the reply is lost, Lookup recovers the authoritative decision
	// instead of double-charging on a blind retry.
	attemptID := "order-4711"
	receipt, err := limiter.AllowOnce(ctx, attemptID, time.Minute,
		pacelock.Charge{Name: rpm.Name, Cost: 1}, pacelock.Charge{Name: tpm.Name, Cost: 120})
	fmt.Printf("AllowOnce: allowed=%v replayed=%v err=%v\n",
		receipt.Allowed, receipt.Replayed, err)

	// A replay with the same ID and same charges returns the retained
	// decision (allows and denials alike) without debiting again.
	replay, err := limiter.AllowOnce(ctx, attemptID, time.Minute,
		pacelock.Charge{Name: tpm.Name, Cost: 120}, pacelock.Charge{Name: rpm.Name, Cost: 1})
	fmt.Printf("Replay:    allowed=%v replayed=%v err=%v\n",
		replay.Allowed, replay.Replayed, err)

	// 5. Recovery: reconstruct a lost reply from retained state.
	if d, found, err := limiter.Lookup(ctx, attemptID); err == nil && found {
		fmt.Printf("Lookup:    found allowed=%v retry_after=%v\n", d.Allowed, d.RetryAfter)
	}

	// 6. Error accounting: failures classify as NotCharged (certainly no
	// debit) or Unknown (cannot rule one out); never assume NotCharged.
	if _, err := limiter.AllowOnce(ctx, "", time.Minute, pacelock.Charge{Name: rpm.Name, Cost: 1}); err != nil {
		fmt.Printf("Invalid:   outcome=%v is_invalid=%v\n",
			pacelock.OutcomeOf(err), errors.Is(err, pacelock.ErrInvalid))
	}
}
