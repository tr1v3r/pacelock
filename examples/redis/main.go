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
	limiter, err := pacelock.New(driver, "llm-gateway", "provider-account-a")
	if err != nil {
		log.Fatal(err)
	}

	// Bind fixed policies once; the bound limiter is immutable and safe
	// for concurrent use. Rebind (a new BindStatic) to change policies.
	rpm := pacelock.Quota{Name: "rpm", Policy: pacelock.Policy{
		Rate: 6, Period: time.Minute, Burst: 6,
	}}
	tpm := pacelock.Quota{Name: "input-tpm", Policy: pacelock.Policy{
		Rate: 1000, Period: time.Minute, Burst: 1000,
	}}
	bound, err := limiter.BindStatic(rpm, tpm)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// 1. Immediate admission: charge 1 request and 200 input tokens.
	// Charges may arrive in any order; each bound quota exactly once.
	d, err := bound.Allow(ctx, tpm.StaticCharge(200), rpm.StaticCharge(1))
	fmt.Printf("Allow:     allowed=%v err=%v\n", d.Allowed, err)

	// 2. Denial is atomic and free: this burst only fits three heavy calls.
	for i := 1; i <= 4; i++ {
		d, err := bound.Allow(ctx, rpm.StaticCharge(1), tpm.StaticCharge(400))
		var retryAfter time.Duration
		if err == nil && !d.Allowed {
			retryAfter = d.RetryAfter
		}
		fmt.Printf("heavy %d:   allowed=%v retry_after=%v limited=%v err=%v\n",
			i, d.Allowed, retryAfter, d.Limited, err)
	}

	// 3. Blocking wait: retry after the advisory RetryAfter until every
	// quota admits atomically or the deadline hits. StaticLimiter has no
	// Wait; feed the same policies and costs to the low-level API.
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	start := time.Now()
	err = limiter.WaitStatic(waitCtx,
		pacelock.Rule{Name: rpm.Name, Policy: rpm.Policy, Cost: 1},
		pacelock.Rule{Name: tpm.Name, Policy: tpm.Policy, Cost: 200})
	fmt.Printf("Wait:      err=%v elapsed=%v\n", err, time.Since(start).Round(time.Millisecond))

	// 4. Idempotent admission: persist the attempt ID *before* dispatch.
	// If the reply is lost, Lookup recovers the authoritative decision
	// instead of double-charging on a blind retry.
	attemptID := "order-4711"
	receipt, err := bound.AllowOnce(ctx, attemptID, time.Minute,
		rpm.StaticCharge(1), tpm.StaticCharge(120))
	fmt.Printf("AllowOnce: allowed=%v replayed=%v err=%v\n",
		receipt.Allowed, receipt.Replayed, err)

	// A replay with the same ID and same charges returns the retained
	// decision (allows and denials alike) without debiting again.
	replay, err := bound.AllowOnce(ctx, attemptID, time.Minute,
		tpm.StaticCharge(120), rpm.StaticCharge(1))
	fmt.Printf("Replay:    allowed=%v replayed=%v err=%v\n",
		replay.Allowed, replay.Replayed, err)

	// 5. Recovery: reconstruct a lost reply from retained state.
	if d, found, err := bound.Lookup(ctx, attemptID); err == nil && found {
		fmt.Printf("Lookup:    found allowed=%v retry_after=%v\n", d.Allowed, d.RetryAfter)
	}

	// 6. Error accounting: failures classify as NotCharged (certainly no
	// debit) or Unknown (cannot rule one out); never assume NotCharged.
	if _, err := bound.AllowOnce(ctx, "", time.Minute, rpm.StaticCharge(1)); err != nil {
		fmt.Printf("Invalid:   outcome=%v is_invalid=%v\n",
			pacelock.OutcomeOf(err), errors.Is(err, pacelock.ErrInvalid))
	}
}
