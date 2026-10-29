// This example publishes backend policies and admits known input tokens;
// it does not enforce output TPM.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	redisdriver "github.com/tr1v3r/pacelock/driver/redis"
)

// main checks request and known-input-token budgets before the placeholder model call.
func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	defer client.Close()
	limiter, err := newLimiter(client, "llm-service", "provider-account-a")
	if err != nil {
		log.Fatal(err)
	}
	rpm := pacelock.Quota{Name: "model-a:rpm", Policy: pacelock.Policy{Revision: 1, Rate: 60, Period: time.Minute, Burst: 10}}
	tokens := pacelock.Quota{Name: "model-a:input-tpm", Policy: pacelock.Policy{Revision: 1, Rate: 60000, Period: time.Minute, Burst: 12000}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// In production a control-plane publisher supplies durable policy revisions.
	if err := limiter.UpdateQuotas(ctx, rpm, tokens); err != nil {
		log.Fatal(err)
	}
	d, err := limiter.Allow(ctx, pacelock.Charge{Name: rpm.Name, Cost: 1}, pacelock.Charge{Name: tokens.Name, Cost: 1500})
	if err != nil {
		log.Fatal(err)
	}
	if !d.Allowed {
		log.Printf("limited=%v retry_after=%s", d.Limited, d.RetryAfter)
		return
	}
	log.Print("admitted; call the LLM provider here")
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
