// This example admits known input tokens; it does not enforce output TPM.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

// main checks request and known-input-token budgets before the placeholder model call.
func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	defer client.Close()
	limiter, err := pacelock.New(client, "llm-service", "provider-account-a")
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d, err := limiter.Allow(ctx,
		pacelock.Rule{Name: "model-a:rpm", Rate: 60, Period: time.Minute, Burst: 10, Cost: 1},
		pacelock.Rule{Name: "model-a:input-tpm", Rate: 60000, Period: time.Minute, Burst: 12000, Cost: 1500},
	)
	if err != nil {
		log.Fatal(err)
	}
	if !d.Allowed {
		log.Printf("limited=%v retry_after=%s", d.Limited, d.RetryAfter)
		return
	}
	log.Print("admitted; call the LLM provider here")
}
