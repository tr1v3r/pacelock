// Run: REDIS_ADDR=localhost:16379 go run ./examples/managed
// All admission is backend-managed. The caller publishes configuration;
// data-plane calls carry only names and costs.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	redisdriver "github.com/tr1v3r/pacelock/driver/redis"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:16379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	driver, err := redisdriver.New(client)
	if err != nil {
		return err
	}
	limiter, err := pacelock.New(driver, "managed-example", "account-a")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A caller-owned config poller may publish this snapshot when its source changes.
	// Revisions must come from a durable monotonic source, not process-local counters.
	q := pacelock.Quota{Name: "requests", Policy: pacelock.Policy{Rate: 10, Period: time.Second, Burst: 10, Revision: 1}}
	if err = limiter.UpdateQuotas(ctx, q); err != nil {
		return err
	}
	// This application node needs neither q.Policy nor its revision to admit work.
	decision, err := limiter.Allow(ctx, pacelock.Charge{Name: "requests", Cost: 1})
	if err != nil {
		return err
	}
	fmt.Printf("allowed=%t retry_after=%s\n", decision.Allowed, decision.RetryAfter)
	return nil
}
