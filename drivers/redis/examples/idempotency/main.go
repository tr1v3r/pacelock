// Demonstrates admission recovery, not exactly-once upstream execution.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	redisdriver "github.com/tr1v3r/pacelock/drivers/redis"
)

// main records and replays one admission, demonstrating lookup after an uncertain response.
func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	defer client.Close()
	limiter, err := newLimiterWithOptions(client, "example", "account", pacelock.Options{Observer: func(_ context.Context, event pacelock.Event) {
		log.Printf("operation=%s result=%s duration=%s replayed=%t", event.Operation, event.Result, event.Duration, event.Replayed)
	}})
	if err != nil {
		log.Fatal(err)
	}
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		log.Fatal(err)
	}
	id := hex.EncodeToString(bytes[:]) // Persist alongside the business attempt before use.
	rules := []pacelock.Rule{{Name: "requests", Rate: 60, Period: time.Minute, Burst: 10, Cost: 1}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	receipt, err := limiter.AllowOnce(ctx, id, 10*time.Minute, rules...)
	if err != nil {
		log.Printf("admission error: outcome=%s error=%v", pacelock.OutcomeOf(err), err)
		// A fresh deadline allows a read after the first context has expired.
		query, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		decision, found, lookupErr := limiter.Lookup(query, id)
		log.Printf("recovery: found=%t decision=%+v error=%v", found, decision, lookupErr)
		// Missing/pending records do not establish that no debit happened.
		return
	}
	log.Printf("original decision: %+v", receipt)
	replay, err := limiter.AllowOnce(ctx, id, 10*time.Minute, rules...)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("same admission, no extra debit: %+v", replay)
	// Replayed=true does not authorize another model call. Coordinate business
	// execution separately; every actual upstream attempt consumes its own quota.
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
