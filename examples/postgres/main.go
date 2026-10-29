// Command postgres demonstrates known-cost admission with an application-owned
// database/sql pool and pgx transport. It does not execute an upstream model call.
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/tr1v3r/pacelock"
	"github.com/tr1v3r/pacelock/driver/postgres"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://pacelock:pacelock-test-only@localhost:15439/pacelock?sslmode=disable"
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	driver, err := postgres.New(db)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A real service runs schema setup with its migration role before accepting traffic.
	if err := driver.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	limiter, err := pacelock.New(driver, "postgres-example", "provider-account-a")
	if err != nil {
		log.Fatal(err)
	}
	rpm := pacelock.Quota{Name: "requests", Policy: pacelock.Policy{Revision: 1, Rate: 60, Period: time.Minute, Burst: 10}}
	tokens := pacelock.Quota{Name: "input-tokens", Policy: pacelock.Policy{Revision: 1, Rate: 60000, Period: time.Minute, Burst: 12000}}
	// Publish from an application-owned monotonic configuration source.
	// Admission callers need only names and costs, not policy snapshots.
	if err := limiter.UpdateQuotas(ctx, rpm, tokens); err != nil {
		log.Fatal(err)
	}
	d, err := limiter.Allow(ctx, pacelock.Charge{Name: rpm.Name, Cost: 1}, pacelock.Charge{Name: tokens.Name, Cost: 1500})
	if err != nil {
		// Unknown may mean the debit committed. Do not blindly retry: every call
		// is a fresh admission and can debit again.
		log.Printf("admission failed: outcome=%s error=%v", pacelock.OutcomeOf(err), err)
		return
	}
	log.Printf("allowed=%t limited=%v retry_after=%s", d.Allowed, d.Limited, d.RetryAfter)
}
