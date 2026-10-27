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
	"github.com/tr1v3r/pacelock/drivers/postgres"
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
	d, err := limiter.Allow(ctx,
		pacelock.Rule{Name: "requests", Rate: 60, Period: time.Minute, Burst: 10, Cost: 1},
		pacelock.Rule{Name: "input-tokens", Rate: 60000, Period: time.Minute, Burst: 12000, Cost: 1500},
	)
	if err != nil {
		log.Printf("admission failed: outcome=%s error=%v", pacelock.OutcomeOf(err), err)
		return
	}
	log.Printf("allowed=%t limited=%v retry_after=%s", d.Allowed, d.Limited, d.RetryAfter)
}
