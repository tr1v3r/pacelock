package contracts_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tr1v3r/pacelock"
	"github.com/tr1v3r/pacelock/driver/postgres"
)

// Exercise SQL statement atomicity with a real server rejecting the final row.
// The dedicated schema keeps fault injection isolated from every other test.
func TestPostgresBatchWriteRollback(t *testing.T) {
	dsn := os.Getenv("PACELOCK_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set PACELOCK_POSTGRES_DSN")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := fmt.Sprintf("batch_test_%d", time.Now().UnixNano())
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE SCHEMA " + schema)
	defer func() {
		if _, err := db.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	exec("SET search_path TO " + schema)
	d, err := postgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	l, err := pacelock.New(d, "batch-rollback", "domain")
	if err != nil {
		t.Fatal(err)
	}
	quotas := make([]pacelock.Quota, pacelock.MaxRules)
	charges := make([]pacelock.Charge, len(quotas))
	for i := range quotas {
		name := fmt.Sprintf("quota-%02d", i)
		quotas[i] = pacelock.Quota{Name: name, Policy: pacelock.Policy{Revision: 1, Rate: 1, Period: time.Hour, Burst: 1}}
		charges[i] = pacelock.Use(name, 1)
	}
	lastID := fmt.Sprintf("%x", sha256.Sum256([]byte(quotas[len(quotas)-1].Name)))
	count := func(table string, want int) {
		t.Helper()
		var got int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s rows=%d, want %d", table, got, want)
		}
	}
	exec("ALTER TABLE pacelock_policies ADD CONSTRAINT reject_last CHECK (quota_id <> '" + lastID + "')")
	err = l.UpdateQuotas(ctx, quotas...)
	if !errors.Is(err, pacelock.ErrBackend) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatalf("policy write: %v", err)
	}
	count("pacelock_policies", 0)
	exec("ALTER TABLE pacelock_policies DROP CONSTRAINT reject_last")
	if err := l.UpdateQuotas(ctx, quotas...); err != nil {
		t.Fatal(err)
	}
	count("pacelock_policies", pacelock.MaxRules)
	exec("ALTER TABLE pacelock_quotas ADD CONSTRAINT reject_last CHECK (quota_id <> '" + lastID + "')")
	_, err = l.Allow(ctx, charges...)
	if !errors.Is(err, pacelock.ErrBackend) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatalf("debt write: %v", err)
	}
	count("pacelock_quotas", 0)
	exec("ALTER TABLE pacelock_quotas DROP CONSTRAINT reject_last")
	if result, err := l.Allow(ctx, charges...); err != nil || !result.Allowed {
		t.Fatalf("after rollback: %+v %v", result, err)
	}
	count("pacelock_quotas", pacelock.MaxRules)
	// A repeated admission must report blockers in caller order, not SQL row order.
	for i, j := 0, len(charges)-1; i < j; i, j = i+1, j-1 {
		charges[i], charges[j] = charges[j], charges[i]
	}
	result, err := l.Allow(ctx, charges...)
	if err != nil || result.Allowed || len(result.Limited) != len(charges) {
		t.Fatalf("denial: %+v %v", result, err)
	}
	for i, c := range charges {
		if result.Limited[i] != c.Name {
			t.Fatalf("blocker %d=%q, want %q", i, result.Limited[i], c.Name)
		}
	}
}
