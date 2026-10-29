package contracts_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/tr1v3r/pacelock"
	"github.com/tr1v3r/pacelock/driver/postgres"
)

// Repeated schema initialization must preserve current state and unrelated data.
// A private schema keeps this independent of the caller's default schema.
func TestPostgresMigratePreservesCurrentState(t *testing.T) {
	dsn := os.Getenv("PACELOCK_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set PACELOCK_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*config)
	defer admin.Close()
	schemaName := "pacelock_migrate_" + pgItemID(freshDomain())[:16]
	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	if _, err = admin.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("remove isolated migration schema: %v", err)
		}
	}()
	config.RuntimeParams["search_path"] = schemaName
	db := stdlib.OpenDB(*config)
	defer db.Close()
	d, err := postgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `CREATE TABLE application_settings (name text PRIMARY KEY, value text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO application_settings VALUES ('theme', 'dark')`); err != nil {
		t.Fatal(err)
	}
	scope := pacelock.Scope{Namespace: "migrate-tests", Domain: freshDomain()}
	l, err := newContractLimiter(d, scope.Namespace, scope.Domain)
	if err != nil {
		t.Fatal(err)
	}
	configured := pacelock.Quota{Name: "configured", Policy: pacelock.Policy{Revision: 7, Rate: 2, Period: time.Hour, Burst: 10}}
	if err = l.UpdateQuotas(ctx, configured); err != nil {
		t.Fatal(err)
	}
	var now int64
	if err = db.QueryRowContext(ctx, `SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	original := pacelock.QuotaState{TAT: now + time.Hour.Microseconds(), Interval: time.Hour.Microseconds(), Burst: 1, ExpiresAt: now + time.Hour.Microseconds()}
	if _, err = db.ExecContext(ctx, `INSERT INTO pacelock_quotas VALUES($1,$2,$3,$4,$5,$6)`, pgScopeID(scope), pgItemID("debt-only"), original.TAT, original.Interval, original.Burst, original.ExpiresAt); err != nil {
		t.Fatal(err)
	}
	readDebt := func() pacelock.QuotaState {
		t.Helper()
		var s pacelock.QuotaState
		if err := db.QueryRowContext(ctx, `SELECT tat_us,interval_us,burst,expires_us FROM pacelock_quotas WHERE scope_id=$1 AND quota_id=$2`, pgScopeID(scope), pgItemID("debt-only")).Scan(&s.TAT, &s.Interval, &s.Burst, &s.ExpiresAt); err != nil {
			t.Fatal(err)
		}
		return s
	}
	for i := 0; i < 2; i++ {
		if err = d.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if got := readDebt(); got != original {
			t.Fatalf("migration %d changed debt: %+v, want %+v", i, got, original)
		}
		var policy pacelock.QuotaState
		if err = db.QueryRowContext(ctx, `SELECT interval_us,burst,revision FROM pacelock_policies WHERE scope_id=$1 AND quota_id=$2`, pgScopeID(scope), pgItemID(configured.Name)).Scan(&policy.Interval, &policy.Burst, &policy.Revision); err != nil {
			t.Fatal(err)
		}
		wantPolicy := pacelock.QuotaState{Interval: (time.Hour / 2).Microseconds(), Burst: 10, Revision: 7}
		if policy != wantPolicy {
			t.Fatalf("migration %d changed policy: %+v, want %+v", i, policy, wantPolicy)
		}
		var value string
		if err = db.QueryRowContext(ctx, `SELECT value FROM application_settings WHERE name='theme'`).Scan(&value); err != nil || value != "dark" {
			t.Fatalf("migration %d changed unrelated data: %q %v", i, value, err)
		}
	}
	charge := pacelock.Charge{Name: "debt-only", Cost: 1}
	if _, err := l.Allow(ctx, charge); !errors.Is(err, pacelock.ErrPolicyMissing) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatalf("debt must not imply policy authority: %v", err)
	}
	q := pacelock.Quota{Name: charge.Name, Policy: pacelock.Policy{Revision: 1, Rate: 1, Period: time.Hour, Burst: 1}}
	if err = l.UpdateQuotas(ctx, q); err != nil {
		t.Fatal(err)
	}
	if got := readDebt(); got != original {
		t.Fatalf("publication changed debt: %+v, want %+v", got, original)
	}
	if decision, err := l.Allow(ctx, charge); err != nil || decision.Allowed || decision.RetryAfter < 50*time.Minute {
		t.Fatalf("current debt lost: %+v %v", decision, err)
	}
}
