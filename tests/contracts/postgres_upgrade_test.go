package contracts_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/tr1v3r/pacelock"
	"github.com/tr1v3r/pacelock/driver/postgres"
)

// Exercise the actual pre-managed three-table schema, not tables already created
// by the current Migrate. A private schema keeps this regression independent of
// other contract tests and of the caller's default schema.
func TestPostgresLegacySchemaUpgrade(t *testing.T) {
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
	schemaName := "pacelock_upgrade_" + pgItemID(freshDomain())[:16]
	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	if _, err = admin.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("remove isolated upgrade schema: %v", err)
		}
	}()
	config.RuntimeParams["search_path"] = schemaName
	db := stdlib.OpenDB(*config)
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE pacelock_domains (scope_id text PRIMARY KEY)`,
		`CREATE TABLE pacelock_quotas (
			scope_id text NOT NULL REFERENCES pacelock_domains(scope_id),
			quota_id text NOT NULL,
			tat_us bigint NOT NULL CHECK (tat_us >= 0),
			interval_us bigint NOT NULL CHECK (interval_us > 0),
			burst bigint NOT NULL CHECK (burst > 0),
			expires_us bigint NOT NULL,
			PRIMARY KEY (scope_id, quota_id))`,
		`CREATE TABLE pacelock_admissions (
			scope_id text NOT NULL REFERENCES pacelock_domains(scope_id),
			admission_id text NOT NULL,
			fingerprint text NOT NULL,
			decision bytea NOT NULL,
			expires_us bigint NOT NULL,
			PRIMARY KEY (scope_id, admission_id))`,
	} {
		if _, err = db.ExecContext(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	scope := pacelock.Scope{Namespace: "upgrade-tests", Domain: freshDomain()}
	rule := pacelock.Rule{Name: "requests", Policy: pacelock.Policy{Rate: 1, Period: time.Hour, Burst: 10}, Cost: 1}
	a := pacelock.StaticAdmission{Scope: scope, Rules: []pacelock.Rule{rule}, ID: "legacy-allow", Retention: time.Hour}
	var now int64
	if err = db.QueryRowContext(ctx, `SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	original := pacelock.QuotaState{TAT: now + (10 * time.Minute).Microseconds(), Interval: time.Hour.Microseconds(), Burst: 10, ExpiresAt: now + (10 * time.Minute).Microseconds()}
	raw, err := json.Marshal(pacelock.Decision{Allowed: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO pacelock_domains VALUES($1)`, pgScopeID(scope)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO pacelock_quotas VALUES($1,$2,$3,$4,$5,$6)`, pgScopeID(scope), pgItemID(rule.Name), original.TAT, original.Interval, original.Burst, original.ExpiresAt); err != nil {
		t.Fatal(err)
	}
	expires := now + time.Hour.Microseconds()
	if _, err = db.ExecContext(ctx, `INSERT INTO pacelock_admissions VALUES($1,$2,$3,$4,$5)`, pgScopeID(scope), pgItemID(a.ID), a.Fingerprint(), raw, expires); err != nil {
		t.Fatal(err)
	}
	d, err := postgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	readDebt := func() pacelock.QuotaState {
		t.Helper()
		var s pacelock.QuotaState
		if err := db.QueryRowContext(ctx, `SELECT tat_us,interval_us,burst,expires_us FROM pacelock_quotas WHERE scope_id=$1 AND quota_id=$2`, pgScopeID(scope), pgItemID(rule.Name)).Scan(&s.TAT, &s.Interval, &s.Burst, &s.ExpiresAt); err != nil {
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
		var columns string
		if err = db.QueryRowContext(ctx, `SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema=$1 AND table_name='pacelock_quotas'`, schemaName).Scan(&columns); err != nil {
			t.Fatal(err)
		}
		if columns != "scope_id,quota_id,tat_us,interval_us,burst,expires_us" {
			t.Fatalf("legacy columns changed: %s", columns)
		}
		var savedFingerprint string
		var savedRaw []byte
		var savedExpiry int64
		if err = db.QueryRowContext(ctx, `SELECT fingerprint,decision,expires_us FROM pacelock_admissions WHERE scope_id=$1 AND admission_id=$2`, pgScopeID(scope), pgItemID(a.ID)).Scan(&savedFingerprint, &savedRaw, &savedExpiry); err != nil {
			t.Fatal(err)
		}
		if savedFingerprint != a.Fingerprint() || string(savedRaw) != string(raw) || savedExpiry != expires {
			t.Fatal("migration changed retained receipt")
		}
	}
	l, err := newContractLimiter(d, scope.Namespace, scope.Domain)
	if err != nil {
		t.Fatal(err)
	}
	replay := func() {
		t.Helper()
		r, err := l.allowRulesOnce(ctx, a.ID, a.Retention, rule)
		if err != nil || !r.Allowed || !r.Replayed {
			t.Fatalf("legacy replay: %+v %v", r, err)
		}
	}
	replay()
	if decision, err := l.allowRules(ctx, rule); err != nil || !decision.Allowed {
		t.Fatalf("static admission after migration: %+v %v", decision, err)
	}
	beforeUpdate := readDebt()
	q := pacelock.Quota{Name: rule.Name, Policy: pacelock.Policy{Revision: 1, Rate: 2, Period: time.Hour, Burst: 10}}
	if err = l.UpdateQuotas(ctx, q); err != nil {
		t.Fatal(err)
	}
	if got := readDebt(); got != beforeUpdate {
		t.Fatalf("activation changed debt: %+v, want %+v", got, beforeUpdate)
	}
	if _, err = l.allowRules(ctx, rule); !errors.Is(err, pacelock.ErrPolicyMismatch) {
		t.Fatalf("static bypass after activation: %v", err)
	}
	if decision, err := l.Allow(ctx, pacelock.Charge{Name: q.Name, Cost: 1}); err != nil || !decision.Allowed {
		t.Fatalf("managed admission after activation: %+v %v", decision, err)
	}
	replay()
}
