package contracts_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tr1v3r/pacelock"
	"github.com/tr1v3r/pacelock/driver/postgres"
)

// BenchmarkPostgresAdmission measures sequential, uncontended admissions over a
// real transport. It includes transaction/lock/time overhead, not policy setup.
// Use a disposable database; each case has a unique namespace.
func BenchmarkPostgresAdmission(b *testing.B) {
	dsn := os.Getenv("PACELOCK_POSTGRES_DSN")
	if dsn == "" {
		b.Skip("set PACELOCK_POSTGRES_DSN")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	d, err := postgres.New(db)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Migrate(ctx); err != nil {
		b.Fatal(err)
	}
	for _, n := range []int{1, 4, pacelock.MaxRules} {
		for _, allowed := range []bool{true, false} {
			b.Run(fmt.Sprintf("quotas=%d/allowed=%t", n, allowed), func(b *testing.B) {
				l, err := pacelock.New(d, fmt.Sprintf("pg-bench-%d", time.Now().UnixNano()), "domain")
				if err != nil {
					b.Fatal(err)
				}
				quotas := make([]pacelock.Quota, n)
				charges := make([]pacelock.Charge, n)
				for i := range quotas {
					name := fmt.Sprintf("quota-%02d", n-i)
					quotas[i] = pacelock.Quota{Name: name, Policy: pacelock.Policy{Revision: 1, Rate: 1000000, Period: time.Second, Burst: 1000000}}
					charges[i] = pacelock.Use(name, 1)
				}
				if !allowed {
					quotas[n-1].Policy = pacelock.Policy{Revision: 1, Rate: 1, Period: 24 * time.Hour, Burst: 1}
				}
				if err := l.UpdateQuotas(ctx, quotas...); err != nil {
					b.Fatal(err)
				}
				if result, err := l.Allow(ctx, charges...); err != nil || !result.Allowed {
					b.Fatalf("warm-up: %+v %v", result, err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := l.Allow(ctx, charges...)
					if err != nil || result.Allowed != allowed {
						b.Fatalf("admission: %+v %v", result, err)
					}
				}
			})
		}
	}
}
