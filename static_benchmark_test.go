package pacelock

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkStaticAdmission isolates facade overhead with an always-allow driver;
// it does not measure backend throughput or persistence cost.
func BenchmarkStaticAdmission(b *testing.B) {
	for _, n := range []int{1, 4, MaxRules} {
		b.Run(fmt.Sprintf("quotas=%d", n), func(b *testing.B) {
			driver := &testDriver{result: Decision{Allowed: true}}
			quotas := make([]Quota, n)
			charges := make([]Charge, n)
			for i := range quotas {
				name := fmt.Sprint(i)
				quotas[i] = Quota{Name: name, Policy: Policy{Rate: 1000, Period: time.Second, Burst: 1000}}
				charges[n-1-i] = Charge{Name: name, Cost: 1}
			}
			limiter, err := NewStatic(driver, "benchmark", "static", quotas...)
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if d, err := limiter.Allow(ctx, charges...); err != nil || !d.Allowed {
					b.Fatalf("%+v %v", d, err)
				}
			}
		})
	}
}
