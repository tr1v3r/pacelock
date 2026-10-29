package pacelock

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkDynamicAdmission isolates facade overhead with an always-allow driver;
// it does not measure backend throughput or persistence cost.
func BenchmarkDynamicAdmission(b *testing.B) {
	for _, n := range []int{1, 4, MaxRules} {
		b.Run(fmt.Sprintf("quotas=%d", n), func(b *testing.B) {
			driver := &testDriver{result: Decision{Allowed: true}}
			charges := make([]Charge, n)
			for i := range charges {
				name := fmt.Sprint(i)
				charges[n-1-i] = Charge{Name: name, Cost: 1}
			}
			limiter, err := New(driver, "benchmark", "dynamic")
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
