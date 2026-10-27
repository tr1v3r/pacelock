package pacelock

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkBoundAdmission isolates facade overhead with an always-allow driver;
// it does not measure backend throughput or persistence cost.
func BenchmarkBoundAdmission(b *testing.B) {
	for _, n := range []int{1, 4, MaxRules} {
		for _, mode := range []string{"rules", "charges"} {
			b.Run(fmt.Sprintf("%s/quotas=%d", mode, n), func(b *testing.B) {
				driver := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
				l, _ := New(driver, "benchmark", "bound")
				quotas := make([]Quota, n)
				charges := make([]Charge, n)
				rules := make([]Rule, n)
				for i := range quotas {
					name := fmt.Sprint(i)
					quotas[i] = Quota{Name: name, Policy: Policy{Rate: 1000, Period: time.Second, Burst: 1000}}
					charges[n-1-i] = Charge{Name: name, Cost: 1}
					rules[i] = Rule{Name: name, Rate: 1000, Period: time.Second, Burst: 1000, Cost: 1}
				}
				bound, err := l.Bind(quotas...)
				if err != nil {
					b.Fatal(err)
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var d Decision
					var err error
					if mode == "rules" {
						d, err = l.Allow(ctx, rules...)
					} else {
						d, err = bound.Allow(ctx, charges...)
					}
					if err != nil || !d.Allowed {
						b.Fatalf("%+v %v", d, err)
					}
				}
			})
		}
	}
}
