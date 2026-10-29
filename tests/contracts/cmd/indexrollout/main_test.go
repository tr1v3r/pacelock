package main

import (
	"testing"
	"time"
)

func TestLatencyDistribution(t *testing.T) {
	if got := dist(nil); got != (latency{}) {
		t.Fatal(got)
	}
	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = time.Duration(100-i) * time.Millisecond
	}
	got := dist(samples)
	// Quantiles use floor((n-1)*q), matching the documented empirical convention.
	if got.Count != 100 || got.P50ms != 50 || got.P95ms != 95 || got.P99ms != 99 || got.Maxms != 100 {
		t.Fatal(got)
	}
}
