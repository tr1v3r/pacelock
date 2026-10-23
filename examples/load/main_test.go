package main

import (
	"context"
	"github.com/tr1v3r/pacelock"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestConcurrentMeasurementAccounting(t *testing.T) {
	m := &metrics{random: rand.New(rand.NewSource(1))}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				m.add(pacelock.Decision{Allowed: true}, false, false, nil, time.Millisecond)
				m.add(pacelock.Decision{}, false, false, context.DeadlineExceeded, time.Second)
			}
		}()
	}
	wg.Wait()
	if m.Calls != 320000 || m.Allowed != 160000 || m.Errors != 160000 || m.ContextErrors != 160000 || m.Unknown != 160000 || len(m.Samples) != 100000 {
		t.Fatalf("incorrect totals: calls=%d allowed=%d errors=%d samples=%d", m.Calls, m.Allowed, m.Errors, len(m.Samples))
	}
	for _, sample := range m.Samples {
		if sample != 1000 && sample != 1000000 {
			t.Fatal("invalid reservoir value")
		}
	}
}
