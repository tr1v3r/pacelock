package pacelock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWaitBackoffBounds(t *testing.T) {
	for _, tc := range []struct{ previous, retry, limit, want time.Duration }{
		{0, time.Second, 0, 0}, {0, time.Second, 10 * time.Second, time.Second},
		{0, MaxHorizon, time.Second, time.Second}, {time.Second, time.Second, 10 * time.Second, 2 * time.Second},
		{8 * time.Second, time.Second, 10 * time.Second, 10 * time.Second},
		{MaxHorizon, MaxHorizon, MaxHorizon, MaxHorizon},
	} {
		if got := growWaitBackoff(tc.previous, tc.retry, tc.limit); got != tc.want {
			t.Fatalf("%+v got %v", tc, got)
		}
	}
	for i := 0; i < 1000; i++ {
		if got := waitDelay(MaxHorizon, MaxHorizon); got < MaxHorizon || got > 2*MaxHorizon {
			t.Fatal(got)
		}
		if got := waitDelay(time.Second, 0); got != time.Second {
			t.Fatal(got)
		}
	}
	window := time.Duration(0)
	for i := 0; i < 10000; i++ {
		window = growWaitBackoff(window, time.Nanosecond, MaxHorizon)
	}
	if window != MaxHorizon {
		t.Fatal(window)
	}
	for _, invalid := range []time.Duration{-1, MaxHorizon + 1} {
		if _, err := NewWithOptions(&dynamicWaitDriver{}, "n", "d", Options{WaitBackoff: invalid}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}

func TestWaitBackoffStopsOnErrorAndCancellation(t *testing.T) {
	for _, outcome := range []Outcome{NotCharged, Unknown} {
		expected := &Error{Outcome: outcome, Err: ErrBackend}
		calls := 0
		d := &dynamicWaitDriver{admit: func(context.Context, Admission) (Decision, error) { calls++; return Decision{}, expected }}
		l, _ := NewWithOptions(d, "n", "d", Options{WaitBackoff: MaxHorizon})
		err := l.Wait(context.Background(), Use("q", 1))
		if calls != 1 || !errors.Is(err, expected) || OutcomeOf(err) != outcome {
			t.Fatalf("calls=%d err=%v", calls, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	d := &dynamicWaitDriver{admit: func(context.Context, Admission) (Decision, error) {
		calls++
		cancel()
		return Decision{RetryAfter: MaxHorizon, Limited: []string{"q"}}, nil
	}}
	l, _ := NewWithOptions(d, "n", "d", Options{WaitBackoff: MaxHorizon})
	done := make(chan error, 1)
	go func() { done <- l.Wait(ctx, Use("q", 1)) }()
	select {
	case err := <-done:
		if calls != 1 || !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
			t.Fatal(calls, err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation blocked by backoff")
	}
}

// contentionDriver uses the real GCRA calculation, with in-process atomicity so
// the benchmark measures retry pressure without transport latency or an external server.
type contentionDriver struct {
	sync.Mutex
	state QuotaState
	calls int
}

func (d *contentionDriver) Admit(_ context.Context, a Admission) (Decision, error) {
	d.Lock()
	defer d.Unlock()
	d.calls++
	decision, next, err := EvaluateManaged(time.Now().UnixMicro(), a.Charges, []QuotaState{{Interval: 1000, Burst: 1, Revision: 1}}, []QuotaState{d.state})
	if err == nil && decision.Allowed {
		d.state = next[0]
	}
	return decision, err
}
func BenchmarkWaitContention(b *testing.B) {
	for _, tc := range []struct {
		name    string
		backoff time.Duration
	}{{"exact-retry", 0}, {"backoff-250ms", 250 * time.Millisecond}} {
		b.Run(tc.name, func(b *testing.B) {
			const callers = 100
			total := 0
			for i := 0; i < b.N; i++ {
				d := &contentionDriver{}
				l, _ := NewWithOptions(d, "benchmark", "hot", Options{WaitBackoff: tc.backoff})
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				var wg sync.WaitGroup
				start := make(chan struct{})
				errs := make(chan error, callers)
				for j := 0; j < callers; j++ {
					wg.Add(1)
					go func() { defer wg.Done(); <-start; errs <- l.Wait(ctx, Use("q", 1)) }()
				}
				close(start)
				wg.Wait()
				cancel()
				close(errs)
				for err := range errs {
					if err != nil {
						b.Fatal(err)
					}
				}
				total += d.calls
			}
			b.ReportMetric(float64(total)/float64(b.N), "attempts/batch")
			b.ReportMetric(float64(total)/float64(b.N*callers), "attempts/admit")
		})
	}
}
