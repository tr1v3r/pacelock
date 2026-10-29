package redisdriver

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

// Observation belongs to the public limiter, including errors from the Redis driver.
type observationScripter struct {
	redis.Scripter
	reply interface{}
	err   error
	calls atomic.Int64
}

func (s *observationScripter) EvalSha(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	s.calls.Add(1)
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(s.reply)
	cmd.SetErr(s.err)
	return cmd
}

func TestPublicObserver(t *testing.T) {
	charge := pacelock.Charge{Name: "q", Cost: 1}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name       string
		ctx        context.Context
		charges    []pacelock.Charge
		reply      interface{}
		backendErr error
		wantErr    error
		outcome    pacelock.Outcome
		result     string
		calls      int64
	}{
		{name: "allowed", ctx: context.Background(), charges: []pacelock.Charge{charge}, reply: []interface{}{int64(1), int64(0), int64(0)}, result: "allowed", calls: 1},
		{name: "denied", ctx: context.Background(), charges: []pacelock.Charge{charge}, reply: []interface{}{int64(0), int64(5), int64(1)}, result: "denied", calls: 1},
		{name: "empty", ctx: context.Background(), wantErr: pacelock.ErrInvalid, outcome: pacelock.NotCharged, result: "error"},
		{name: "nil context", charges: []pacelock.Charge{charge}, wantErr: pacelock.ErrInvalid, outcome: pacelock.NotCharged, result: "error"},
		{name: "canceled", ctx: canceled, charges: []pacelock.Charge{charge}, wantErr: context.Canceled, outcome: pacelock.NotCharged, result: "error"},
		{name: "redis nil", ctx: context.Background(), charges: []pacelock.Charge{charge}, backendErr: redis.Nil, wantErr: pacelock.ErrBackend, outcome: pacelock.Unknown, result: "error", calls: 1},
		{name: "lost reply", ctx: context.Background(), charges: []pacelock.Charge{charge}, backendErr: io.ErrUnexpectedEOF, wantErr: pacelock.ErrBackend, outcome: pacelock.Unknown, result: "error", calls: 1},
		{name: "malformed reply", ctx: context.Background(), charges: []pacelock.Charge{charge}, reply: int64(1), wantErr: pacelock.ErrState, outcome: pacelock.Unknown, result: "error", calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &observationScripter{reply: tc.reply, err: tc.backendErr}
			driver, err := New(s)
			if err != nil {
				t.Fatal(err)
			}
			var events []pacelock.Event
			l, err := pacelock.NewWithOptions(driver, "test", "observer", pacelock.Options{Observer: func(ctx context.Context, e pacelock.Event) {
				if ctx == nil {
					t.Error("nil observer context")
				}
				if tc.ctx != nil && ctx != tc.ctx {
					t.Error("observer lost caller context")
				}
				events = append(events, e)
			}})
			if err != nil {
				t.Fatal(err)
			}
			d, err := l.Allow(tc.ctx, tc.charges...)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
			if err != nil && pacelock.OutcomeOf(err) != tc.outcome {
				t.Fatalf("outcome: %v", err)
			}
			if tc.backendErr != nil && !errors.Is(err, tc.backendErr) {
				t.Fatalf("lost backend error: %v", err)
			}
			if s.calls.Load() != tc.calls {
				t.Fatalf("dispatches=%d want %d", s.calls.Load(), tc.calls)
			}
			if len(events) != 1 {
				t.Fatalf("events=%+v", events)
			}
			e := events[0]
			if e.Operation != "allow" || e.Result != tc.result || e.RuleCount != len(tc.charges) || e.Duration < 0 || e.Err != err || e.RetryAfter != d.RetryAfter {
				t.Fatalf("event=%+v decision=%+v err=%v", e, d, err)
			}
			if err == nil && d.Allowed != (tc.result == "allowed") {
				t.Fatal(d)
			}
		})
	}
}

func TestPublicObserverConcurrentCancellation(t *testing.T) {
	s := &observationScripter{reply: []interface{}{int64(1), int64(0), int64(0)}}
	driver, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	events := make(map[string]int)
	l, err := pacelock.NewWithOptions(driver, "test", "observer", pacelock.Options{Observer: func(ctx context.Context, e pacelock.Event) {
		mu.Lock()
		defer mu.Unlock()
		events[e.Operation+"/"+e.Result]++
		if ctx == nil || e.Duration < 0 {
			t.Error("invalid observation")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 600; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			charges := []pacelock.Charge{{Name: "q", Cost: 1}}
			var want error
			switch i % 6 {
			case 1, 5:
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx, want = canceled, context.Canceled
			case 2:
				expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				ctx, want = expired, context.DeadlineExceeded
			case 3:
				charges = append(charges, charges[0])
				want = pacelock.ErrInvalid
			}
			d, err := l.Allow(ctx, charges...)
			if !errors.Is(err, want) || (err != nil && pacelock.OutcomeOf(err) != pacelock.NotCharged) || (err == nil && !d.Allowed) {
				t.Errorf("decision=%+v err=%v want=%v", d, err, want)
			}
		}(i)
	}
	wg.Wait()
	if len(events) != 2 || events["allow/allowed"] != 200 || events["allow/error"] != 400 || s.calls.Load() != 200 {
		t.Fatalf("events=%v dispatches=%d", events, s.calls.Load())
	}
}
