package pacelock

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

// waitDriver scripts attempts while retaining the ordinary test driver's lookup.
type waitDriver struct {
	testDriver
	admit func(context.Context, Admission) (Receipt, error)
}

func (d *waitDriver) Admit(ctx context.Context, a Admission) (Receipt, error) {
	d.calls++
	return d.admit(ctx, a)
}

func TestWaitRetriesOnlyDenials(t *testing.T) {
	rules := []Rule{sampleRule(), {Name: "tokens", Rate: 10, Period: time.Second, Burst: 20, Cost: 7}}
	original := append([]Rule(nil), rules...)
	var lastAttempt time.Time
	const delay = 5 * time.Millisecond
	d := &waitDriver{}
	d.admit = func(_ context.Context, a Admission) (Receipt, error) {
		if !reflect.DeepEqual(a.Rules, original) || a.ID != "" || a.Retention != 0 || a.Scope != (Scope{"a", "b"}) {
			t.Fatalf("changed admission: %+v", a)
		}
		if !lastAttempt.IsZero() && time.Since(lastAttempt) < delay {
			t.Fatal("retried before RetryAfter")
		}
		lastAttempt = time.Now()
		// Changing the source slice between attempts must not change the wait.
		rules[0].Cost = 5
		if d.calls < 3 {
			return Receipt{Decision: Decision{RetryAfter: delay, Limited: []string{"tokens"}}}, nil
		}
		return Receipt{Decision: Decision{Allowed: true}}, nil
	}
	var events []Event
	l, _ := NewWithOptions(d, "a", "b", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	if err := l.Wait(context.Background(), rules...); err != nil {
		t.Fatal(err)
	}
	if d.calls != 3 || len(events) != 1 || events[0].Operation != "wait" || events[0].Result != "allowed" || events[0].Replayed || events[0].RuleCount != 2 || events[0].RetryAfter != 0 || events[0].Duration < 2*delay {
		t.Fatal(d.calls, events)
	}
}

func TestWaitValidation(t *testing.T) {
	d := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	l, _ := New(d, "a", "b")
	var nilLimiter *Limiter
	for name, call := range map[string]func() error{
		"nil receiver":    func() error { return nilLimiter.Wait(context.Background(), sampleRule()) },
		"zero receiver":   func() error { return (&Limiter{}).Wait(context.Background(), sampleRule()) },
		"nil context":     func() error { return l.Wait(nil, sampleRule()) },
		"no rules":        func() error { return l.Wait(context.Background()) },
		"duplicate rules": func() error { return l.Wait(context.Background(), sampleRule(), sampleRule()) },
		"invalid cost":    func() error { r := sampleRule(); r.Cost = r.Burst + 1; return l.Wait(context.Background(), r) },
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			var typed *Error
			if !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged || !errors.As(err, &typed) || typed.Op != "wait" {
				t.Fatal(err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Wait(ctx, sampleRule()); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if d.calls != 0 {
		t.Fatal("invalid request reached driver", d.calls)
	}
}

func TestWaitStopsOnEveryError(t *testing.T) {
	for _, backendErr := range []error{
		io.ErrUnexpectedEOF,
		&Error{Outcome: NotCharged, Err: ErrPolicyMismatch},
		&Error{Outcome: NotCharged, Err: ErrBackend},
		&Error{Outcome: Unknown, Err: context.Canceled},
		&Error{Outcome: Unknown, Err: ErrOutcomeUnknown},
	} {
		t.Run(backendErr.Error(), func(t *testing.T) {
			d := &waitDriver{}
			d.admit = func(context.Context, Admission) (Receipt, error) {
				if d.calls == 1 {
					return Receipt{Decision: Decision{RetryAfter: time.Microsecond, Limited: []string{"q"}}}, nil
				}
				return Receipt{}, backendErr
			}
			l, _ := New(d, "a", "b")
			err := l.Wait(context.Background(), sampleRule())
			var typed *Error
			if !errors.Is(err, backendErr) || OutcomeOf(err) != OutcomeOf(backendErr) || !errors.As(err, &typed) || typed.Op != "wait" || d.calls != 2 {
				t.Fatal(d.calls, err)
			}
		})
	}
	for _, result := range []Receipt{
		{},
		{Decision: Decision{Allowed: true}, Replayed: true},
		{Decision: Decision{RetryAfter: -time.Second, Limited: []string{"q"}}},
		{Decision: Decision{RetryAfter: time.Second, Limited: []string{"other"}}},
	} {
		d := &testDriver{result: result}
		l, _ := New(d, "a", "b")
		if err := l.Wait(context.Background(), sampleRule()); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown || d.calls != 1 {
			t.Fatal(result, err, d.calls)
		}
	}
}

func TestWaitCancellationDuringDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &waitDriver{admit: func(context.Context, Admission) (Receipt, error) {
		cancel()
		return Receipt{Decision: Decision{RetryAfter: time.Hour, Limited: []string{"q"}}}, nil
	}}
	var event Event
	l, _ := NewWithOptions(d, "a", "b", Options{Observer: func(_ context.Context, e Event) { event = e }})
	err := l.Wait(ctx, sampleRule())
	if !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged || d.calls != 1 || event.Operation != "wait" || event.Result != "error" || event.Err != err {
		t.Fatal(d.calls, err, event)
	}
}

func TestWaitDeadlineDuringDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	d := &testDriver{result: Receipt{Decision: Decision{RetryAfter: time.Hour, Limited: []string{"q"}}}}
	l, _ := New(d, "a", "b")
	if err := l.Wait(ctx, sampleRule()); !errors.Is(err, context.DeadlineExceeded) || OutcomeOf(err) != NotCharged || d.calls > 1 {
		t.Fatal(d.calls, err)
	}
}

func TestWaitPreservesSuccessDespiteCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &waitDriver{admit: func(context.Context, Admission) (Receipt, error) {
		cancel() // A confirmed debit must not be replaced with NotCharged.
		return Receipt{Decision: Decision{Allowed: true}}, nil
	}}
	l, _ := New(d, "a", "b")
	if err := l.Wait(ctx, sampleRule()); err != nil || d.calls != 1 {
		t.Fatal(d.calls, err)
	}
}
