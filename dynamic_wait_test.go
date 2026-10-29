package pacelock

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

// dynamicWaitDriver scripts dynamic admissions. Inherited static and Lookup
// methods share its dispatch counter, exposing accidental extra calls.
type dynamicWaitDriver struct {
	testDriver
	admit func(context.Context, Admission) (Receipt, error)
}

func (d *dynamicWaitDriver) Admit(ctx context.Context, a Admission) (Receipt, error) {
	d.calls++
	return d.admit(ctx, a)
}

func TestDynamicWaitSnapshotsChargesAndRetriesOnlyDenials(t *testing.T) {
	charges := []Charge{{Name: "q", Cost: 1}, {Name: "tokens", Cost: 7}}
	original := append([]Charge(nil), charges...)
	const delay = 5 * time.Millisecond
	var lastAttempt time.Time
	d := &dynamicWaitDriver{}
	d.admit = func(_ context.Context, a Admission) (Receipt, error) {
		if !reflect.DeepEqual(a.Charges, original) || a.ID != "" || a.Retention != 0 || a.Scope != (Scope{"app", "domain"}) {
			t.Fatalf("changed admission: %+v", a)
		}
		if !lastAttempt.IsZero() && time.Since(lastAttempt) < delay {
			t.Fatal("retried before RetryAfter")
		}
		lastAttempt = time.Now()
		// A synchronized caller mutation after dispatch must not change retries.
		charges[0] = Charge{Name: "other", Cost: 99}
		if d.calls < 3 {
			return Receipt{Decision: Decision{RetryAfter: delay, Limited: []string{"tokens"}}}, nil
		}
		return Receipt{Decision: Decision{Allowed: true}}, nil
	}
	var events []Event
	l, err := NewWithOptions(d, "app", "domain", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Wait(context.Background(), charges...); err != nil {
		t.Fatal(err)
	}
	if d.calls != 3 || len(events) != 1 {
		t.Fatal(d.calls, events)
	}
	e := events[0]
	if e.Operation != "wait" || e.Result != "allowed" || e.Replayed || e.RuleCount != 2 || e.RetryAfter != 0 || e.Err != nil || e.Duration < 2*delay {
		t.Fatal(e)
	}
}

func TestDynamicWaitValidationBeforeDispatch(t *testing.T) {
	d := &dynamicOnlyDriver{}
	l, err := New(d, "app", "domain")
	if err != nil {
		t.Fatal(err)
	}
	var nilLimiter *Limiter
	for name, call := range map[string]func() error{
		"nil receiver":  func() error { return nilLimiter.Wait(context.Background(), sampleCharge()) },
		"zero receiver": func() error { return (&Limiter{}).Wait(context.Background(), sampleCharge()) },
		"nil context":   func() error { return l.Wait(nil, sampleCharge()) },
		"no charges":    func() error { return l.Wait(context.Background()) },
		"duplicate":     func() error { return l.Wait(context.Background(), sampleCharge(), sampleCharge()) },
		"empty name":    func() error { return l.Wait(context.Background(), Charge{Cost: 1}) },
		"zero cost":     func() error { return l.Wait(context.Background(), Charge{Name: "q"}) },
		"negative cost": func() error {
			return l.Wait(context.Background(), Charge{Name: "q", Cost: -1})
		},
		"too many": func() error {
			return l.Wait(context.Background(), make([]Charge, MaxRules+1)...)
		},
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
	if err := l.Wait(ctx, sampleCharge()); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if d.calls != 0 {
		t.Fatal("invalid request reached driver", d.calls)
	}
	// Dynamic Wait must not require the optional StaticDriver or QuotaDriver.
	if err := l.Wait(context.Background(), sampleCharge()); err != nil || d.calls != 1 {
		t.Fatal(d.calls, err)
	}
}

func TestDynamicWaitStopsOnEveryError(t *testing.T) {
	for _, backendErr := range []error{
		io.ErrUnexpectedEOF,
		&Error{Outcome: NotCharged, Err: ErrPolicyMissing},
		&Error{Outcome: NotCharged, Err: ErrPaused},
		&Error{Outcome: NotCharged, Err: ErrInvalid},
		&Error{Outcome: NotCharged, Err: ErrUnsupported},
		&Error{Outcome: NotCharged, Err: ErrBackend},
		&Error{Outcome: NotCharged, Err: context.Canceled},
		&Error{Outcome: Unknown, Err: context.DeadlineExceeded},
		&Error{Outcome: Unknown, Err: ErrBackend},
		&Error{Outcome: Unknown, Err: ErrState},
	} {
		t.Run(backendErr.Error(), func(t *testing.T) {
			for _, denyFirst := range []bool{false, true} {
				d := &dynamicWaitDriver{}
				d.admit = func(context.Context, Admission) (Receipt, error) {
					if denyFirst && d.calls == 1 {
						return Receipt{Decision: Decision{RetryAfter: time.Microsecond, Limited: []string{"q"}}}, nil
					}
					return Receipt{}, backendErr
				}
				var events []Event
				l, _ := NewWithOptions(d, "app", "domain", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
				err := l.Wait(context.Background(), sampleCharge())
				wantCalls := 1
				if denyFirst {
					wantCalls++
				}
				var typed *Error
				if !errors.Is(err, backendErr) || OutcomeOf(err) != OutcomeOf(backendErr) || !errors.As(err, &typed) || typed.Op != "wait" || d.calls != wantCalls {
					t.Fatal(denyFirst, d.calls, err)
				}
				if len(events) != 1 || events[0].Operation != "wait" || events[0].Result != "error" || events[0].Replayed || events[0].Err != err {
					t.Fatal(events)
				}
				if denyFirst && events[0].RetryAfter != time.Microsecond {
					t.Fatal("lost last denial", events[0])
				}
			}
		})
	}
}

func TestDynamicWaitRejectsInvalidDriverResults(t *testing.T) {
	for _, result := range []Receipt{
		{},
		{Decision: Decision{Allowed: true}, Replayed: true},
		{Decision: Decision{Allowed: true, RetryAfter: time.Second}},
		{Decision: Decision{RetryAfter: -time.Second, Limited: []string{"q"}}},
		{Decision: Decision{RetryAfter: MaxHorizon + time.Second, Limited: []string{"q"}}},
		{Decision: Decision{RetryAfter: time.Second, Limited: []string{"other"}}},
		{Decision: Decision{RetryAfter: time.Second, Limited: []string{"tokens", "q"}}},
	} {
		d := &dynamicWaitDriver{admit: func(context.Context, Admission) (Receipt, error) { return result, nil }}
		l, _ := New(d, "app", "domain")
		err := l.Wait(context.Background(), Charge{Name: "q", Cost: 1}, Charge{Name: "tokens", Cost: 1})
		if !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown || d.calls != 1 {
			t.Fatal(result, d.calls, err)
		}
	}
}

func TestDynamicWaitCancellationDuringDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &dynamicWaitDriver{admit: func(context.Context, Admission) (Receipt, error) {
		cancel()
		return Receipt{Decision: Decision{RetryAfter: time.Hour, Limited: []string{"q"}}}, nil
	}}
	var events []Event
	l, _ := NewWithOptions(d, "app", "domain", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	err := l.Wait(ctx, sampleCharge())
	if !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged || d.calls != 1 {
		t.Fatal(d.calls, err)
	}
	if len(events) != 1 || events[0].Operation != "wait" || events[0].Result != "error" || events[0].Err != err || events[0].RetryAfter != time.Hour {
		t.Fatal(events)
	}
}

func TestDynamicWaitDeadlineDuringDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	d := &dynamicWaitDriver{admit: func(context.Context, Admission) (Receipt, error) {
		return Receipt{Decision: Decision{RetryAfter: time.Hour, Limited: []string{"q"}}}, nil
	}}
	l, _ := New(d, "app", "domain")
	if err := l.Wait(ctx, sampleCharge()); !errors.Is(err, context.DeadlineExceeded) || OutcomeOf(err) != NotCharged || d.calls > 1 {
		t.Fatal(d.calls, err)
	}
}

func TestDynamicWaitPreservesDispatchOutcomeDespiteCancellation(t *testing.T) {
	for _, outcome := range []Outcome{"", NotCharged, Unknown} {
		t.Run(string(outcome), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d := &dynamicWaitDriver{admit: func(context.Context, Admission) (Receipt, error) {
				cancel()
				if outcome != "" {
					return Receipt{}, &Error{Outcome: outcome, Err: context.Canceled}
				}
				return Receipt{Decision: Decision{Allowed: true}}, nil
			}}
			l, _ := New(d, "app", "domain")
			err := l.Wait(ctx, sampleCharge())
			if outcome == "" {
				if err != nil {
					t.Fatal("confirmed debit replaced by cancellation", err)
				}
			} else if !errors.Is(err, context.Canceled) || OutcomeOf(err) != outcome {
				t.Fatal(outcome, err)
			}
			if d.calls != 1 {
				t.Fatal(d.calls)
			}
		})
	}
}
