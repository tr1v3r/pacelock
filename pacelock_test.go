package pacelock

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

type testDriver struct {
	calls   int
	request StaticAdmission
	result  Decision
	err     error
}

func (d *testDriver) Admit(context.Context, Admission) (Decision, error) {
	return Decision{}, failure("allow", NotCharged, ErrUnsupported)
}

func (d *testDriver) AdmitStatic(_ context.Context, a StaticAdmission) (Decision, error) {
	d.calls++
	d.request = a
	return d.result, d.err
}

func sampleRule() Rule {
	return Rule{Name: "q", Policy: Policy{Rate: 1, Period: time.Second, Burst: 5}, Cost: 1}
}

func sampleQuota() Quota { return Quota{Name: "q", Policy: sampleRule().Policy} }

func sampleCharge() Charge { return Charge{Name: "q", Cost: 1} }

func TestFacadeValidationAndErrors(t *testing.T) {
	var typedNil *testDriver
	for _, driver := range []Driver{nil, typedNil} {
		if _, err := NewStatic(driver, "app", "domain", sampleQuota()); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	driver := &testDriver{result: Decision{Allowed: true}}
	if _, err := New(driver, "", "domain"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	l, _ := NewStatic(driver, "app", "domain", sampleQuota())
	ctx := context.Background()
	for _, call := range []func() error{
		func() error { _, err := l.Allow(ctx); return err },
		func() error { _, err := l.Allow(ctx, sampleCharge(), sampleCharge()); return err },
		func() error { _, err := l.Allow(nil, sampleCharge()); return err },
	} {
		if err := call(); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.Allow(canceled, sampleCharge()); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if driver.calls != 0 {
		t.Fatal("invalid request reached driver")
	}
	if _, err := l.Allow(ctx, sampleCharge()); err != nil {
		t.Fatal(err)
	}
	if driver.request.Namespace != "app" || driver.request.Domain != "domain" {
		t.Fatal(driver.request)
	}
	for _, tc := range []struct {
		err  error
		want Outcome
	}{
		{io.ErrUnexpectedEOF, Unknown},
		{&Error{Op: "backend", Outcome: NotCharged, Err: ErrPolicyMismatch}, NotCharged},
		{&Error{Op: "backend", Outcome: Unknown, Err: context.DeadlineExceeded}, Unknown},
	} {
		driver.err = tc.err
		decision, err := l.Allow(ctx, sampleCharge())
		var typed *Error
		if !errors.Is(err, tc.err) || !errors.As(err, &typed) || typed.Op != "allow_static" || OutcomeOf(err) != tc.want || decision.Allowed {
			t.Fatalf("%+v %v", decision, err)
		}
	}
}

func TestFacadeRejectsMalformedDriverResults(t *testing.T) {
	for _, decision := range []Decision{
		{}, {Allowed: true, RetryAfter: time.Second},
		{RetryAfter: time.Second, Limited: []string{"wrong"}},
		{RetryAfter: time.Second, Limited: []string{"q", "q"}},
	} {
		driver := &testDriver{result: decision}
		l, _ := NewStatic(driver, "a", "b", sampleQuota())
		if _, err := l.Allow(context.Background(), sampleCharge()); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
			t.Fatalf("%+v %v", decision, err)
		}
	}
}

func TestFacadeObservation(t *testing.T) {
	var events []Event
	driver := &testDriver{result: Decision{Allowed: true}}
	l, _ := NewStaticWithOptions(driver, "a", "b", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }}, sampleQuota())
	l.Allow(context.Background(), sampleCharge())
	l.Allow(context.Background())
	driver.result = Decision{RetryAfter: time.Second, Limited: []string{"q"}}
	l.Allow(context.Background(), sampleCharge())
	var results []string
	for _, e := range events {
		results = append(results, e.Result)
	}
	if !reflect.DeepEqual(results, []string{"allowed", "error", "denied"}) {
		t.Fatal(events)
	}
}

type waitDriver struct {
	testDriver
	admit func(context.Context, StaticAdmission) (Decision, error)
}

func (d *waitDriver) AdmitStatic(ctx context.Context, a StaticAdmission) (Decision, error) {
	d.calls++
	return d.admit(ctx, a)
}

func TestWaitRetriesOnlyDenials(t *testing.T) {
	rules := []Rule{sampleRule(), {Name: "tokens", Policy: Policy{Rate: 10, Period: time.Second, Burst: 20}, Cost: 7}}
	original := append([]Rule(nil), rules...)
	charges := []Charge{{Name: rules[0].Name, Cost: rules[0].Cost}, {Name: rules[1].Name, Cost: rules[1].Cost}}
	var lastAttempt time.Time
	const delay = 5 * time.Millisecond
	d := &waitDriver{}
	d.admit = func(_ context.Context, a StaticAdmission) (Decision, error) {
		if !reflect.DeepEqual(a.Rules, original) || a.Scope != (Scope{"a", "b"}) {
			t.Fatalf("changed admission: %+v", a)
		}
		if !lastAttempt.IsZero() && time.Since(lastAttempt) < delay {
			t.Fatal("retried before RetryAfter")
		}
		lastAttempt = time.Now()
		// Changing the source slice between attempts must not change the wait.
		rules[0].Cost = 5
		charges[0].Cost = 5
		if d.calls < 3 {
			return Decision{RetryAfter: delay, Limited: []string{"tokens"}}, nil
		}
		return Decision{Allowed: true}, nil
	}
	var events []Event
	l, _ := NewStaticWithOptions(d, "a", "b", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }}, Quota{Name: original[0].Name, Policy: original[0].Policy}, Quota{Name: original[1].Name, Policy: original[1].Policy})
	if err := l.Wait(context.Background(), charges...); err != nil {
		t.Fatal(err)
	}
	if d.calls != 3 || len(events) != 1 || events[0].Operation != "wait_static" || events[0].Result != "allowed" || events[0].RuleCount != 2 || events[0].RetryAfter != 0 || events[0].Duration < 2*delay {
		t.Fatal(d.calls, events)
	}
}

func TestWaitValidation(t *testing.T) {
	d := &testDriver{result: Decision{Allowed: true}}
	l, _ := NewStatic(d, "a", "b", sampleQuota())
	var nilLimiter *StaticLimiter
	for name, call := range map[string]func() error{
		"nil receiver":    func() error { return nilLimiter.Wait(context.Background(), sampleCharge()) },
		"zero receiver":   func() error { return (&StaticLimiter{}).Wait(context.Background(), sampleCharge()) },
		"nil context":     func() error { return l.Wait(nil, sampleCharge()) },
		"no rules":        func() error { return l.Wait(context.Background()) },
		"duplicate rules": func() error { return l.Wait(context.Background(), sampleCharge(), sampleCharge()) },
		"invalid cost": func() error {
			r := sampleCharge()
			r.Cost = sampleQuota().Policy.Burst + 1
			return l.Wait(context.Background(), r)
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			var typed *Error
			if !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged || !errors.As(err, &typed) || typed.Op != "wait_static" {
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
}

func TestWaitStopsOnEveryError(t *testing.T) {
	for _, backendErr := range []error{
		io.ErrUnexpectedEOF,
		&Error{Outcome: NotCharged, Err: ErrPolicyMismatch},
		&Error{Outcome: NotCharged, Err: ErrBackend},
		&Error{Outcome: Unknown, Err: context.Canceled},
		&Error{Outcome: Unknown, Err: ErrBackend},
		&Error{Outcome: Unknown, Err: ErrState},
	} {
		t.Run(backendErr.Error(), func(t *testing.T) {
			d := &waitDriver{}
			d.admit = func(context.Context, StaticAdmission) (Decision, error) {
				if d.calls == 1 {
					return Decision{RetryAfter: time.Microsecond, Limited: []string{"q"}}, nil
				}
				return Decision{}, backendErr
			}
			l, _ := NewStatic(d, "a", "b", sampleQuota())
			err := l.Wait(context.Background(), sampleCharge())
			var typed *Error
			if !errors.Is(err, backendErr) || OutcomeOf(err) != OutcomeOf(backendErr) || !errors.As(err, &typed) || typed.Op != "wait_static" || d.calls != 2 {
				t.Fatal(d.calls, err)
			}
		})
	}
	for _, result := range []Decision{
		{},
		Decision{RetryAfter: -time.Second, Limited: []string{"q"}},
		Decision{RetryAfter: time.Second, Limited: []string{"other"}},
	} {
		d := &testDriver{result: result}
		l, _ := NewStatic(d, "a", "b", sampleQuota())
		if err := l.Wait(context.Background(), sampleCharge()); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown || d.calls != 1 {
			t.Fatal(result, err, d.calls)
		}
	}
}

func TestWaitCancellationDuringDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &waitDriver{admit: func(context.Context, StaticAdmission) (Decision, error) {
		cancel()
		return Decision{RetryAfter: time.Hour, Limited: []string{"q"}}, nil
	}}
	var event Event
	l, _ := NewStaticWithOptions(d, "a", "b", Options{Observer: func(_ context.Context, e Event) { event = e }}, sampleQuota())
	err := l.Wait(ctx, sampleCharge())
	if !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged || d.calls != 1 || event.Operation != "wait_static" || event.Result != "error" || event.Err != err {
		t.Fatal(d.calls, err, event)
	}
}

func TestWaitDeadlineDuringDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	d := &testDriver{result: Decision{RetryAfter: time.Hour, Limited: []string{"q"}}}
	l, _ := NewStatic(d, "a", "b", sampleQuota())
	if err := l.Wait(ctx, sampleCharge()); !errors.Is(err, context.DeadlineExceeded) || OutcomeOf(err) != NotCharged || d.calls > 1 {
		t.Fatal(d.calls, err)
	}
}

func TestWaitPreservesSuccessDespiteCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &waitDriver{admit: func(context.Context, StaticAdmission) (Decision, error) {
		cancel() // A confirmed debit must not be replaced with NotCharged.
		return Decision{Allowed: true}, nil
	}}
	l, _ := NewStatic(d, "a", "b", sampleQuota())
	if err := l.Wait(ctx, sampleCharge()); err != nil || d.calls != 1 {
		t.Fatal(d.calls, err)
	}
}
