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
	result  Receipt
	err     error
	found   bool
}

func (d *testDriver) Admit(context.Context, Admission) (Receipt, error) {
	return Receipt{}, failure("allow", NotCharged, ErrUnsupported)
}

func (d *testDriver) AdmitStatic(_ context.Context, a StaticAdmission) (Receipt, error) {
	d.calls++
	d.request = a
	return d.result, d.err
}
func (d *testDriver) Lookup(context.Context, LookupRequest) (Decision, bool, error) {
	d.calls++
	return d.result.Decision, d.found, d.err
}
func sampleRule() Rule {
	return Rule{Name: "q", Policy: Policy{Rate: 1, Period: time.Second, Burst: 5}, Cost: 1}
}

func TestFacadeValidationAndErrors(t *testing.T) {
	var typedNil *testDriver
	for _, driver := range []Driver{nil, typedNil} {
		if _, err := New(driver, "app", "domain"); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	driver := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	if _, err := New(driver, "", "domain"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	l, _ := New(driver, "app", "domain")
	ctx := context.Background()
	for _, call := range []func() error{
		func() error { _, err := l.AllowStatic(ctx); return err },
		func() error { _, err := l.AllowStatic(ctx, sampleRule(), sampleRule()); return err },
		func() error { _, err := l.AllowStatic(nil, sampleRule()); return err },
		func() error { _, err := l.AllowStaticOnce(ctx, "", time.Second, sampleRule()); return err },
		func() error { _, err := l.AllowStaticOnce(ctx, "id", time.Nanosecond, sampleRule()); return err },
		func() error { _, _, err := l.Lookup(ctx, ""); return err },
	} {
		if err := call(); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.AllowStatic(canceled, sampleRule()); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if driver.calls != 0 {
		t.Fatal("invalid request reached driver")
	}
	if _, err := l.AllowStaticOnce(ctx, "id", time.Minute, sampleRule()); err != nil {
		t.Fatal(err)
	}
	if driver.request.Namespace != "app" || driver.request.Domain != "domain" || driver.request.ID != "id" {
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
		decision, err := l.AllowStatic(ctx, sampleRule())
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
		driver := &testDriver{result: Receipt{Decision: decision}}
		l, _ := New(driver, "a", "b")
		if _, err := l.AllowStatic(context.Background(), sampleRule()); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
			t.Fatalf("%+v %v", decision, err)
		}
	}
	driver := &testDriver{result: Receipt{Decision: Decision{Allowed: true}, Replayed: true}}
	l, _ := New(driver, "a", "b")
	if _, err := l.AllowStatic(context.Background(), sampleRule()); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
}

func TestFacadeObservation(t *testing.T) {
	var events []Event
	driver := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	l, _ := NewWithOptions(driver, "a", "b", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	l.AllowStatic(context.Background(), sampleRule())
	l.AllowStatic(context.Background())
	driver.result.Replayed = true
	l.AllowStaticOnce(context.Background(), "id", time.Minute, sampleRule())
	driver.found = true
	l.Lookup(context.Background(), "id")
	driver.found = false
	l.Lookup(context.Background(), "id")
	var results []string
	for _, e := range events {
		results = append(results, e.Result)
	}
	if !reflect.DeepEqual(results, []string{"allowed", "error", "allowed", "found", "missing"}) || !events[2].Replayed || events[3].Replayed {
		t.Fatal(events)
	}
}

func TestFingerprintAndRounding(t *testing.T) {
	a := StaticAdmission{Scope: Scope{"a", "b"}, ID: "id", Retention: time.Minute, Rules: []Rule{sampleRule(), {Name: "tokens", Policy: Policy{Rate: 3, Period: time.Second, Burst: 4}, Cost: 2}}}
	step, err := a.Rules[1].Interval()
	if err != nil || step != 333334*time.Microsecond {
		t.Fatal(step, err)
	}
	fp := a.Fingerprint()
	a.Rules[0].Policy.Rate = 60
	a.Rules[0].Policy.Period = time.Minute
	if fp != a.Fingerprint() {
		t.Fatal("equivalent policy changed fingerprint")
	}
	a.Rules[0], a.Rules[1] = a.Rules[1], a.Rules[0]
	if fp == a.Fingerprint() {
		t.Fatal("ordering not bound")
	}
}

// waitDriver scripts attempts while retaining the ordinary test driver's lookup.
type waitDriver struct {
	testDriver
	admit func(context.Context, StaticAdmission) (Receipt, error)
}

func (d *waitDriver) AdmitStatic(ctx context.Context, a StaticAdmission) (Receipt, error) {
	d.calls++
	return d.admit(ctx, a)
}

func TestWaitRetriesOnlyDenials(t *testing.T) {
	rules := []Rule{sampleRule(), {Name: "tokens", Policy: Policy{Rate: 10, Period: time.Second, Burst: 20}, Cost: 7}}
	original := append([]Rule(nil), rules...)
	var lastAttempt time.Time
	const delay = 5 * time.Millisecond
	d := &waitDriver{}
	d.admit = func(_ context.Context, a StaticAdmission) (Receipt, error) {
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
	if err := l.WaitStatic(context.Background(), rules...); err != nil {
		t.Fatal(err)
	}
	if d.calls != 3 || len(events) != 1 || events[0].Operation != "wait_static" || events[0].Result != "allowed" || events[0].Replayed || events[0].RuleCount != 2 || events[0].RetryAfter != 0 || events[0].Duration < 2*delay {
		t.Fatal(d.calls, events)
	}
}

func TestWaitValidation(t *testing.T) {
	d := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	l, _ := New(d, "a", "b")
	var nilLimiter *Limiter
	for name, call := range map[string]func() error{
		"nil receiver":    func() error { return nilLimiter.WaitStatic(context.Background(), sampleRule()) },
		"zero receiver":   func() error { return (&Limiter{}).WaitStatic(context.Background(), sampleRule()) },
		"nil context":     func() error { return l.WaitStatic(nil, sampleRule()) },
		"no rules":        func() error { return l.WaitStatic(context.Background()) },
		"duplicate rules": func() error { return l.WaitStatic(context.Background(), sampleRule(), sampleRule()) },
		"invalid cost": func() error {
			r := sampleRule()
			r.Cost = r.Policy.Burst + 1
			return l.WaitStatic(context.Background(), r)
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
	if err := l.WaitStatic(ctx, sampleRule()); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
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
			d.admit = func(context.Context, StaticAdmission) (Receipt, error) {
				if d.calls == 1 {
					return Receipt{Decision: Decision{RetryAfter: time.Microsecond, Limited: []string{"q"}}}, nil
				}
				return Receipt{}, backendErr
			}
			l, _ := New(d, "a", "b")
			err := l.WaitStatic(context.Background(), sampleRule())
			var typed *Error
			if !errors.Is(err, backendErr) || OutcomeOf(err) != OutcomeOf(backendErr) || !errors.As(err, &typed) || typed.Op != "wait_static" || d.calls != 2 {
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
		if err := l.WaitStatic(context.Background(), sampleRule()); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown || d.calls != 1 {
			t.Fatal(result, err, d.calls)
		}
	}
}

func TestWaitCancellationDuringDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &waitDriver{admit: func(context.Context, StaticAdmission) (Receipt, error) {
		cancel()
		return Receipt{Decision: Decision{RetryAfter: time.Hour, Limited: []string{"q"}}}, nil
	}}
	var event Event
	l, _ := NewWithOptions(d, "a", "b", Options{Observer: func(_ context.Context, e Event) { event = e }})
	err := l.WaitStatic(ctx, sampleRule())
	if !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged || d.calls != 1 || event.Operation != "wait_static" || event.Result != "error" || event.Err != err {
		t.Fatal(d.calls, err, event)
	}
}

func TestWaitDeadlineDuringDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	d := &testDriver{result: Receipt{Decision: Decision{RetryAfter: time.Hour, Limited: []string{"q"}}}}
	l, _ := New(d, "a", "b")
	if err := l.WaitStatic(ctx, sampleRule()); !errors.Is(err, context.DeadlineExceeded) || OutcomeOf(err) != NotCharged || d.calls > 1 {
		t.Fatal(d.calls, err)
	}
}

func TestWaitPreservesSuccessDespiteCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &waitDriver{admit: func(context.Context, StaticAdmission) (Receipt, error) {
		cancel() // A confirmed debit must not be replaced with NotCharged.
		return Receipt{Decision: Decision{Allowed: true}}, nil
	}}
	l, _ := New(d, "a", "b")
	if err := l.WaitStatic(ctx, sampleRule()); err != nil || d.calls != 1 {
		t.Fatal(d.calls, err)
	}
}
