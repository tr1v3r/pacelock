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
	request Admission
	result  Receipt
	err     error
	found   bool
}

func (d *testDriver) Admit(_ context.Context, a Admission) (Receipt, error) {
	d.calls++
	d.request = a
	return d.result, d.err
}
func (d *testDriver) Lookup(context.Context, LookupRequest) (Decision, bool, error) {
	d.calls++
	return d.result.Decision, d.found, d.err
}
func sampleRule() Rule { return Rule{Name: "q", Rate: 1, Period: time.Second, Burst: 5, Cost: 1} }

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
		func() error { _, err := l.Allow(ctx); return err },
		func() error { _, err := l.Allow(ctx, sampleRule(), sampleRule()); return err },
		func() error { _, err := l.Allow(nil, sampleRule()); return err },
		func() error { _, err := l.AllowOnce(ctx, "", time.Second, sampleRule()); return err },
		func() error { _, err := l.AllowOnce(ctx, "id", time.Nanosecond, sampleRule()); return err },
		func() error { _, _, err := l.Lookup(ctx, ""); return err },
	} {
		if err := call(); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.Allow(canceled, sampleRule()); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if driver.calls != 0 {
		t.Fatal("invalid request reached driver")
	}
	if _, err := l.AllowOnce(ctx, "id", time.Minute, sampleRule()); err != nil {
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
		decision, err := l.Allow(ctx, sampleRule())
		var typed *Error
		if !errors.Is(err, tc.err) || !errors.As(err, &typed) || typed.Op != "allow" || OutcomeOf(err) != tc.want || decision.Allowed {
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
		if _, err := l.Allow(context.Background(), sampleRule()); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
			t.Fatalf("%+v %v", decision, err)
		}
	}
	driver := &testDriver{result: Receipt{Decision: Decision{Allowed: true}, Replayed: true}}
	l, _ := New(driver, "a", "b")
	if _, err := l.Allow(context.Background(), sampleRule()); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
}

func TestFacadeObservation(t *testing.T) {
	var events []Event
	driver := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	l, _ := NewWithOptions(driver, "a", "b", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	l.Allow(context.Background(), sampleRule())
	l.Allow(context.Background())
	driver.result.Replayed = true
	l.AllowOnce(context.Background(), "id", time.Minute, sampleRule())
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
	a := Admission{Scope: Scope{"a", "b"}, ID: "id", Retention: time.Minute, Rules: []Rule{sampleRule(), {Name: "tokens", Rate: 3, Period: time.Second, Burst: 4, Cost: 2}}}
	step, err := a.Rules[1].Interval()
	if err != nil || step != 333334*time.Microsecond {
		t.Fatal(step, err)
	}
	fp := a.Fingerprint()
	a.Rules[0].Rate = 60
	a.Rules[0].Period = time.Minute
	if fp != a.Fingerprint() {
		t.Fatal("equivalent policy changed fingerprint")
	}
	a.Rules[0], a.Rules[1] = a.Rules[1], a.Rules[0]
	if fp == a.Fingerprint() {
		t.Fatal("ordering not bound")
	}
}
