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
	result  Decision
	err     error
}

func (d *testDriver) Admit(_ context.Context, a Admission) (Decision, error) {
	d.calls++
	d.request = a
	return d.result, d.err
}

func sampleRule() Rule {
	return Rule{Name: "q", Policy: Policy{Rate: 1, Period: time.Second, Burst: 5}, Cost: 1}
}

func sampleCharge() Charge { return Charge{Name: "q", Cost: 1} }

func TestFacadeValidationAndErrors(t *testing.T) {
	var typedNil *testDriver
	for _, driver := range []Driver{nil, typedNil} {
		if _, err := New(driver, "app", "domain"); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	driver := &testDriver{result: Decision{Allowed: true}}
	if _, err := New(driver, "", "domain"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	l, _ := New(driver, "app", "domain")
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
		driver := &testDriver{result: decision}
		l, _ := New(driver, "a", "b")
		if _, err := l.Allow(context.Background(), sampleCharge()); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
			t.Fatalf("%+v %v", decision, err)
		}
	}
}

func TestFacadeObservation(t *testing.T) {
	var events []Event
	driver := &testDriver{result: Decision{Allowed: true}}
	l, _ := NewWithOptions(driver, "a", "b", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
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
