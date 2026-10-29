package pacelock

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A dynamic-only driver intentionally has no optional StaticDriver capability.
type dynamicOnlyDriver struct {
	calls int
}

func (d *dynamicOnlyDriver) Admit(context.Context, Admission) (Receipt, error) {
	d.calls++
	return Receipt{Decision: Decision{Allowed: true}}, nil
}

func (*dynamicOnlyDriver) Lookup(context.Context, LookupRequest) (Decision, bool, error) {
	return Decision{}, false, nil
}

func TestAdmissionFingerprintEncoding(t *testing.T) {
	static := StaticAdmission{Rules: []Rule{sampleRule()}, Retention: time.Minute}
	// Static fingerprints use the current protocol without a compatibility slot.
	if got, want := static.Fingerprint(), "10e07ca7947bdd45374a09b74df360d81744c2cc95a6c4dd828928bdb5098c94"; got != want {
		t.Fatalf("static fingerprint changed: %s != %s", got, want)
	}
	dynamic := Admission{Charges: []Charge{{Name: "q", Cost: 1}}, Retention: time.Minute}
	if got, want := dynamic.Fingerprint(), "cb8ae8ea6ddef7b72d2a6889d32f8fbd073fc432c8d3faf6bc3aff97ebcb9acf"; got != want {
		t.Fatalf("dynamic fingerprint discriminator changed: %s != %s", got, want)
	}
}

func TestDefaultDynamicDoesNotFallBackToStatic(t *testing.T) {
	ctx := context.Background()
	d := &managedTestDriver{
		testDriver: testDriver{result: Receipt{Decision: Decision{Allowed: true}}},
		policies:   map[string]QuotaState{},
		debts:      map[string]QuotaState{},
	}
	var events []Event
	l, err := NewWithOptions(d, "app", "domain", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	if err != nil {
		t.Fatal(err)
	}
	charge := Charge{Name: "q", Cost: 1}
	for _, call := range []func() error{
		func() error { _, err := l.Allow(ctx, charge); return err },
		func() error { _, err := l.AllowOnce(ctx, "attempt", time.Minute, charge); return err },
	} {
		if err := call(); !errors.Is(err, ErrPolicyMissing) || OutcomeOf(err) != NotCharged {
			t.Fatalf("default admission must fail closed: %v", err)
		}
	}
	if d.calls != 0 || d.managedCalls != 2 {
		t.Fatalf("default admission dispatched static=%d dynamic=%d", d.calls, d.managedCalls)
	}
	static, _ := NewStaticWithOptions(d, "app", "domain", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }}, sampleQuota())
	if decision, err := static.Allow(ctx, sampleCharge()); err != nil || !decision.Allowed || d.calls != 1 {
		t.Fatalf("explicit static alternative: %+v %v", decision, err)
	}
	if len(events) != 3 || events[0].Operation != "allow" || events[1].Operation != "allow_once" || events[2].Operation != "allow_static" {
		t.Fatalf("operations: %+v", events)
	}
}

func TestOptionalStaticDriverUnsupportedBeforeDispatch(t *testing.T) {
	ctx := context.Background()
	d := &dynamicOnlyDriver{}
	l, err := New(d, "app", "domain")
	if err != nil {
		t.Fatal(err)
	}
	if decision, err := l.Allow(ctx, Charge{Name: "q", Cost: 1}); err != nil || !decision.Allowed {
		t.Fatalf("mandatory dynamic admission: %+v %v", decision, err)
	}
	q := Quota{Name: "q", Policy: sampleRule().Policy}
	bound, err := NewStatic(d, "app", "domain", q)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		op   string
		call func() error
	}{
		{"allow_static", func() error { _, err := bound.Allow(ctx, sampleCharge()); return err }},
		{"allow_static_once", func() error { _, err := bound.AllowOnce(ctx, "attempt", time.Minute, sampleCharge()); return err }},
		{"wait_static", func() error { return bound.Wait(ctx, sampleCharge()) }},
	} {
		err := tc.call()
		var typed *Error
		if !errors.Is(err, ErrUnsupported) || OutcomeOf(err) != NotCharged || !errors.As(err, &typed) || typed.Op != tc.op {
			t.Fatalf("%s unsupported: %v", tc.op, err)
		}
	}
	if d.calls != 1 {
		t.Fatalf("unsupported static operation dispatched dynamic Admit: %d", d.calls)
	}
}
