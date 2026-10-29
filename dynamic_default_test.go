package pacelock

import (
	"context"
	"errors"
	"testing"
)

// dynamicOnlyDriver implements admission without optional policy updates.
type dynamicOnlyDriver struct{ calls int }

func (d *dynamicOnlyDriver) Admit(context.Context, Admission) (Decision, error) {
	d.calls++
	return Decision{Allowed: true}, nil
}

func TestDynamicAdmissionRequiresPolicy(t *testing.T) {
	ctx := context.Background()
	d := &managedTestDriver{policies: map[string]QuotaState{}, debts: map[string]QuotaState{}}
	var events []Event
	l, err := NewWithOptions(d, "app", "domain", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []func() error{
		func() error { _, err := l.Allow(ctx, sampleCharge()); return err },
		func() error { return l.Wait(ctx, sampleCharge()) },
	} {
		if err := call(); !errors.Is(err, ErrPolicyMissing) || OutcomeOf(err) != NotCharged {
			t.Fatalf("admission must fail closed: %v", err)
		}
	}
	if d.managedCalls != 2 || len(d.debts) != 0 {
		t.Fatal(d.managedCalls, d.debts)
	}
	if len(events) != 2 || events[0].Operation != "allow" || events[1].Operation != "wait" {
		t.Fatal(events)
	}
}

func TestAdmissionWithoutQuotaDriver(t *testing.T) {
	d := &dynamicOnlyDriver{}
	l, err := New(d, "app", "domain")
	if err != nil {
		t.Fatal(err)
	}
	if decision, err := l.Allow(context.Background(), sampleCharge()); err != nil || !decision.Allowed {
		t.Fatal(decision, err)
	}
	if err := l.UpdateQuotas(context.Background(), managedQuota(1, 10, 10)); !errors.Is(err, ErrUnsupported) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if d.calls != 1 {
		t.Fatal(d.calls)
	}
}
