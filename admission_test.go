package pacelock

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestDynamicValidationAndObservationBeforeDispatch(t *testing.T) {
	d := &testDriver{}
	var events []Event
	l, _ := NewWithOptions(d, "app", "domain", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	invalid := [][]Charge{nil, make([]Charge, MaxRules+1), {{}}, {{Name: "q", Cost: 0}}, {{Name: "q", Cost: -1}}, {{Name: "q", Cost: MaxHorizon.Microseconds() + 1}}, {{Name: strings.Repeat("q", 257), Cost: 1}}, {sampleCharge(), sampleCharge()}}
	for _, charges := range invalid {
		for _, op := range []string{"allow", "wait"} {
			var err error
			if op == "allow" {
				_, err = l.Allow(context.Background(), charges...)
			} else {
				err = l.Wait(context.Background(), charges...)
			}
			var sdk *Error
			if !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged || !errors.As(err, &sdk) || sdk.Op != op {
				t.Fatal(op, err)
			}
			e := events[len(events)-1]
			if e.Operation != op || e.RuleCount != len(charges) || e.Result != "error" || e.Err != err {
				t.Fatal(e)
			}
		}
	}
	if d.calls != 0 || len(events) != 2*len(invalid) {
		t.Fatal(d.calls, events)
	}
	for _, current := range []*Limiter{nil, {}} {
		if _, err := current.Allow(context.Background(), sampleCharge()); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatal(err)
		}
	}
	for _, scope := range []Scope{{}, {Namespace: "app"}, {Domain: "domain"}, {Namespace: strings.Repeat("n", 257), Domain: "domain"}, {Namespace: "app", Domain: strings.Repeat("d", 257)}} {
		if got, err := New(d, scope.Namespace, scope.Domain); got != nil || !errors.Is(err, ErrInvalid) {
			t.Fatal(got, err)
		}
	}
}

func TestDynamicChargeOrderAndSnapshot(t *testing.T) {
	d := &testDriver{result: Decision{Allowed: true}}
	l, _ := New(d, "app", "domain")
	charges := make([]Charge, MaxRules)
	for i := range charges {
		charges[i] = Charge{Name: fmt.Sprint(MaxRules - i), Cost: int64(i + 1)}
	}
	original := append([]Charge(nil), charges...)
	for _, wait := range []bool{false, true} {
		var err error
		if wait {
			err = l.Wait(context.Background(), charges...)
		} else {
			_, err = l.Allow(context.Background(), charges...)
		}
		if err != nil || !reflect.DeepEqual(d.request.Charges, original) || !reflect.DeepEqual(charges, original) {
			t.Fatal(d.request, err)
		}
		// Each dispatch owns a copy, independent of the caller's slice.
		d.request.Charges[0].Name = "changed"
		if !reflect.DeepEqual(charges, original) {
			t.Fatal("driver request aliases caller")
		}
	}
	// A selected subset preserves caller order; there is no local binding order.
	selected := []Charge{charges[3], charges[1]}
	d.result = Decision{RetryAfter: 1, Limited: []string{selected[0].Name, selected[1].Name}}
	if _, err := l.Allow(context.Background(), selected...); err != nil {
		t.Fatal(err)
	}
	d.result.Limited = []string{selected[1].Name, selected[0].Name}
	if _, err := l.Allow(context.Background(), selected...); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
		t.Fatal(err)
	}
}

func TestAdmissionNamesAreOpaqueBytes(t *testing.T) {
	name := string([]byte{0xff})
	scope := Scope{Namespace: "app", Domain: "domain"}
	a := Admission{Scope: scope, Charges: []Charge{{Name: name, Cost: 1}}}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	d := &testDriver{result: Decision{Allowed: true}}
	l, err := New(d, scope.Namespace, scope.Domain)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = l.Allow(context.Background(), a.Charges...); err != nil {
		t.Fatal(err)
	}
	if d.request.Charges[0].Name != name {
		t.Fatal("name changed")
	}
}
