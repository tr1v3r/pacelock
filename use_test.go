package pacelock

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestUseConstructsCharge(t *testing.T) {
	for _, want := range []Charge{
		{Name: "rpm", Cost: 1},
		{Name: "tokens", Cost: 800},
		{Name: string([]byte{0xff}), Cost: 2},
		{Name: "", Cost: 0},
		{Name: "tokens", Cost: -1},
	} {
		if got := Use(want.Name, want.Cost); got != want {
			t.Fatalf("Use = %+v, want %+v", got, want)
		}
	}
}

func TestUseAllowAndWait(t *testing.T) {
	d := &testDriver{result: Decision{Allowed: true}}
	l, err := New(d, "app", "domain")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, cost := range []int64{800, 1500} {
		want := []Charge{{Name: "rpm", Cost: 1}, {Name: "tokens", Cost: cost}}
		if decision, err := l.Allow(ctx, Use("rpm", 1), Use("tokens", cost)); err != nil || !decision.Allowed {
			t.Fatalf("Allow: %+v %v", decision, err)
		}
		if !reflect.DeepEqual(d.request.Charges, want) {
			t.Fatalf("Allow charges: %+v, want %+v", d.request.Charges, want)
		}
		if err := l.Wait(ctx, Use("rpm", 1), Use("tokens", cost)); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(d.request.Charges, want) {
			t.Fatalf("Wait charges: %+v, want %+v", d.request.Charges, want)
		}
	}
}

func TestUseDoesNotBypassValidation(t *testing.T) {
	d := &testDriver{}
	l, err := New(d, "app", "domain")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, charges := range [][]Charge{
		{Use("", 1)},
		{Use("rpm", 0)},
		{Use("rpm", -1)},
		{Use("rpm", 1), Use("rpm", 2)},
	} {
		if _, err := l.Allow(ctx, charges...); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatalf("Allow: %v", err)
		}
		if err := l.Wait(ctx, charges...); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatalf("Wait: %v", err)
		}
	}
	if d.calls != 0 {
		t.Fatalf("invalid charges dispatched %d times", d.calls)
	}
}
