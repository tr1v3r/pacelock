package pacelock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

func bindForTest(t *testing.T, l *Limiter, quotas ...Quota) *BoundLimiter {
	t.Helper()
	b, err := l.Bind(quotas...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func boundTestQuotas() []Quota {
	return []Quota{
		{Name: "rpm", Policy: Policy{Rate: 1, Period: time.Hour, Burst: 2}},
		{Name: "tokens", Policy: Policy{Rate: 1, Period: time.Hour, Burst: 5}},
	}
}

func TestBindValidation(t *testing.T) {
	s := &testDriver{}
	l, _ := New(s, "test", "bind")
	for _, current := range []*Limiter{nil, {}} {
		if b, err := current.Bind(boundTestQuotas()...); b != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("uninitialized limiter: %v %v", b, err)
		}
	}
	for _, quotas := range [][]Quota{nil, make([]Quota, MaxRules+1), {boundTestQuotas()[0], boundTestQuotas()[0]}} {
		if b, err := l.Bind(quotas...); b != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid set: %v %v", b, err)
		}
	}
	for _, tc := range []struct {
		name   string
		change func(*Quota)
	}{
		{"empty name", func(q *Quota) { q.Name = "" }},
		{"long name", func(q *Quota) { q.Name = strings.Repeat("x", 257) }},
		{"zero rate", func(q *Quota) { q.Policy.Rate = 0 }},
		{"negative rate", func(q *Quota) { q.Policy.Rate = -1 }},
		{"zero period", func(q *Quota) { q.Policy.Period = 0 }},
		{"negative period", func(q *Quota) { q.Policy.Period = -1 }},
		{"long period", func(q *Quota) { q.Policy.Period = MaxHorizon + 1 }},
		{"zero burst", func(q *Quota) { q.Policy.Burst = 0 }},
		{"negative burst", func(q *Quota) { q.Policy.Burst = -1 }},
		{"refill horizon", func(q *Quota) { q.Policy.Burst = 25 }},
		{"overflow", func(q *Quota) { q.Policy.Burst = 1 << 62 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := boundTestQuotas()[0]
			tc.change(&q)
			if b, err := l.Bind(q); b != nil || !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid policy: %v %v", b, err)
			}
		})
	}
	quotas := make([]Quota, MaxRules)
	charges := make([]Charge, MaxRules)
	for i := range quotas {
		quotas[i] = Quota{Name: fmt.Sprint(i), Policy: Policy{Rate: 1, Period: time.Nanosecond, Burst: 1}}
		charges[MaxRules-1-i] = Charge{Name: quotas[i].Name, Cost: 1}
	}
	b := bindForTest(t, l, quotas...)
	if s.calls != 0 {
		t.Fatal("binding or local validation contacted Driver")
	}
	s.result = Receipt{Decision: Decision{Allowed: true}}
	if d, err := b.Allow(context.Background(), charges...); err != nil || !d.Allowed {
		t.Fatalf("maximum quota set: %+v %v", d, err)
	}
}

func TestBoundValidationBeforeDriver(t *testing.T) {
	s := &testDriver{}
	var events []Event
	l, _ := NewWithOptions(s, "test", "validation", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	b := bindForTest(t, l, boundTestQuotas()...)
	for _, charges := range [][]Charge{
		nil,
		{{Name: "rpm", Cost: 1}},
		{{Name: "rpm", Cost: 1}, {Name: "tokens", Cost: 1}, {Name: "extra", Cost: 1}},
		{{Name: "rpm", Cost: 1}, {Name: "rpm", Cost: 1}},
		{{Name: "rpm", Cost: 1}, {Name: "unknown", Cost: 1}},
		{{Name: "rpm", Cost: 1}, {Name: "", Cost: 1}},
		{{Name: "rpm", Cost: 0}, {Name: "tokens", Cost: 1}},
		{{Name: "rpm", Cost: -1}, {Name: "tokens", Cost: 1}},
		{{Name: "rpm", Cost: 3}, {Name: "tokens", Cost: 1}},
	} {
		for _, op := range []string{"allow", "allow_once"} {
			var err error
			if op == "allow" {
				_, err = b.Allow(context.Background(), charges...)
			} else {
				_, err = b.AllowOnce(context.Background(), "id", time.Minute, charges...)
			}
			var sdk *Error
			if !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged || !errors.As(err, &sdk) || sdk.Op != op {
				t.Fatalf("%s: %v", op, err)
			}
			e := events[len(events)-1]
			if e.Operation != op || e.RuleCount != len(charges) || e.Result != "error" || e.Err != err {
				t.Fatalf("invalid input observation: %+v", e)
			}
		}
	}
	if len(events) != 18 || s.calls != 0 {
		t.Fatalf("events=%d driver calls=%d", len(events), s.calls)
	}
	for _, current := range []*BoundLimiter{nil, {}, b} {
		if _, err := current.Allow(nil); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatal(err)
		}
		if _, err := current.AllowOnce(nil, "id", time.Minute); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatal(err)
		}
		if _, _, err := current.Lookup(nil, "id"); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatal(err)
		}
	}
	for _, current := range []*BoundLimiter{nil, {}} {
		if _, err := current.Allow(context.Background()); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if _, err := current.AllowOnce(context.Background(), "id", time.Minute); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if _, _, err := current.Lookup(context.Background(), "id"); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	charges := []Charge{{Name: "rpm", Cost: 1}, {Name: "tokens", Cost: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Allow(ctx, charges...); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if _, err := b.AllowOnce(ctx, "id", time.Minute, charges...); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if _, _, err := b.Lookup(ctx, "id"); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id        string
		retention time.Duration
	}{
		{"", time.Minute}, {strings.Repeat("x", 257), time.Minute},
		{"id", 0}, {"id", time.Millisecond + 1}, {"id", MaxHorizon + time.Millisecond},
	} {
		if _, err := b.AllowOnce(context.Background(), tc.id, tc.retention, charges...); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatal(err)
		}
	}
	if _, _, err := b.Lookup(context.Background(), ""); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	opaque := string([]byte{255})
	other := bindForTest(t, l, Quota{Name: opaque, Policy: boundTestQuotas()[0].Policy})
	if _, err := other.AllowOnce(context.Background(), "id", time.Minute, Charge{Name: opaque, Cost: 1}); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if s.calls != 0 {
		t.Fatal("invalid admission contacted Driver")
	}
}

func TestBoundObservationAndUnknownOutcome(t *testing.T) {
	var events []Event
	s := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	l, _ := NewWithOptions(s, "test", "observation", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	b := bindForTest(t, l, boundTestQuotas()...)
	charges := []Charge{{Name: "tokens", Cost: 2}, {Name: "rpm", Cost: 1}}
	before := append([]Charge(nil), charges...)
	if d, err := b.Allow(context.Background(), charges...); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	if !reflect.DeepEqual(charges, before) {
		t.Fatal("admission mutated caller charges")
	}
	s.result.Replayed = true
	if r, err := b.AllowOnce(context.Background(), "id", time.Minute, charges...); err != nil || !r.Replayed {
		t.Fatalf("%+v %v", r, err)
	}
	s.result.Replayed = false
	s.err = io.ErrUnexpectedEOF
	for _, op := range []string{"allow", "allow_once"} {
		var err error
		if op == "allow" {
			_, err = b.Allow(context.Background(), charges...)
		} else {
			_, err = b.AllowOnce(context.Background(), "id", time.Minute, charges...)
		}
		if !errors.Is(err, ErrBackend) || OutcomeOf(err) != Unknown {
			t.Fatalf("%s lost unknown outcome: %v", op, err)
		}
	}
	if len(events) != 4 || events[0].Result != "allowed" || events[0].RuleCount != 2 || !events[1].Replayed || events[1].Operation != "allow_once" || events[2].Result != "error" || events[3].Operation != "allow_once" || s.calls != 4 {
		t.Fatalf("events=%+v driver calls=%d", events, s.calls)
	}
}
