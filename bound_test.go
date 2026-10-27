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
		charges[MaxRules-1-i] = quotas[i].Charge(1)
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
	quotas := boundTestQuotas()
	rpm, tokens := quotas[0], quotas[1]
	b := bindForTest(t, l, quotas...)
	invalid := [][]Charge{
		nil,
		{rpm.Charge(1)},
		{rpm.Charge(1), tokens.Charge(1), (Quota{Name: "extra", Policy: rpm.Policy}).Charge(1)},
		{rpm.Charge(1), rpm.Charge(1)},
		{rpm.Charge(1), (Quota{Name: "unknown", Policy: tokens.Policy}).Charge(1)},
		{rpm.Charge(1), (Quota{Policy: tokens.Policy}).Charge(1)},
		{rpm.Charge(0), tokens.Charge(1)},
		{rpm.Charge(-1), tokens.Charge(1)},
		{rpm.Charge(3), tokens.Charge(1)},
		{rpm.Charge(1), {}},
	}
	for _, charges := range invalid {
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
	if len(events) != 2*len(invalid) || s.calls != 0 {
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
	charges := []Charge{rpm.Charge(1), tokens.Charge(1)}
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
	opaqueQuota := Quota{Name: opaque, Policy: rpm.Policy}
	other := bindForTest(t, l, opaqueQuota)
	if _, err := other.AllowOnce(context.Background(), "id", time.Minute, opaqueQuota.Charge(1)); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if s.calls != 0 {
		t.Fatal("invalid admission contacted Driver")
	}
}

func TestQuotaChargeSnapshot(t *testing.T) {
	s := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	l, _ := New(s, "test", "snapshot")
	q := boundTestQuotas()[0]
	original := q
	b := bindForTest(t, l, q)
	charge := q.Charge(1)
	if s.calls != 0 {
		t.Fatal("binding or charge construction contacted Driver")
	}
	q.Name = "changed"
	q.Policy.Rate++
	q.Policy.Burst++
	for _, once := range []bool{false, true} {
		var err error
		if once {
			_, err = b.AllowOnce(context.Background(), "id", time.Minute, charge)
		} else {
			_, err = b.Allow(context.Background(), charge)
		}
		if err != nil || !reflect.DeepEqual(s.request.Rules, []Rule{original.rule(1)}) {
			t.Fatalf("quota changes affected snapshot: %+v %v", s.request, err)
		}
	}
	if s.calls != 2 {
		t.Fatalf("driver calls=%d, want 2", s.calls)
	}
}

func TestBoundChargePolicyValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Quota)
	}{
		{"different interval", func(q *Quota) { q.Policy.Rate++ }},
		{"different burst", func(q *Quota) { q.Policy.Burst++ }},
		{"zero rate", func(q *Quota) { q.Policy.Rate = 0 }},
		{"negative rate", func(q *Quota) { q.Policy.Rate = -1 }},
		{"zero period", func(q *Quota) { q.Policy.Period = 0 }},
		{"negative period", func(q *Quota) { q.Policy.Period = -1 }},
		// This has the same effective interval, but the policy is itself invalid.
		{"invalid equivalent period", func(q *Quota) { q.Policy.Rate = 25; q.Policy.Period = 25 * time.Hour }},
		{"zero burst", func(q *Quota) { q.Policy.Burst = 0 }},
		{"negative burst", func(q *Quota) { q.Policy.Burst = -1 }},
		{"refill horizon", func(q *Quota) { q.Policy.Burst = 25 }},
		{"burst overflow", func(q *Quota) { q.Policy.Burst = 1 << 62 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &testDriver{}
			l, _ := New(s, "test", "policy-validation")
			quotas := boundTestQuotas()
			b := bindForTest(t, l, quotas...)
			changed := quotas[0]
			tc.change(&changed)
			charges := []Charge{quotas[1].Charge(1), changed.Charge(1)}
			for _, once := range []bool{false, true} {
				var err error
				if once {
					_, err = b.AllowOnce(context.Background(), "id", time.Minute, charges...)
				} else {
					_, err = b.Allow(context.Background(), charges...)
				}
				if !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
					t.Fatalf("once=%t: %v", once, err)
				}
			}
			if s.calls != 0 {
				t.Fatal("invalid or mismatched policy contacted Driver")
			}
		})
	}
}

func TestBoundEquivalentChargePolicies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bound   Policy
		charged Policy
	}{
		{"scaled rate and period", Policy{Rate: 1, Period: time.Minute, Burst: 2}, Policy{Rate: 2, Period: 2 * time.Minute, Burst: 2}},
		{"microsecond rounding", Policy{Rate: 1, Period: 1001 * time.Nanosecond, Burst: 2}, Policy{Rate: 1, Period: 1999 * time.Nanosecond, Burst: 2}},
		{"submicrosecond floor", Policy{Rate: 2, Period: time.Nanosecond, Burst: 2}, Policy{Rate: 1, Period: time.Microsecond, Burst: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
			l, _ := New(s, "test", "equivalent-policy")
			q := Quota{Name: "rpm", Policy: tc.bound}
			b := bindForTest(t, l, q)
			equivalent := Quota{Name: q.Name, Policy: tc.charged}
			for _, once := range []bool{false, true} {
				var err error
				if once {
					_, err = b.AllowOnce(context.Background(), "id", time.Minute, equivalent.Charge(1))
				} else {
					_, err = b.Allow(context.Background(), equivalent.Charge(1))
				}
				if err != nil {
					t.Fatalf("once=%t: equivalent policy rejected: %v", once, err)
				}
				if !reflect.DeepEqual(s.request.Rules, []Rule{q.rule(1)}) {
					t.Fatalf("charge replaced bound policy: %+v", s.request.Rules)
				}
			}
		})
	}
}

func TestBoundObservationAndUnknownOutcome(t *testing.T) {
	var events []Event
	s := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	l, _ := NewWithOptions(s, "test", "observation", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	quotas := boundTestQuotas()
	b := bindForTest(t, l, quotas...)
	charges := []Charge{quotas[1].Charge(2), quotas[0].Charge(1)}
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
