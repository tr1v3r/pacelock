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

func staticForTest(t *testing.T, l *Limiter, quotas ...Quota) *StaticLimiter {
	t.Helper()
	b, err := NewStaticWithOptions(l.core.driver, l.core.scope.Namespace, l.core.scope.Domain, Options{Observer: l.core.observer}, quotas...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func staticTestQuotas() []Quota {
	return []Quota{
		{Name: "rpm", Policy: Policy{Rate: 1, Period: time.Hour, Burst: 2}},
		{Name: "tokens", Policy: Policy{Rate: 1, Period: time.Hour, Burst: 5}},
	}
}

func TestNewStaticValidation(t *testing.T) {
	s := &testDriver{}
	l, _ := New(s, "test", "bind")
	var typedNil *testDriver
	for _, driver := range []Driver{nil, typedNil} {
		if b, err := NewStatic(driver, "test", "static", staticTestQuotas()...); b != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid driver: %v %v", b, err)
		}
	}
	for _, scope := range []Scope{{}, {Namespace: "test"}, {Domain: "static"}, {Namespace: strings.Repeat("n", 257), Domain: "static"}, {Namespace: "test", Domain: strings.Repeat("d", 257)}} {
		if b, err := NewStatic(s, scope.Namespace, scope.Domain, staticTestQuotas()...); b != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid scope: %v %v", b, err)
		}
	}
	for _, quotas := range [][]Quota{nil, make([]Quota, MaxRules+1), {staticTestQuotas()[0], staticTestQuotas()[0]}} {
		if b, err := NewStatic(s, "test", "static", quotas...); b != nil || !errors.Is(err, ErrInvalid) {
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
			q := staticTestQuotas()[0]
			tc.change(&q)
			if b, err := NewStatic(s, "test", "static", q); b != nil || !errors.Is(err, ErrInvalid) {
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
	b := staticForTest(t, l, quotas...)
	if s.calls != 0 {
		t.Fatal("construction or local validation contacted Driver")
	}
	s.result = Receipt{Decision: Decision{Allowed: true}}
	if d, err := b.Allow(context.Background(), charges...); err != nil || !d.Allowed {
		t.Fatalf("maximum quota set: %+v %v", d, err)
	}
}

func TestStaticValidationBeforeDriver(t *testing.T) {
	s := &testDriver{}
	var events []Event
	l, _ := NewWithOptions(s, "test", "validation", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	quotas := staticTestQuotas()
	rpm, tokens := quotas[0], quotas[1]
	b := staticForTest(t, l, quotas...)
	invalid := [][]Charge{
		nil,
		{Charge{Name: rpm.Name, Cost: 0}},
		{Charge{Name: rpm.Name, Cost: -1}},
		{Charge{Name: rpm.Name, Cost: 3}},
		{Charge{Name: "unknown", Cost: 1}},
		{{}},
		{Charge{Name: rpm.Name, Cost: 1}, Charge{Name: tokens.Name, Cost: 1}, Charge{Name: "extra", Cost: 1}},
		{Charge{Name: rpm.Name, Cost: 1}, Charge{Name: rpm.Name, Cost: 1}},
		{Charge{Name: rpm.Name, Cost: 1}, Charge{Name: "unknown", Cost: 1}},
		{Charge{Name: rpm.Name, Cost: 1}, Charge{Cost: 1}},
		{Charge{Name: rpm.Name, Cost: 0}, Charge{Name: tokens.Name, Cost: 1}},
		{Charge{Name: rpm.Name, Cost: -1}, Charge{Name: tokens.Name, Cost: 1}},
		{Charge{Name: rpm.Name, Cost: 3}, Charge{Name: tokens.Name, Cost: 1}},
		{Charge{Name: rpm.Name, Cost: 1}, {}},
	}
	for _, charges := range invalid {
		for _, op := range []string{"allow_static", "allow_static_once"} {
			var err error
			if op == "allow_static" {
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
	for _, current := range []*StaticLimiter{nil, {}, b} {
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
	for _, current := range []*StaticLimiter{nil, {}} {
		if _, err := current.Allow(context.Background(), Charge{Name: rpm.Name, Cost: 1}); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatal(err)
		}
		if _, err := current.AllowOnce(context.Background(), "id", time.Minute, Charge{Name: rpm.Name, Cost: 1}); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatal(err)
		}
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
	charges := []Charge{Charge{Name: rpm.Name, Cost: 1}, Charge{Name: tokens.Name, Cost: 1}}
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
	other := staticForTest(t, l, opaqueQuota)
	if _, err := other.AllowOnce(context.Background(), "id", time.Minute, Charge{Name: opaqueQuota.Name, Cost: 1}); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if s.calls != 0 {
		t.Fatal("invalid admission contacted Driver")
	}
}

func TestStaticQuotaSnapshot(t *testing.T) {
	s := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	l, _ := New(s, "test", "snapshot")
	quotas := staticTestQuotas()[:1]
	original := quotas[0]
	b := staticForTest(t, l, quotas...)
	charge := Charge{Name: original.Name, Cost: 1}
	if s.calls != 0 {
		t.Fatal("limiter or charge construction contacted Driver")
	}
	quotas[0].Name = "changed"
	quotas[0].Policy.Rate++
	quotas[0].Policy.Burst++
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

// Equivalent construction policies preserve the driver protocol fingerprint.
func TestStaticEquivalentPolicyFingerprints(t *testing.T) {
	for _, policies := range [][2]Policy{
		{{Rate: 1, Period: time.Minute, Burst: 2}, {Rate: 2, Period: 2 * time.Minute, Burst: 2}},
		{{Rate: 1, Period: 1001 * time.Nanosecond, Burst: 2}, {Rate: 1, Period: 1999 * time.Nanosecond, Burst: 2}},
		{{Rate: 2, Period: time.Nanosecond, Burst: 2}, {Rate: 1, Period: time.Microsecond, Burst: 2}},
	} {
		d := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
		var fingerprint string
		for i, policy := range policies {
			l, err := NewStatic(d, "test", "equivalent", Quota{Name: "q", Policy: policy})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := l.AllowOnce(context.Background(), "id", time.Minute, sampleCharge()); err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				fingerprint = d.request.Fingerprint()
			} else if fingerprint != d.request.Fingerprint() {
				t.Fatal("equivalent policy changed fingerprint")
			}
		}
	}
}

func TestStaticSubsetNormalization(t *testing.T) {
	var events []Event
	s := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	l, _ := NewWithOptions(s, "test", "subsets", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	quotas := append(staticTestQuotas(), Quota{Name: "third", Policy: staticTestQuotas()[0].Policy})
	b := staticForTest(t, l, quotas...)
	beforeRules := append([]Rule(nil), b.rules...)
	beforeIndex := make(map[string]int)
	for name, i := range b.index {
		beforeIndex[name] = i
	}
	for _, selected := range [][]Charge{
		{Charge{Name: quotas[1].Name, Cost: 3}},
		{Charge{Name: quotas[2].Name, Cost: 2}, Charge{Name: quotas[0].Name, Cost: 1}},
		{Charge{Name: quotas[0].Name, Cost: 1}, Charge{Name: quotas[2].Name, Cost: 2}},
	} {
		before := append([]Charge(nil), selected...)
		want := []Rule{quotas[1].rule(3)}
		if len(selected) == 2 {
			want = []Rule{quotas[0].rule(1), quotas[2].rule(2)}
		}
		for _, once := range []bool{false, true} {
			var err error
			if once {
				_, err = b.AllowOnce(context.Background(), "id", time.Minute, selected...)
			} else {
				_, err = b.Allow(context.Background(), selected...)
			}
			if err != nil || !reflect.DeepEqual(s.request.Rules, want) {
				t.Fatalf("once=%t: rules=%+v want=%+v err=%v", once, s.request.Rules, want, err)
			}
			e := events[len(events)-1]
			if e.RuleCount != len(selected) || e.Result != "allowed" {
				t.Fatalf("subset observation: %+v", e)
			}
			if !reflect.DeepEqual(selected, before) || !reflect.DeepEqual(b.rules, beforeRules) || !reflect.DeepEqual(b.index, beforeIndex) {
				t.Fatal("subset admission mutated caller or binding")
			}
		}
	}
	if len(events) != 6 || s.calls != 6 {
		t.Fatalf("events=%d calls=%d", len(events), s.calls)
	}
}

func TestStaticObservationAndUnknownOutcome(t *testing.T) {
	var events []Event
	s := &testDriver{result: Receipt{Decision: Decision{Allowed: true}}}
	l, _ := NewWithOptions(s, "test", "observation", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	quotas := staticTestQuotas()
	b := staticForTest(t, l, quotas...)
	charges := []Charge{Charge{Name: quotas[1].Name, Cost: 2}, Charge{Name: quotas[0].Name, Cost: 1}}
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
	for _, op := range []string{"allow_static", "allow_static_once"} {
		var err error
		if op == "allow_static" {
			_, err = b.Allow(context.Background(), charges...)
		} else {
			_, err = b.AllowOnce(context.Background(), "id", time.Minute, charges...)
		}
		if !errors.Is(err, ErrBackend) || OutcomeOf(err) != Unknown {
			t.Fatalf("%s lost unknown outcome: %v", op, err)
		}
	}
	if len(events) != 4 || events[0].Result != "allowed" || events[0].RuleCount != 2 || !events[1].Replayed || events[1].Operation != "allow_static_once" || events[2].Result != "error" || events[3].Operation != "allow_static_once" || s.calls != 4 {
		t.Fatalf("events=%+v driver calls=%d", events, s.calls)
	}
}
