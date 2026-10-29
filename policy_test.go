package pacelock

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func managedQuota(rev, rate, burst int64) Quota {
	return Quota{Name: "requests", Policy: Policy{Revision: rev, Rate: rate, Period: time.Second, Burst: burst}}
}
func configs(t *testing.T, qs ...Quota) []QuotaState {
	t.Helper()
	p, err := ApplyPolicies(10000000, qs, make([]QuotaState, len(qs)))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestManagedTransitions(t *testing.T) {
	const now int64 = 10000000
	old := QuotaState{TAT: now + 800000, Interval: 100000, Burst: 10, ExpiresAt: now + 800000}
	debt := []QuotaState{old}
	charges := []Charge{{Name: "requests", Cost: 1}}
	policies := configs(t, managedQuota(1, 20, 2))
	d, next, err := EvaluateManaged(now, charges, policies, debt)
	if err != nil || d.Allowed || next != nil || d.RetryAfter != 750*time.Millisecond {
		t.Fatalf("shrunk capacity: %+v %+v %v", d, next, err)
	}
	paused, err := ApplyPolicies(now, []Quota{managedQuota(2, 0, 2)}, policies)
	if err != nil || debt[0] != old {
		t.Fatal(paused, err)
	}
	for _, at := range []int64{now, now + 2000000} {
		if _, next, err = EvaluateManaged(at, charges, paused, debt); !errors.Is(err, ErrPaused) || next != nil {
			t.Fatal(next, err)
		}
	}
	resumed, err := ApplyPolicies(now+2000000, []Quota{managedQuota(3, 10, 2)}, paused)
	if err != nil || debt[0] != old {
		t.Fatal(resumed, err)
	}
	charges[0].Cost = 2
	if d, _, err = EvaluateManaged(now+2000000, charges, resumed, debt); err != nil || !d.Allowed {
		t.Fatal(d, err)
	}
	if _, err = ApplyPolicies(now+2000000, []Quota{managedQuota(1, 10, 2)}, resumed); !errors.Is(err, ErrPolicyMismatch) {
		t.Fatal("fence expired", err)
	}
	// Fresh managed admission carries only names and costs, unchanged across updates.
	if _, _, err = EvaluateManaged(now, charges, []QuotaState{{}}, debt); !errors.Is(err, ErrPolicyMissing) {
		t.Fatal(err)
	}
}

func TestManagedUpdateAtomicAndFenced(t *testing.T) {
	q := managedQuota(2, 10, 10)
	state := configs(t, q)
	equal := q
	equal.Policy.Rate = 20
	equal.Policy.Period = 2 * time.Second
	got, err := ApplyPolicies(10000000, []Quota{equal}, state)
	if err != nil || !reflect.DeepEqual(got, state) {
		t.Fatal(got, err)
	}
	for _, bad := range []Quota{managedQuota(1, 10, 10), managedQuota(2, 20, 10)} {
		if got, err = ApplyPolicies(10000000, []Quota{bad}, state); !errors.Is(err, ErrPolicyMismatch) || got != nil {
			t.Fatal(got, err)
		}
	}
	second := managedQuota(1, 10, 10)
	second.Name = "second"
	states := []QuotaState{state[0], {Revision: 2, Interval: 100000, Burst: 10}}
	snapshot := append([]QuotaState(nil), states...)
	if got, err = ApplyPolicies(10000000, []Quota{managedQuota(3, 20, 10), second}, states); !errors.Is(err, ErrPolicyMismatch) || got != nil || !reflect.DeepEqual(states, snapshot) {
		t.Fatal(got, err, states)
	}
}

func TestManagedValidation(t *testing.T) {
	for _, q := range []Quota{managedQuota(0, 0, 10), managedQuota(-1, 10, 10), managedQuota(MaxRevision+1, 10, 10), managedQuota(1, -1, 10), managedQuota(1, 0, 0)} {
		if _, err := q.rule(1).Interval(); !errors.Is(err, ErrInvalid) {
			t.Fatal(q, err)
		}
	}
	q := managedQuota(1, 0, 10)
	if interval, err := q.rule(1).Interval(); interval != 0 || err != nil {
		t.Fatal(interval, err)
	}
	if _, err := ApplyPolicies(100, []Quota{q}, []QuotaState{{Revision: 1, Interval: 0, Burst: 10, TAT: 100, ExpiresAt: 100}}); !errors.Is(err, ErrState) {
		t.Fatal("debt accepted as config", err)
	}
	a := Admission{Scope: Scope{Namespace: "ns", Domain: "d"}, Charges: []Charge{{Name: "q", Cost: 1}}}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]Charge{nil, {{Name: "", Cost: 1}}, {{Name: "q", Cost: 0}}, {{Name: "q", Cost: MaxHorizon.Microseconds() + 1}}, {{Name: "q", Cost: 1}, {Name: "q", Cost: 2}}} {
		a.Charges = bad
		if err := a.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatal(bad, err)
		}
	}
}

func TestManagedEvaluationErrors(t *testing.T) {
	charges := []Charge{{Name: "requests", Cost: 2}}
	good := configs(t, managedQuota(1, 10, 1))
	if _, next, err := EvaluateManaged(10000000, charges, good, []QuotaState{{}}); !errors.Is(err, ErrInvalid) || next != nil {
		t.Fatal(next, err)
	}
	paused := configs(t, managedQuota(2, 0, 1))
	if _, next, err := EvaluateManaged(10000000, charges, paused, []QuotaState{{}}); !errors.Is(err, ErrPaused) || next != nil {
		t.Fatal(next, err)
	}
	charges[0].Cost = 1
	bad := []QuotaState{{TAT: 100, ExpiresAt: 99, Interval: 1, Burst: 1}}
	if _, _, err := EvaluateManaged(10000000, charges, good, bad); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	badcfg := append([]QuotaState(nil), good...)
	badcfg[0].Revision = -1
	if _, _, err := EvaluateManaged(10000000, charges, badcfg, []QuotaState{{}}); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
}

type managedTestDriver struct {
	testDriver
	policies       map[string]QuotaState
	debts          map[string]QuotaState
	managedCalls   int
	managedRequest Admission
	override       *Decision
}

func (d *managedTestDriver) UpdateQuotas(ctx context.Context, u QuotaBatch) error {
	if err := ctx.Err(); err != nil {
		return failure("update_quotas", NotCharged, err)
	}
	states := make([]QuotaState, len(u.Quotas))
	for i, q := range u.Quotas {
		states[i] = d.policies[q.Name]
	}
	next, err := ApplyPolicies(time.Now().UnixMicro(), u.Quotas, states)
	if err != nil {
		return failure("update_quotas", NotCharged, err)
	}
	for i, q := range u.Quotas {
		d.policies[q.Name] = next[i]
	}
	return nil
}
func (d *managedTestDriver) Admit(_ context.Context, a Admission) (Decision, error) {
	d.managedCalls++
	d.managedRequest = a
	if d.override != nil {
		return *d.override, nil
	}
	configs := make([]QuotaState, len(a.Charges))
	debts := make([]QuotaState, len(a.Charges))
	for i, c := range a.Charges {
		configs[i] = d.policies[c.Name]
		debts[i] = d.debts[c.Name]
	}
	decision, next, err := EvaluateManaged(time.Now().UnixMicro(), a.Charges, configs, debts)
	if err != nil {
		return Decision{}, failure("allow", NotCharged, err)
	}
	if decision.Allowed {
		for i, c := range a.Charges {
			d.debts[c.Name] = next[i]
		}
	}
	return decision, nil
}

func TestManagedFacade(t *testing.T) {
	ctx := context.Background()
	d := &managedTestDriver{policies: map[string]QuotaState{}, debts: map[string]QuotaState{}}
	l, _ := New(d, "ns", "d")
	q := managedQuota(1, 0, 10)
	c := Charge{Name: q.Name, Cost: 1}
	if _, err := l.Allow(ctx, c); !errors.Is(err, ErrPolicyMissing) {
		t.Fatal(err)
	}
	if err := l.UpdateQuotas(ctx, q); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Allow(ctx, c); !errors.Is(err, ErrPaused) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	q.Policy.Revision++
	q.Policy.Rate = 10
	if err := l.UpdateQuotas(ctx, q); err != nil {
		t.Fatal(err)
	}
	if decision, err := l.Allow(ctx, c); err != nil || !decision.Allowed {
		t.Fatal(decision, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	count := d.managedCalls
	if _, err := l.Allow(canceled, c); !errors.Is(err, context.Canceled) || d.managedCalls != count {
		t.Fatal(err)
	}
	if err := l.UpdateQuotas(canceled, q); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	d.override = &Decision{Allowed: false}
	if _, err := l.Allow(ctx, c); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	unsupported, _ := New(&testDriver{}, "ns", "d")
	if err := unsupported.UpdateQuotas(ctx, q); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
}

func TestUpdateQuotasErrorOperation(t *testing.T) {
	var _ QuotaDriver = (*managedTestDriver)(nil)
	l, err := New(&testDriver{}, "ns", "d")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		quotas []Quota
		want   error
	}{
		{name: "invalid", want: ErrInvalid},
		{name: "unsupported", quotas: []Quota{managedQuota(1, 10, 10)}, want: ErrUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := l.UpdateQuotas(context.Background(), tc.quotas...)
			var failure *Error
			if !errors.Is(err, tc.want) || !errors.As(err, &failure) || failure.Op != "update_quotas" || failure.Outcome != NotCharged {
				t.Fatalf("quota update error: %+v", err)
			}
		})
	}
}

func TestManagedObserver(t *testing.T) {
	d := &managedTestDriver{override: &Decision{Allowed: true}}
	var events []Event
	l, _ := NewWithOptions(d, "ns", "d", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	if _, err := l.Allow(context.Background(), Charge{Name: "q", Cost: 1}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Operation != "allow" || events[0].Result != "allowed" || events[0].RuleCount != 1 {
		t.Fatal(events)
	}
}
