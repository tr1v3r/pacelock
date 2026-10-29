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
	a.Charges = []Charge{{Name: "q", Cost: 1}}
	a.Retention = time.Second
	if err := a.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	a.ID = "id"
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	a.Retention = time.Nanosecond
	if err := a.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	a.Retention = time.Second
	a.Charges[0].Name = string([]byte{0xff})
	if err := a.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
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
	override       *Receipt
}

func (d *managedTestDriver) UpdateQuotas(ctx context.Context, u QuotaUpdate) error {
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
func (d *managedTestDriver) Admit(_ context.Context, a Admission) (Receipt, error) {
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
		return Receipt{}, failure("allow", NotCharged, err)
	}
	if decision.Allowed {
		for i, c := range a.Charges {
			d.debts[c.Name] = next[i]
		}
	}
	return Receipt{Decision: decision}, nil
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
	if _, err := l.AllowStatic(ctx, q.rule(1)); !errors.Is(err, ErrInvalid) || d.calls != 0 {
		t.Fatal(err)
	}
	if _, err := l.BindStatic(q); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := l.AllowOnce(ctx, "", time.Minute, c); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
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
	d.override = &Receipt{Decision: Decision{Allowed: true}, Replayed: true}
	if _, err := l.Allow(ctx, c); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
		t.Fatal(err)
	}
	if r, err := l.AllowOnce(ctx, "id", time.Minute, c); err != nil || !r.Replayed || d.managedRequest.ID != "id" {
		t.Fatal(r, err)
	}
	d.override = &Receipt{Decision: Decision{Allowed: false}}
	if _, err := l.Allow(ctx, c); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	unsupported, _ := New(&testDriver{}, "ns", "d")
	if _, err := unsupported.Allow(ctx, c); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
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

func TestManagedObserverReplay(t *testing.T) {
	d := &managedTestDriver{override: &Receipt{Decision: Decision{Allowed: true}, Replayed: true}}
	var events []Event
	l, err := NewWithOptions(d, "ns", "d", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	if err != nil {
		t.Fatal(err)
	}
	c := Charge{Name: "q", Cost: 1}
	if _, err = l.AllowOnce(context.Background(), "id", time.Minute, c); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Operation != "allow_once" || !events[0].Replayed || events[0].Result != "allowed" {
		t.Fatal(events)
	}
	d.override.Replayed = false
	if _, err = l.Allow(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].Operation != "allow" || events[1].Replayed || events[1].RuleCount != 1 {
		t.Fatal(events)
	}
}

func TestManagedFingerprint(t *testing.T) {
	a := Admission{Charges: []Charge{{Name: "q", Cost: 1}}, Retention: time.Minute}
	before := a.Fingerprint()
	a.Charges[0].Cost++
	if a.Fingerprint() == before {
		t.Fatal("cost omitted")
	}
	a.Charges[0].Cost = 1
	a.ID = "different-id"
	a.Scope = Scope{Namespace: "other", Domain: "other"}
	if a.Fingerprint() != before {
		t.Fatal("identity should select record, not fingerprint")
	}
	static := StaticAdmission{Rules: []Rule{{Name: "q", Cost: 1, Policy: Policy{Rate: 1, Period: time.Second, Burst: 1}}}, Retention: time.Minute}
	if static.Fingerprint() == before {
		t.Fatal("missing API discriminator")
	}
}
