package redisdriver

import (
	"context"
	"testing"

	"github.com/tr1v3r/pacelock"
)

type Policy = pacelock.Policy

func OutcomeOf(err error) Outcome { return pacelock.OutcomeOf(err) }

// Rules are fixture descriptions only. Runtime admission carries names and costs.
func (l *limiter) admitRules(ctx context.Context, rules ...Rule) (Decision, error) {
	request := pacelock.Admission{Scope: pacelock.Scope{Namespace: "test", Domain: "test"}}
	for _, r := range rules {
		request.Charges = append(request.Charges, pacelock.Charge{Name: r.Name, Cost: r.Cost})
	}
	return l.Admit(ctx, request)
}

func publishRules(t testing.TB, l *limiter, rules ...Rule) {
	t.Helper()
	quotas := make([]pacelock.Quota, len(rules))
	for i, r := range rules {
		p := r.Policy
		if p.Revision == 0 {
			p.Revision = 1
		}
		quotas[i] = pacelock.Quota{Name: r.Name, Policy: p}
	}
	if err := l.UpdateQuotas(context.Background(), quotas...); err != nil {
		t.Fatal(err)
	}
}

func (l *limiter) prepare(rules []Rule) ([]string, []interface{}, error) {
	if err := pacelock.Validate(rules...); err != nil {
		return nil, nil, err
	}
	keys := make([]string, 2*len(rules))
	args := make([]interface{}, len(rules))
	for i, r := range rules {
		keys[i] = l.key(r.Name)
		keys[len(rules)+i] = l.policyKey(r.Name)
		args[i] = r.Cost
	}
	return keys, args, nil
}
func namesOf(rules []Rule) []string {
	names := make([]string, len(rules))
	for i, r := range rules {
		names[i] = r.Name
	}
	return names
}
func interval(r Rule) (int64, error) { d, err := r.Interval(); return d.Microseconds(), err }
