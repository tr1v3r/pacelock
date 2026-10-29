package postgres

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/tr1v3r/pacelock"
)

// The strict script is also a query-count assertion: an extra per-quota query
// fails immediately, for both minimum and maximum supported batch sizes.
func TestAdmissionBatchMappingAndBoundedQueries(t *testing.T) {
	for _, n := range []int{1, pacelock.MaxRules} {
		for _, denied := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/denied=%v", n, denied), func(t *testing.T) {
				a := pacelock.Admission{Scope: testScope}
				policies := make([]pacelock.QuotaState, n)
				states := make([]pacelock.QuotaState, n)
				pr, dr := policyRead(pacelock.QuotaState{}), debtRead(pacelock.QuotaState{})
				for i := 0; i < n; i++ {
					a.Charges = append(a.Charges, pacelock.Charge{Name: fmt.Sprintf("quota-%d", i), Cost: int64(i + 1)})
					policies[i] = pacelock.QuotaState{Interval: int64(1000 + i), Burst: 64, Revision: int64(i + 1)}
					// Omit alternating debt rows. Missing debt must not shift later rows.
					if i%2 == 0 {
						states[i] = pacelock.QuotaState{TAT: testNow + 1000, Interval: 1000, Burst: 64, ExpiresAt: testNow + 1000}
					}
				}
				if denied {
					states[n-1] = pacelock.QuotaState{TAT: testNow + 1000000, Interval: 1000, Burst: 64, ExpiresAt: testNow + 1000000}
				}
				for i := n - 1; i >= 0; i-- {
					p, s := policies[i], states[i]
					id := itemID(a.Charges[i].Name)
					pr.rows = append(pr.rows, []driver.Value{id, p.Interval, p.Burst, p.Revision})
					if s != (pacelock.QuotaState{}) {
						dr.rows = append(dr.rows, []driver.Value{id, s.TAT, s.Interval, s.Burst, s.ExpiresAt})
					}
				}
				pr.check = func(args []driver.NamedValue) {
					if len(args) != n+1 || args[0].Value != scopeID(testScope) {
						t.Fatalf("read arguments: %v", args)
					}
					for i, c := range a.Charges {
						if args[i+1].Value != itemID(c.Name) {
							t.Fatalf("read order: %v", args)
						}
					}
				}
				want, next, err := pacelock.EvaluateManaged(testNow, a.Charges, policies, states)
				if err != nil {
					t.Fatal(err)
				}
				steps := append(lockedSteps(), pr, dr)
				if !denied {
					steps = append(steps, sqlStep{kind: "exec", contains: "INSERT INTO pacelock_quotas", check: func(args []driver.NamedValue) {
						if len(args) != 6*n {
							t.Fatalf("write arguments: %d", len(args))
						}
						for i, s := range next {
							got := []any{args[6*i].Value, args[6*i+1].Value, args[6*i+2].Value, args[6*i+3].Value, args[6*i+4].Value, args[6*i+5].Value}
							expected := []any{scopeID(testScope), itemID(a.Charges[i].Name), s.TAT, s.Interval, s.Burst, s.ExpiresAt}
							if !reflect.DeepEqual(got, expected) {
								t.Fatalf("write %d: %v want %v", i, got, expected)
							}
						}
					}})
				}
				steps = append(steps, sqlStep{kind: "commit"})
				got, err := scripted(t, steps).Admit(context.Background(), a)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("got %+v %v want %+v", got, err, want)
				}
			})
		}
	}
}

func TestBatchMissingPolicyAndBadScanRollBack(t *testing.T) {
	for _, mode := range []string{"missing", "scan", "unexpected ID"} {
		t.Run(mode, func(t *testing.T) {
			pr := policyRead(pacelock.QuotaState{Interval: 1000, Burst: 3, Revision: 1})
			want := pacelock.ErrPolicyMissing
			switch mode {
			case "scan":
				pr.rows[0][1] = "not an integer"
				want = pacelock.ErrBackend
			case "unexpected ID":
				pr.rows[0][0] = itemID("unrequested")
				want = pacelock.ErrState
			}
			steps := append(lockedSteps(), pr)
			if mode == "missing" {
				steps = append(steps, debtRead(pacelock.QuotaState{}))
			}
			steps = append(steps, sqlStep{kind: "rollback"})
			a := managed(1)
			a.Charges = append(a.Charges, pacelock.Charge{Name: "absent", Cost: 1})
			_, err := scripted(t, steps).Admit(context.Background(), a)
			if !errors.Is(err, want) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestUpdateBatchOnlyChangedRows(t *testing.T) {
	u := pacelock.QuotaUpdate{Scope: testScope}
	pr := policyRead(pacelock.QuotaState{})
	var changed []string
	for i := 0; i < pacelock.MaxRules; i++ {
		q := quota(2, 1, 3)
		q.Name = fmt.Sprintf("q-%d", i)
		u.Quotas = append(u.Quotas, q)
		revision := int64(2)
		if i%2 == 0 {
			revision = 1
			changed = append(changed, itemID(q.Name))
		}
		pr.rows = append([][]driver.Value{{itemID(q.Name), int64(1000000), int64(3), revision}}, pr.rows...)
	}
	steps := append(lockedSteps(), pr, sqlStep{kind: "exec", contains: "INSERT INTO pacelock_policies", check: func(args []driver.NamedValue) {
		if len(args) != len(changed)*5 {
			t.Fatalf("args: %v", args)
		}
		for i, id := range changed {
			if args[5*i+1].Value != id || args[5*i+4].Value != int64(2) {
				t.Fatalf("args: %v", args)
			}
		}
	}}, sqlStep{kind: "commit"})
	if err := scripted(t, steps).UpdateQuotas(context.Background(), u); err != nil {
		t.Fatal(err)
	}
}

func TestBatchIterationFailureRollsBack(t *testing.T) {
	for _, policy := range []bool{true, false} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			failure := errors.New("row stream interrupted")
			pr := policyRead(pacelock.QuotaState{Interval: 1000, Burst: 3, Revision: 1})
			dr := debtRead(pacelock.QuotaState{})
			steps := lockedSteps()
			if policy {
				pr.rowsErr = failure
				steps = append(steps, pr)
			} else {
				dr.rowsErr = failure
				steps = append(steps, pr, dr)
			}
			steps = append(steps, sqlStep{kind: "rollback"})
			_, err := scripted(t, steps).Admit(context.Background(), managed(1))
			if !errors.Is(err, failure) || !errors.Is(err, pacelock.ErrBackend) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestPlaceholders(t *testing.T) {
	if got := placeholders(2, 3); got != "$2,$3,$4" {
		t.Fatal(got)
	}
	got := placeholders(1, 6*pacelock.MaxRules)
	if strings.Count(got, "$") != 192 || !strings.HasSuffix(got, "$192") {
		t.Fatal(got)
	}
}
