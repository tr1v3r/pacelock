package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/tr1v3r/pacelock"
	"io"
	"strings"
	"testing"
	"time"
)

// Strict transport scripts verify SQL ordering and failure boundaries without a
// runtime transport dependency. The shared contract module exercises real PG.
type sqlStep struct {
	kind, contains string
	rows           [][]driver.Value
	err            error
	check          func([]driver.NamedValue)
}
type scriptConn struct {
	t     *testing.T
	steps []sqlStep
}

func (c *scriptConn) take(kind, query string, args []driver.NamedValue) sqlStep {
	c.t.Helper()
	if len(c.steps) == 0 {
		c.t.Fatalf("unexpected %s: %s", kind, query)
	}
	s := c.steps[0]
	c.steps = c.steps[1:]
	if s.kind != kind || !strings.Contains(query, s.contains) {
		c.t.Fatalf("got %s %q, want %s containing %q", kind, query, s.kind, s.contains)
	}
	if s.check != nil {
		s.check(args)
	}
	return s
}
func (c *scriptConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (c *scriptConn) Close() error { return nil }
func (c *scriptConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *scriptConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	s := c.take("begin", "", nil)
	return c, s.err
}
func (c *scriptConn) Commit() error   { return c.take("commit", "", nil).err }
func (c *scriptConn) Rollback() error { return c.take("rollback", "", nil).err }
func (c *scriptConn) ExecContext(_ context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	s := c.take("exec", q, a)
	return driver.RowsAffected(1), s.err
}
func (c *scriptConn) QueryContext(_ context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	s := c.take("query", q, a)
	return &scriptRows{rows: s.rows}, s.err
}

type scriptRows struct{ rows [][]driver.Value }

func (r *scriptRows) Columns() []string {
	n := 1
	if len(r.rows) > 0 {
		n = len(r.rows[0])
	}
	return make([]string, n)
}
func (r *scriptRows) Close() error { return nil }
func (r *scriptRows) Next(dest []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dest, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}

type scriptConnector struct{ conn *scriptConn }

func (s scriptConnector) Connect(context.Context) (driver.Conn, error) { return s.conn, nil }
func (s scriptConnector) Driver() driver.Driver                        { return scriptDriver{s.conn} }

type scriptDriver struct{ conn *scriptConn }

func (s scriptDriver) Open(string) (driver.Conn, error) { return s.conn, nil }
func scripted(t *testing.T, steps []sqlStep) *Driver {
	t.Helper()
	c := &scriptConn{t: t, steps: steps}
	db := sql.OpenDB(scriptConnector{c})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		db.Close()
		if len(c.steps) != 0 {
			t.Errorf("%d unconsumed SQL steps: %+v", len(c.steps), c.steps)
		}
	})
	d, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

const testNow int64 = 1700000000000000

var testScope = pacelock.Scope{Namespace: "test", Domain: "dynamic"}

func lockedSteps() []sqlStep {
	return []sqlStep{
		{kind: "begin"}, {kind: "exec", contains: "INSERT INTO pacelock_domains"},
		{kind: "query", contains: "FOR UPDATE", rows: [][]driver.Value{{"locked"}}},
		{kind: "query", contains: "clock_timestamp()", rows: [][]driver.Value{{testNow}}},
	}
}
func policyRead(s pacelock.QuotaState) sqlStep {
	x := sqlStep{kind: "query", contains: "SELECT interval_us,burst,revision FROM pacelock_policies"}
	if s != (pacelock.QuotaState{}) {
		x.rows = [][]driver.Value{{s.Interval, s.Burst, s.Revision}}
	}
	return x
}
func debtRead(s pacelock.QuotaState) sqlStep {
	x := sqlStep{kind: "query", contains: "SELECT tat_us,interval_us,burst,expires_us FROM pacelock_quotas"}
	if s != (pacelock.QuotaState{}) {
		x.rows = [][]driver.Value{{s.TAT, s.Interval, s.Burst, s.ExpiresAt}}
	}
	return x
}
func quota(rev, rate, burst int64) pacelock.Quota {
	return pacelock.Quota{Name: "requests", Policy: pacelock.Policy{Revision: rev, Rate: rate, Period: time.Second, Burst: burst}}
}
func update(q ...pacelock.Quota) pacelock.QuotaUpdate {
	return pacelock.QuotaUpdate{Scope: testScope, Quotas: q}
}
func managed(cost int64) pacelock.Admission {
	return pacelock.Admission{Scope: testScope, Charges: []pacelock.Charge{{Name: "requests", Cost: cost}}}
}
func static() pacelock.StaticAdmission {
	q := quota(0, 1, 3)
	return pacelock.StaticAdmission{Scope: testScope, Rules: []pacelock.Rule{{Name: q.Name, Policy: q.Policy, Cost: 1}}}
}
func policyWrite(t *testing.T, want pacelock.QuotaState) sqlStep {
	return sqlStep{kind: "exec", contains: "INSERT INTO pacelock_policies", check: func(a []driver.NamedValue) {
		got := pacelock.QuotaState{Interval: a[2].Value.(int64), Burst: a[3].Value.(int64), Revision: a[4].Value.(int64)}
		if got != want {
			t.Fatalf("stored %+v want %+v", got, want)
		}
	}}
}
func debtWrite(t *testing.T, want pacelock.QuotaState) sqlStep {
	return sqlStep{kind: "exec", contains: "INSERT INTO pacelock_quotas", check: func(a []driver.NamedValue) {
		if len(a) != 6 {
			t.Fatalf("debt has extra policy fields: %v", a)
		}
		got := pacelock.QuotaState{TAT: a[2].Value.(int64), Interval: a[3].Value.(int64), Burst: a[4].Value.(int64), ExpiresAt: a[5].Value.(int64)}
		if got != want {
			t.Fatalf("stored %+v want %+v", got, want)
		}
	}}
}
func TestUpdateQuotasTouchesConfigurationOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		old  pacelock.QuotaState
		q    pacelock.Quota
	}{
		{"new", pacelock.QuotaState{}, quota(1, 1, 3)},
		{"raise", pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 1}, quota(2, 10, 3)},
		{"shrink", pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 1}, quota(2, 1, 1)},
		{"pause", pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 1}, quota(2, 0, 3)},
		{"resume", pacelock.QuotaState{Burst: 3, Revision: 2}, quota(3, 1, 3)},
		{"idempotent no write", pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 1}, quota(1, 1, 3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			step, _ := (pacelock.Rule{Name: tc.q.Name, Policy: tc.q.Policy, Cost: 1}).Interval()
			want := pacelock.QuotaState{Interval: step.Microseconds(), Burst: tc.q.Policy.Burst, Revision: tc.q.Policy.Revision}
			steps := append(lockedSteps(), policyRead(tc.old))
			if want == tc.old {
				steps = append(steps, sqlStep{kind: "rollback"})
			} else {
				steps = append(steps, policyWrite(t, want), sqlStep{kind: "commit"})
			}
			if err := scripted(t, steps).UpdateQuotas(context.Background(), update(tc.q)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestUpdateAtomicValidationAndWriteFailure(t *testing.T) {
	for _, mode := range []string{"stale", "same conflict", "write failure"} {
		t.Run(mode, func(t *testing.T) {
			q := quota(3, 1, 3)
			other := quota(1, 2, 3)
			other.Name = "other"
			old := pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 2}
			if mode == "same conflict" {
				other.Policy.Revision = 2
			}
			steps := append(lockedSteps(), policyRead(pacelock.QuotaState{}))
			want := pacelock.ErrPolicyMismatch
			if mode == "write failure" {
				want = errors.New("second write rejected")
				steps = append(steps, policyRead(pacelock.QuotaState{}), sqlStep{kind: "exec", contains: "INSERT INTO pacelock_policies"}, sqlStep{kind: "exec", contains: "INSERT INTO pacelock_policies", err: want})
			} else {
				steps = append(steps, policyRead(old))
			}
			steps = append(steps, sqlStep{kind: "rollback"})
			err := scripted(t, steps).UpdateQuotas(context.Background(), update(q, other))
			if !errors.Is(err, want) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
				t.Fatalf("got %v", err)
			}
		})
	}
}
func TestUpdateCommitUnknown(t *testing.T) {
	failure := errors.New("commit reply lost")
	steps := append(lockedSteps(), policyRead(pacelock.QuotaState{}), sqlStep{kind: "exec", contains: "INSERT INTO pacelock_policies"}, sqlStep{kind: "commit", err: failure})
	err := scripted(t, steps).UpdateQuotas(context.Background(), update(quota(1, 1, 3)))
	if !errors.Is(err, failure) || pacelock.OutcomeOf(err) != pacelock.Unknown {
		t.Fatalf("got %v", err)
	}
}
func TestCancelledBeforeDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := scripted(t, nil)
	if err := d.UpdateQuotas(ctx, update(quota(1, 1, 1))); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := d.Admit(ctx, managed(1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestManagedAdmissionUsesCurrentPolicyAndOriginalDebt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		p, debt pacelock.QuotaState
		allowed bool
	}{
		{"new", pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 2}, pacelock.QuotaState{}, true},
		{"rate changed", pacelock.QuotaState{Interval: 500000, Burst: 3, Revision: 9}, pacelock.QuotaState{TAT: testNow + 1000000, Interval: 1000000, Burst: 3, ExpiresAt: testNow + 1000000}, true},
		{"shrink preserves debt", pacelock.QuotaState{Interval: 1000000, Burst: 1, Revision: 9}, pacelock.QuotaState{TAT: testNow + 3000000, Interval: 1000000, Burst: 3, ExpiresAt: testNow + 3000000}, false},
		{"idle", pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 9}, pacelock.QuotaState{TAT: testNow - 1000, Interval: 1000000, Burst: 3, ExpiresAt: testNow - 1000}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			steps := append(lockedSteps(), policyRead(tc.p), debtRead(tc.debt))
			if tc.allowed {
				tat := max(testNow, tc.debt.TAT) + tc.p.Interval
				want := pacelock.QuotaState{TAT: tat, Interval: tc.p.Interval, Burst: tc.p.Burst, ExpiresAt: tat}
				steps = append(steps, debtWrite(t, want))
			}
			steps = append(steps, sqlStep{kind: "commit"})
			r, err := scripted(t, steps).Admit(context.Background(), managed(1))
			if err != nil || r.Allowed != tc.allowed {
				t.Fatalf("got %+v %v", r, err)
			}
		})
	}
}
func TestManagedAdmissionFailuresDoNotWrite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		p, debt pacelock.QuotaState
		cost    int64
		want    error
	}{
		{"missing", pacelock.QuotaState{}, pacelock.QuotaState{}, 1, pacelock.ErrPolicyMissing},
		{"paused even over burst", pacelock.QuotaState{Burst: 1, Revision: 2}, pacelock.QuotaState{}, 2, pacelock.ErrPaused},
		{"over burst", pacelock.QuotaState{Interval: 1000000, Burst: 1, Revision: 2}, pacelock.QuotaState{}, 2, pacelock.ErrInvalid},
		{"bad policy", pacelock.QuotaState{Interval: -1, Burst: 1, Revision: 2}, pacelock.QuotaState{}, 1, pacelock.ErrState},
		{"bad old debt", pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 2}, pacelock.QuotaState{TAT: testNow, Interval: -1, Burst: 3, ExpiresAt: testNow}, 1, pacelock.ErrState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := managed(tc.cost)
			steps := append(lockedSteps(), policyRead(tc.p), debtRead(tc.debt), sqlStep{kind: "rollback"})
			_, err := scripted(t, steps).Admit(context.Background(), a)
			if !errors.Is(err, tc.want) || pacelock.OutcomeOf(err) != pacelock.NotCharged || errors.Is(err, pacelock.ErrBackend) {
				t.Fatalf("got %v", err)
			}
		})
	}
}
func TestAdmissionOperationNames(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		op := "allow_static"
		if dynamic {
			op = "allow"
		}
		t.Run(op, func(t *testing.T) {
			d := scripted(t, nil)
			var err error
			if dynamic {
				_, err = d.Admit(context.Background(), pacelock.Admission{Scope: testScope})
			} else {
				_, err = d.AdmitStatic(context.Background(), pacelock.StaticAdmission{Scope: testScope})
			}
			var detail *pacelock.Error
			if !errors.Is(err, pacelock.ErrInvalid) || !errors.As(err, &detail) || detail.Op != op || detail.Outcome != pacelock.NotCharged {
				t.Fatalf("got %v, want %s / NotCharged", err, op)
			}
		})
	}
}

func TestStaticCannotBypassPolicy(t *testing.T) {
	a := static()
	steps := append(lockedSteps(), policyRead(pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 1}), sqlStep{kind: "rollback"})
	if _, err := scripted(t, steps).AdmitStatic(context.Background(), a); !errors.Is(err, pacelock.ErrPolicyMismatch) {
		t.Fatal(err)
	}
	a.Rules[0].Policy.Revision = 1
	if _, err := scripted(t, nil).AdmitStatic(context.Background(), a); !errors.Is(err, pacelock.ErrInvalid) {
		t.Fatal(err)
	}
}
func TestManagedAdmissionCommitAccounting(t *testing.T) {
	for _, commitFails := range []bool{false, true} {
		t.Run(fmt.Sprint(commitFails), func(t *testing.T) {
			a := managed(1)
			p := pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 2}
			want := pacelock.QuotaState{TAT: testNow + 1000000, Interval: 1000000, Burst: 3, ExpiresAt: testNow + 1000000}
			steps := append(lockedSteps(), policyRead(p), debtRead(pacelock.QuotaState{}), debtWrite(t, want))
			var commitErr error
			if commitFails {
				commitErr = errors.New("lost commit response")
			}
			steps = append(steps, sqlStep{kind: "commit", err: commitErr})
			r, err := scripted(t, steps).Admit(context.Background(), a)
			if commitFails {
				if !errors.Is(err, commitErr) || pacelock.OutcomeOf(err) != pacelock.Unknown {
					t.Fatalf("got %v", err)
				}
			} else if err != nil || !r.Allowed {
				t.Fatalf("got %+v %v", r, err)
			}
		})
	}
}

func TestStaticAdmissionCommitUnknown(t *testing.T) {
	failure := errors.New("lost commit response")
	steps := append(lockedSteps(), policyRead(pacelock.QuotaState{}), debtRead(pacelock.QuotaState{}), sqlStep{kind: "exec", contains: "INSERT INTO pacelock_quotas"}, sqlStep{kind: "commit", err: failure})
	_, err := scripted(t, steps).AdmitStatic(context.Background(), static())
	if !errors.Is(err, failure) || pacelock.OutcomeOf(err) != pacelock.Unknown {
		t.Fatalf("got %v", err)
	}
}

func TestPruneOnlyRecoveredDebt(t *testing.T) {
	for _, commitFails := range []bool{false, true} {
		t.Run(fmt.Sprint(commitFails), func(t *testing.T) {
			var failure error
			if commitFails {
				failure = errors.New("lost commit response")
			}
			steps := append(lockedSteps(), sqlStep{kind: "exec", contains: "FROM pacelock_quotas WHERE scope_id=$1 AND expires_us<=$2 AND tat_us<=$2", check: func(args []driver.NamedValue) {
				if args[1].Value != testNow || args[2].Value != int64(100) {
					t.Fatalf("prune args: %+v", args)
				}
			}}, sqlStep{kind: "commit", err: failure})
			count, err := scripted(t, steps).Prune(context.Background(), testScope, 100)
			if !errors.Is(err, failure) || (commitFails && count != 0) || (!commitFails && count != 1) {
				t.Fatalf("got %d %v", count, err)
			}
		})
	}
}
func TestMigrateAddsSeparatePolicyTable(t *testing.T) {
	steps := []sqlStep{{kind: "begin"}}
	for _, s := range splitSchema() {
		steps = append(steps, sqlStep{kind: "exec", contains: s})
	}
	steps = append(steps, sqlStep{kind: "commit"})
	if err := scripted(t, steps).Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(schema, "ALTER TABLE") || strings.Contains(schema, "DROP TABLE") || strings.Contains(schema, "pacelock_admissions") {
		t.Fatal("legacy schema must not be changed; new installs must not create admission tables")
	}
	for _, s := range []string{"CREATE TABLE IF NOT EXISTS pacelock_policies", "interval_us >= 0", "revision >= 1 AND revision <= 9007199254740991"} {
		if !strings.Contains(schema, s) {
			t.Errorf("missing %q", s)
		}
	}
}

func TestAdmissionWriteFailureRollsBack(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		t.Run(fmt.Sprint(dynamic), func(t *testing.T) {
			failure := errors.New("second debt write failed")
			policy := pacelock.QuotaState{}
			if dynamic {
				policy = pacelock.QuotaState{Interval: 1000000, Burst: 3, Revision: 1}
			}
			steps := append(lockedSteps(), policyRead(policy), debtRead(pacelock.QuotaState{}), policyRead(policy), debtRead(pacelock.QuotaState{}), sqlStep{kind: "exec", contains: "INSERT INTO pacelock_quotas"}, sqlStep{kind: "exec", contains: "INSERT INTO pacelock_quotas", err: failure}, sqlStep{kind: "rollback"})
			d := scripted(t, steps)
			var err error
			if dynamic {
				a := managed(1)
				a.Charges = append(a.Charges, pacelock.Charge{Name: "other", Cost: 1})
				_, err = d.Admit(context.Background(), a)
			} else {
				a := static()
				other := a.Rules[0]
				other.Name = "other"
				a.Rules = append(a.Rules, other)
				_, err = d.AdmitStatic(context.Background(), a)
			}
			if !errors.Is(err, failure) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
				t.Fatalf("got %v", err)
			}
		})
	}
}
