package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tr1v3r/pacelock"
)

type fakeAdmission struct {
	calls    int
	decision pacelock.Decision
	found    bool
	err      error
}

func (f *fakeAdmission) AllowStaticOnce(_ context.Context, _ string, _ time.Duration, r ...pacelock.Rule) (pacelock.Receipt, error) {
	f.calls++
	if len(r) != 3 {
		panic("missing quota")
	}
	return pacelock.Receipt{Decision: f.decision}, f.err
}
func (f *fakeAdmission) Lookup(context.Context, string) (pacelock.Decision, bool, error) {
	return f.decision, f.found, f.err
}
func fixture(t *testing.T) (journal, *fakeAdmission) {
	t.Helper()
	j := journal{t.TempDir()}
	if err := j.save(attempt{ID: "id", Tenant: "a", Tokens: 100, Policy: 1, Created: time.Now(), State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	return j, &fakeAdmission{decision: pacelock.Decision{Allowed: true}, found: true}
}
func TestNormalAndAdmissionRecovery(t *testing.T) {
	for _, fault := range []string{"", "after-admission"} {
		t.Run(fault, func(t *testing.T) {
			j, f := fixture(t)
			executions := 0
			execute := func(string) error { executions++; return nil }
			err := advance(context.Background(), j, f, "run", fault, execute)
			if fault != "" {
				if err == nil || executions != 0 {
					t.Fatal("fault must stop before execution")
				}
				// Reload the journal to model a fresh process; recovery must not execute.
				if err = advance(context.Background(), journal{j.dir}, f, "recover", "", execute); err != nil {
					t.Fatal(err)
				}
				if executions != 0 {
					t.Fatal("lookup executed upstream")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err = advance(context.Background(), journal{j.dir}, f, "run", "", execute); err != nil {
					t.Fatal(err)
				}
			}
			if executions != 1 || f.calls != 1 {
				t.Fatalf("executions=%d admissions=%d", executions, f.calls)
			}
		})
	}
}
func TestUnknownExecutionNeverRetries(t *testing.T) {
	j, f := fixture(t)
	count := 0
	execute := func(string) error { count++; return nil }
	if err := advance(context.Background(), j, f, "run", "after-execution", execute); err == nil {
		t.Fatal("expected fault")
	}
	for _, action := range []string{"recover", "run"} {
		if err := advance(context.Background(), journal{j.dir}, f, action, "", execute); err == nil {
			t.Fatal("unknown execution accepted")
		}
	}
	if count != 1 || f.calls != 1 {
		t.Fatal("repeated side effect")
	}
}
func TestUnresolvedAdmissionPreserved(t *testing.T) {
	for _, kind := range []string{"missing", "corrupt", "expired"} {
		t.Run(kind, func(t *testing.T) {
			j, f := fixture(t)
			a, _ := j.load()
			a.State = "admitting"
			switch kind {
			case "missing":
				f.found = false
			case "corrupt":
				f.err = &pacelock.Error{Op: "lookup", Outcome: pacelock.Unknown, Err: pacelock.ErrState}
			case "expired":
				a.Created = time.Now().Add(-retention)
			}
			if err := j.save(a); err != nil {
				t.Fatal(err)
			}
			execute := func(string) error { t.Fatal("must not execute"); return nil }
			if err := advance(context.Background(), j, f, "recover", "", execute); err == nil {
				t.Fatal("unknown admitted")
			}
			after, err := j.load()
			if err != nil || after.State != "admitting" {
				t.Fatal("journal reset")
			}
			if f.calls != 0 {
				t.Fatal("reissued admission")
			}
		})
	}
}
func TestDenialAndNotChargedTerminal(t *testing.T) {
	for _, notCharged := range []bool{false, true} {
		j, f := fixture(t)
		f.decision = pacelock.Decision{Allowed: false, RetryAfter: time.Second, Limited: []string{"account:rpm"}}
		if notCharged {
			f.err = &pacelock.Error{Op: "allow_once", Outcome: pacelock.NotCharged, Err: errors.New("invalid")}
		}
		execute := func(string) error { t.Fatal("denied execution"); return nil }
		_ = advance(context.Background(), j, f, "run", "", execute)
		if err := advance(context.Background(), j, f, "run", "", execute); err != nil {
			t.Fatal(err)
		}
		if f.calls != 1 {
			t.Fatal("terminal attempt retried")
		}
	}
}
func TestTrustedRulesAndExclusiveJournal(t *testing.T) {
	a := attempt{Policy: 1, Tenant: "tenant-a", Tokens: 123}
	r, err := rules(a)
	if err != nil || len(r) != 3 || r[0].Name != "account:rpm" || r[1].Name != "model:input-tpm" || r[2].Name != "tenant:tenant-a:input-tpm" || r[0].Cost != 1 || r[1].Cost != 123 || r[2].Cost != 123 {
		t.Fatalf("incomplete rules: %+v %v", r, err)
	}
	a.Policy = 2
	if _, err = rules(a); err == nil {
		t.Fatal("unknown policy accepted")
	}
	dir := t.TempDir()
	_, unlock, err := openJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if _, close, err := openJournal(dir); err == nil {
		close()
		t.Fatal("concurrent owner accepted")
	}
}
