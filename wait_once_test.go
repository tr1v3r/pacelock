package pacelock

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWaitOnceRetriesAndObservesReceipt(t *testing.T) {
	for _, replayed := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "replayed"}[replayed], func(t *testing.T) {
			rules := []Rule{sampleRule(), {Name: "tokens", Rate: 10, Period: time.Second, Burst: 20, Cost: 7}}
			original := append([]Rule(nil), rules...)
			var last time.Time
			const delay = 5 * time.Millisecond
			d := &waitDriver{}
			d.admit = func(_ context.Context, a Admission) (Receipt, error) {
				if !a.SuccessOnly || a.ID != "persisted-id" || a.Retention != time.Minute || a.Scope != (Scope{"a", "b"}) || !reflect.DeepEqual(a.Rules, original) {
					t.Fatalf("changed request: %+v", a)
				}
				if !last.IsZero() && time.Since(last) < delay {
					t.Fatal("retried before RetryAfter")
				}
				last = time.Now()
				rules[0].Cost = 5
				if d.calls < 3 {
					return Receipt{Decision: Decision{RetryAfter: delay, Limited: []string{"tokens"}}}, nil
				}
				return Receipt{Decision: Decision{Allowed: true}, Replayed: replayed}, nil
			}
			var events []Event
			l, _ := NewWithOptions(d, "a", "b", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
			r, err := l.WaitOnce(context.Background(), "persisted-id", time.Minute, rules...)
			if err != nil || !r.Allowed || r.Replayed != replayed || d.calls != 3 {
				t.Fatal(r, err, d.calls)
			}
			if len(events) != 1 || events[0].Operation != "wait_once" || events[0].Result != "allowed" || events[0].Replayed != replayed || events[0].RuleCount != 2 || events[0].Duration < 2*delay {
				t.Fatal(events)
			}
		})
	}
}

func TestWaitOnceValidation(t *testing.T) {
	d := &waitDriver{}
	l, _ := New(d, "a", "b")
	var nilLimiter *Limiter
	for name, call := range map[string]func() error{
		"nil receiver": func() error {
			_, e := nilLimiter.WaitOnce(context.Background(), "id", time.Minute, sampleRule())
			return e
		},
		"zero receiver": func() error {
			_, e := (&Limiter{}).WaitOnce(context.Background(), "id", time.Minute, sampleRule())
			return e
		},
		"nil context": func() error { _, e := l.WaitOnce(nil, "id", time.Minute, sampleRule()); return e },
		"empty id":    func() error { _, e := l.WaitOnce(context.Background(), "", time.Minute, sampleRule()); return e },
		"long id": func() error {
			_, e := l.WaitOnce(context.Background(), strings.Repeat("x", 257), time.Minute, sampleRule())
			return e
		},
		"zero retention": func() error { _, e := l.WaitOnce(context.Background(), "id", 0, sampleRule()); return e },
		"fractional retention": func() error {
			_, e := l.WaitOnce(context.Background(), "id", time.Millisecond+1, sampleRule())
			return e
		},
		"long retention": func() error {
			_, e := l.WaitOnce(context.Background(), "id", MaxHorizon+time.Millisecond, sampleRule())
			return e
		},
		"no rules": func() error { _, e := l.WaitOnce(context.Background(), "id", time.Minute); return e },
		"invalid utf8": func() error {
			r := sampleRule()
			r.Name = string([]byte{255})
			_, e := l.WaitOnce(context.Background(), "id", time.Minute, r)
			return e
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			var typed *Error
			if !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged || !errors.As(err, &typed) || typed.Op != "wait_once" {
				t.Fatal(err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.WaitOnce(ctx, "id", time.Minute, sampleRule()); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if d.calls != 0 {
		t.Fatal("invalid request reached driver")
	}
}

func TestWaitOnceStopsOnErrorsAndRetainedDenial(t *testing.T) {
	for _, tc := range []struct {
		result  Receipt
		err     error
		want    error
		outcome Outcome
	}{
		{err: io.ErrUnexpectedEOF, want: io.ErrUnexpectedEOF, outcome: Unknown},
		{err: &Error{Outcome: NotCharged, Err: ErrBackend}, want: ErrBackend, outcome: NotCharged},
		{err: &Error{Outcome: NotCharged, Err: ErrPolicyMismatch}, want: ErrPolicyMismatch, outcome: NotCharged},
		{err: &Error{Outcome: Unknown, Err: context.DeadlineExceeded}, want: context.DeadlineExceeded, outcome: Unknown},
		{err: &Error{Outcome: Unknown, Err: ErrOutcomeUnknown}, want: ErrOutcomeUnknown, outcome: Unknown},
		{result: Receipt{}, want: ErrState, outcome: Unknown},
		{result: Receipt{Decision: Decision{RetryAfter: time.Second, Limited: []string{"other"}}}, want: ErrState, outcome: Unknown},
		{result: Receipt{Decision: Decision{RetryAfter: time.Second, Limited: []string{"q"}}, Replayed: true}, want: ErrConflict, outcome: NotCharged},
	} {
		t.Run(tc.want.Error(), func(t *testing.T) {
			d := &waitDriver{}
			d.admit = func(context.Context, Admission) (Receipt, error) {
				if d.calls == 1 {
					return Receipt{Decision: Decision{RetryAfter: time.Microsecond, Limited: []string{"q"}}}, nil
				}
				return tc.result, tc.err
			}
			l, _ := New(d, "a", "b")
			_, err := l.WaitOnce(context.Background(), "id", time.Minute, sampleRule())
			var typed *Error
			if !errors.Is(err, tc.want) || OutcomeOf(err) != tc.outcome || !errors.As(err, &typed) || typed.Op != "wait_once" || d.calls != 2 {
				t.Fatal(err, d.calls)
			}
		})
	}
}

func TestWaitOnceCancellationAccounting(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		d := &waitDriver{}
		d.admit = func(context.Context, Admission) (Receipt, error) {
			cancel()
			if allowed {
				return Receipt{Decision: Decision{Allowed: true}}, nil
			}
			return Receipt{Decision: Decision{RetryAfter: time.Hour, Limited: []string{"q"}}}, nil
		}
		l, _ := New(d, "a", "b")
		r, err := l.WaitOnce(ctx, "id", time.Minute, sampleRule())
		cancel()
		if allowed && (err != nil || !r.Allowed) {
			t.Fatal(r, err)
		}
		if !allowed && (!errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged) {
			t.Fatal(err)
		}
		if d.calls != 1 {
			t.Fatal(d.calls)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	d := &waitDriver{}
	d.admit = func(context.Context, Admission) (Receipt, error) {
		return Receipt{Decision: Decision{RetryAfter: time.Hour, Limited: []string{"q"}}}, nil
	}
	l, _ := New(d, "a", "b")
	if _, err := l.WaitOnce(ctx, "id", time.Minute, sampleRule()); !errors.Is(err, context.DeadlineExceeded) || OutcomeOf(err) != NotCharged || d.calls > 1 {
		t.Fatal(err, d.calls)
	}
}
