package pacelock

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDecisionValidationBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name       string
		decision   Decision
		structural bool
	}{
		{"empty", Decision{}, false},
		{"allow with delay", Decision{Allowed: true, RetryAfter: time.Second}, false},
		{"allow with limited", Decision{Allowed: true, Limited: []string{"q"}}, false},
		{"negative delay", Decision{RetryAfter: -1, Limited: []string{"q"}}, false},
		{"excessive delay", Decision{RetryAfter: MaxHorizon + 1, Limited: []string{"q"}}, false},
		{"missing delay", Decision{Limited: []string{"q"}}, false},
		{"missing limited", Decision{RetryAfter: time.Second}, false},
		{"empty name", Decision{RetryAfter: time.Second, Limited: []string{""}}, false},
		{"long name", Decision{RetryAfter: time.Second, Limited: []string{strings.Repeat("q", 257)}}, false},
		{"duplicate name", Decision{RetryAfter: time.Second, Limited: []string{"q", "q"}}, false},
		{"too many names", Decision{RetryAfter: time.Second, Limited: make([]string, MaxRules+1)}, false},
		{"unknown name", Decision{RetryAfter: time.Second, Limited: []string{"other"}}, true},
		{"reversed names", Decision{RetryAfter: time.Second, Limited: []string{"tokens", "q"}}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.decision.Validate(); (err == nil) != tt.structural || (err != nil && !errors.Is(err, ErrState)) {
				t.Fatalf("structural validation: %v", err)
			}
			for _, op := range []string{"allow", "wait"} {
				t.Run(op, func(t *testing.T) {
					d := &dynamicWaitDriver{admit: func(context.Context, Admission) (Decision, error) { return tt.decision, nil }}
					var events []Event
					l, err := NewWithOptions(d, "app", "domain", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
					if err != nil {
						t.Fatal(err)
					}
					charges := []Charge{{Name: "q", Cost: 1}, {Name: "tokens", Cost: 1}}
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if op == "allow" {
						var decision Decision
						decision, err = l.Allow(ctx, charges...)
						if !reflect.DeepEqual(decision, Decision{}) {
							t.Fatal(decision)
						}
					} else {
						err = l.Wait(ctx, charges...)
					}
					var typed *Error
					if !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown || !errors.As(err, &typed) || typed.Op != op || d.calls != 1 {
						t.Fatalf("calls=%d err=%v", d.calls, err)
					}
					if len(events) != 1 || events[0].Operation != op || events[0].Result != "error" || events[0].Err != err {
						t.Fatal(events)
					}
				})
			}
		})
	}
	for _, decision := range []Decision{{Allowed: true}, {RetryAfter: MaxHorizon, Limited: []string{"tokens"}}} {
		if err := decision.Validate(); err != nil {
			t.Fatal(err)
		}
		if err := validateDecision(decision, []Charge{{Name: "q"}, {Name: "tokens"}}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAdmissionValidatesDispatchedSnapshot(t *testing.T) {
	for _, op := range []string{"allow", "wait"} {
		for _, limited := range []string{"q", "changed"} {
			t.Run(op+"/"+limited, func(t *testing.T) {
				charges := []Charge{{Name: "q", Cost: 1}, {Name: "tokens", Cost: 7}}
				original := append([]Charge(nil), charges...)
				d := &dynamicWaitDriver{}
				d.admit = func(_ context.Context, a Admission) (Decision, error) {
					if !reflect.DeepEqual(a.Charges, original) {
						t.Fatalf("changed snapshot: %+v", a.Charges)
					}
					// Synchronous mutation of the original slice, not the driver's snapshot.
					charges[0] = Charge{Name: "changed", Cost: 99}
					if d.calls > 1 {
						return Decision{Allowed: true}, nil
					}
					return Decision{RetryAfter: time.Microsecond, Limited: []string{limited}}, nil
				}
				var events []Event
				l, err := NewWithOptions(d, "app", "domain", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if op == "allow" {
					_, err = l.Allow(ctx, charges...)
				} else {
					err = l.Wait(ctx, charges...)
				}
				wantCalls := 1
				if limited == "q" {
					if err != nil {
						t.Fatal(err)
					}
					if op == "wait" {
						wantCalls = 2
					}
				} else if !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
					t.Fatal(err)
				}
				if d.calls != wantCalls || len(events) != 1 || events[0].Operation != op || events[0].Err != err {
					t.Fatal(d.calls, events)
				}
			})
		}
	}
}
