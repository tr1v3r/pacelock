package contracts_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/tr1v3r/pacelock"
)

// dynamicWaitDriver keeps the real backend's atomic admission and authoritative
// clock. Hooks coordinate changes at the driver boundary, not with guessed sleeps.
// Wait always uses Admit; the hook does not replace backend policy resolution.
type dynamicWaitDriver struct {
	pacelock.Driver
	admissions []pacelock.Admission
	before     func(int)
	after      func(int, pacelock.Decision, error) (pacelock.Decision, error)
}

func (d *dynamicWaitDriver) Admit(ctx context.Context, a pacelock.Admission) (pacelock.Decision, error) {
	snapshot := a
	snapshot.Charges = append([]pacelock.Charge(nil), a.Charges...)
	d.admissions = append(d.admissions, snapshot)
	if d.before != nil {
		d.before(len(d.admissions))
	}
	r, err := d.Driver.Admit(ctx, a)
	if d.after != nil {
		return d.after(len(d.admissions), r, err)
	}
	return r, err
}

func newDynamicWaitLimiter(t *testing.T, l *contractLimiter, d *dynamicWaitDriver) (*pacelock.Limiter, *[]pacelock.Event) {
	t.Helper()
	var events []pacelock.Event
	waiter, err := pacelock.NewWithOptions(d, l.scope.Namespace, l.scope.Domain, pacelock.Options{
		Observer: func(_ context.Context, event pacelock.Event) { events = append(events, event) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return waiter, &events
}

func checkDynamicWaitCall(t *testing.T, d *dynamicWaitDriver, events []pacelock.Event, charges []pacelock.Charge, err error) {
	t.Helper()
	for i, a := range d.admissions {
		if !reflect.DeepEqual(a.Charges, charges) {
			t.Fatalf("attempt %d changed snapshot: %+v", i+1, a)
		}
	}
	if len(events) != 1 {
		t.Fatalf("observer events=%+v, want one whole-call event", events)
	}
	event := events[0]
	result := "allowed"
	if err != nil {
		result = "error"
		var detail *pacelock.Error
		if !errors.As(err, &detail) || detail.Op != "wait" {
			t.Fatalf("Wait error operation: %v", err)
		}
	}
	if event.Operation != "wait" || event.RuleCount != len(charges) || event.Result != result || event.Err != err {
		t.Fatalf("whole-call observer event: %+v, err=%v", event, err)
	}
}

func allowDynamic(t *testing.T, l *contractLimiter, charges ...pacelock.Charge) {
	t.Helper()
	if decision, err := l.Allow(context.Background(), charges...); err != nil || !decision.Allowed {
		t.Fatalf("dynamic admission: %+v %v", decision, err)
	}
}

func TestDynamicWaitDriverContract(t *testing.T) {
	// Include Cluster as well as standalone Redis and PostgreSQL using the same
	// real fixtures as the published-policy blocking contracts.
	for _, b := range allBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			t.Run("weighted recovery consumes once", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				blocked := dynamicQuota(1, 1, 2)
				blocked.Policy.Period = 500 * time.Millisecond
				fresh := dynamicQuota(1, 1, 5)
				fresh.Name = "fresh"
				if err := l.UpdateQuotas(context.Background(), blocked, fresh); err != nil {
					t.Fatal(err)
				}
				allowDynamic(t, l, quotaCharge(blocked, 2))
				d := &dynamicWaitDriver{Driver: b.driver[1]}
				waiter, events := newDynamicWaitLimiter(t, l, d)
				charges := []pacelock.Charge{quotaCharge(fresh, 3), quotaCharge(blocked, 2)}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				err := waiter.Wait(ctx, charges...)
				if err != nil {
					t.Fatal(err)
				}
				if len(d.admissions) < 2 {
					t.Fatal("expected a real capacity denial before recovery")
				}
				checkDynamicWaitCall(t, d, *events, charges, err)
				allowDynamic(t, l, quotaCharge(fresh, 2))
				if decision, err := l.Allow(ctx, quotaCharge(fresh, 1)); err != nil || decision.Allowed {
					t.Fatalf("Wait did not debit exactly three fresh units: %+v %v", decision, err)
				}
			})

			t.Run("deadline leaves every dimension uncharged", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				blocked := dynamicQuota(1, 1, 1)
				fresh := dynamicQuota(1, 1, 5)
				fresh.Name = "fresh"
				if err := l.UpdateQuotas(context.Background(), blocked, fresh); err != nil {
					t.Fatal(err)
				}
				allowDynamic(t, l, quotaCharge(blocked, 1))
				d := &dynamicWaitDriver{Driver: b.driver[1]}
				waiter, events := newDynamicWaitLimiter(t, l, d)
				charges := []pacelock.Charge{quotaCharge(fresh, 3), quotaCharge(blocked, 1)}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				err := waiter.Wait(ctx, charges...)
				if !errors.Is(err, context.DeadlineExceeded) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
				if len(d.admissions) != 1 {
					t.Fatalf("deadline dispatched %d attempts, want one denied attempt", len(d.admissions))
				}
				checkDynamicWaitCall(t, d, *events, charges, err)
				allowDynamic(t, l, quotaCharge(fresh, 5))
				// Expose exactly one more unit without resetting old debt. A hidden
				// debit on the blocked dimension would make this admission deny.
				blocked.Policy.Revision++
				blocked.Policy.Burst++
				updateQuota(t, l, blocked)
				allowDynamic(t, l, quotaCharge(blocked, 1))
			})

			t.Run("new revision and immutable charge snapshot", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				blocked := dynamicQuota(1, 1, 2)
				blocked.Policy.Period = 500 * time.Millisecond
				fresh := dynamicQuota(1, 1, 5)
				fresh.Name = "fresh"
				if err := l.UpdateQuotas(context.Background(), blocked, fresh); err != nil {
					t.Fatal(err)
				}
				allowDynamic(t, l, quotaCharge(blocked, 2))
				charges := []pacelock.Charge{quotaCharge(fresh, 3), quotaCharge(blocked, 2)}
				original := append([]pacelock.Charge(nil), charges...)
				var deniedAt time.Time
				var advisory time.Duration
				d := &dynamicWaitDriver{Driver: b.driver[1]}
				d.before = func(attempt int) {
					if attempt == 2 && time.Since(deniedAt) < advisory {
						t.Fatal("policy update bypassed the previous RetryAfter")
					}
				}
				d.after = func(attempt int, r pacelock.Decision, err error) (pacelock.Decision, error) {
					if attempt == 1 {
						if err != nil || r.Allowed {
							t.Fatalf("expected first real denial: %+v %v", r, err)
						}
						deniedAt, advisory = time.Now(), r.RetryAfter
						// The driver has already seen the first snapshot, so mutating
						// caller-owned values is ordered rather than a data race.
						charges[0] = pacelock.Charge{Name: "unpublished", Cost: 99}
						charges[1].Cost = 99
						blocked.Policy.Revision++
						blocked.Policy.Period = time.Minute
						blocked.Policy.Burst = 4
						fresh.Policy.Revision++
						fresh.Policy.Burst = 7
						if updateErr := l.UpdateQuotas(context.Background(), blocked, fresh); updateErr != nil {
							t.Fatal(updateErr)
						}
					}
					return r, err
				}
				waiter, events := newDynamicWaitLimiter(t, l, d)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				err := waiter.Wait(ctx, charges...)
				if err != nil || len(d.admissions) != 2 {
					t.Fatalf("updated policy retry: attempts=%d err=%v", len(d.admissions), err)
				}
				checkDynamicWaitCall(t, d, *events, original, err)
				allowDynamic(t, l, quotaCharge(fresh, 4))
				if decision, err := l.Allow(ctx, quotaCharge(fresh, 1)); err != nil || decision.Allowed {
					t.Fatalf("updated burst or snapshotted cost ignored: %+v %v", decision, err)
				}
			})

			for _, terminal := range []string{"paused", "cost exceeds new burst"} {
				t.Run(terminal+" after denial", func(t *testing.T) {
					l := limiter(t, b, 0, freshDomain())
					blocked := dynamicQuota(1, 1, 2)
					blocked.Policy.Period = 500 * time.Millisecond
					fresh := dynamicQuota(1, 1, 5)
					fresh.Name = "fresh"
					if err := l.UpdateQuotas(context.Background(), blocked, fresh); err != nil {
						t.Fatal(err)
					}
					allowDynamic(t, l, quotaCharge(blocked, 2))
					want := pacelock.ErrPaused
					d := &dynamicWaitDriver{Driver: b.driver[1]}
					d.after = func(attempt int, r pacelock.Decision, err error) (pacelock.Decision, error) {
						if attempt == 1 {
							if err != nil || r.Allowed {
								t.Fatalf("expected first real denial: %+v %v", r, err)
							}
							blocked.Policy.Revision++
							if terminal == "paused" {
								blocked.Policy.Rate = 0
							} else {
								blocked.Policy.Burst = 1
								want = pacelock.ErrInvalid
							}
							updateQuota(t, l, blocked)
						}
						return r, err
					}
					waiter, events := newDynamicWaitLimiter(t, l, d)
					charges := []pacelock.Charge{quotaCharge(fresh, 3), quotaCharge(blocked, 2)}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					err := waiter.Wait(ctx, charges...)
					if !errors.Is(err, want) || pacelock.OutcomeOf(err) != pacelock.NotCharged || len(d.admissions) != 2 {
						t.Fatalf("terminal policy error: attempts=%d err=%v", len(d.admissions), err)
					}
					checkDynamicWaitCall(t, d, *events, charges, err)
					allowDynamic(t, l, quotaCharge(fresh, 5))
				})
			}

			t.Run("missing policy fails closed without partial debit", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				fresh := dynamicQuota(1, 1, 5)
				updateQuota(t, l, fresh)
				d := &dynamicWaitDriver{Driver: b.driver[1]}
				waiter, events := newDynamicWaitLimiter(t, l, d)
				charges := []pacelock.Charge{quotaCharge(fresh, 3), {Name: "missing", Cost: 1}}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				err := waiter.Wait(ctx, charges...)
				if !errors.Is(err, pacelock.ErrPolicyMissing) || pacelock.OutcomeOf(err) != pacelock.NotCharged || len(d.admissions) != 1 {
					t.Fatalf("missing policy: attempts=%d err=%v", len(d.admissions), err)
				}
				checkDynamicWaitCall(t, d, *events, charges, err)
				allowDynamic(t, l, quotaCharge(fresh, 5))
			})
		})
	}
}

func TestDynamicWaitCancellationAccounting(t *testing.T) {
	for _, b := range allBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			for _, mode := range []string{"confirmed debit", "confirmed denial", "lost debit reply", "uncanceled lost debit reply", "uncharged transport error"} {
				t.Run(mode, func(t *testing.T) {
					l := limiter(t, b, 0, freshDomain())
					q := dynamicQuota(1, 1, 2)
					updateQuota(t, l, q)
					if mode == "confirmed denial" {
						allowDynamic(t, l, quotaCharge(q, 2))
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					lost := errors.New("injected transport reply loss")
					d := &dynamicWaitDriver{Driver: b.driver[1]}
					d.before = func(attempt int) {
						if attempt != 1 {
							t.Fatal("terminal outcome must not dispatch another admission")
						}
					}
					d.after = func(_ int, r pacelock.Decision, err error) (pacelock.Decision, error) {
						if err != nil || r.Allowed == (mode == "confirmed denial") {
							t.Fatalf("unexpected real backend outcome: %+v %v", r, err)
						}
						if mode != "uncanceled lost debit reply" {
							cancel()
						}
						if mode == "lost debit reply" || mode == "uncanceled lost debit reply" {
							return pacelock.Decision{}, lost
						}
						return r, err
					}
					if mode == "uncharged transport error" {
						// Abort before dispatch, preserving a proven no-debit error.
						d.Driver = dynamicWaitUnchargedDriver{Driver: b.driver[1], err: lost, cancel: cancel}
						d.after = nil
					}
					waiter, events := newDynamicWaitLimiter(t, l, d)
					charges := []pacelock.Charge{quotaCharge(q, 1)}
					err := waiter.Wait(ctx, charges...)
					switch mode {
					case "confirmed debit":
						if err != nil {
							t.Fatalf("confirmed debit replaced by cancellation: %v", err)
						}
					case "confirmed denial":
						if !errors.Is(err, context.Canceled) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
							t.Fatal(err)
						}
					case "lost debit reply", "uncanceled lost debit reply":
						if !errors.Is(err, lost) || !errors.Is(err, pacelock.ErrBackend) || pacelock.OutcomeOf(err) != pacelock.Unknown {
							t.Fatalf("ambiguous debit error lost: %v", err)
						}
					case "uncharged transport error":
						if !errors.Is(err, lost) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
							t.Fatalf("proven no-debit error replaced by cancellation: %v", err)
						}
					}
					if len(d.admissions) != 1 {
						t.Fatalf("terminal outcome retried: %d attempts", len(d.admissions))
					}
					checkDynamicWaitCall(t, d, *events, charges, err)
					if mode != "confirmed denial" {
						remainder := int64(1)
						if mode == "uncharged transport error" {
							remainder = 2
						}
						allowDynamic(t, l, quotaCharge(q, remainder))
						if decision, err := l.Allow(context.Background(), quotaCharge(q, 1)); err != nil || decision.Allowed {
							t.Fatalf("unexpected debit count: %+v %v", decision, err)
						}
					}
				})
			}
		})
	}
}

type dynamicWaitUnchargedDriver struct {
	pacelock.Driver
	err    error
	cancel context.CancelFunc
}

func (d dynamicWaitUnchargedDriver) Admit(context.Context, pacelock.Admission) (pacelock.Decision, error) {
	d.cancel()
	return pacelock.Decision{}, &pacelock.Error{Op: "admit", Outcome: pacelock.NotCharged, Err: d.err}
}
