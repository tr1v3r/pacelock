package contracts_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/pacelock"
)

// cancelOnDenialDriver cancels only after a real backend confirms no debit.
// This keeps cancellation between attempts even on slow race-test hosts.
type cancelOnDenialDriver struct {
	pacelock.Driver
	cancel context.CancelFunc
}

func (d cancelOnDenialDriver) Admit(ctx context.Context, a pacelock.Admission) (pacelock.Receipt, error) {
	r, err := d.Driver.Admit(ctx, a)
	if err == nil && !r.Allowed {
		d.cancel()
	}
	return r, err
}

// TestWaitDriverContract exercises the shared wrapper through real backends.
func TestWaitDriverContract(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			t.Run("weighted recovery charges all quotas once", func(t *testing.T) {
				l := limiter(t, b, 0, freshDomain())
				slow := rule("slow", 2, 2)
				slow.Period = 50 * time.Millisecond
				mustAllow(t, l, slow)
				fresh := rule("fresh", 5, 3)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := l.Wait(ctx, fresh, slow); err != nil {
					t.Fatal(err)
				}
				fresh.Cost = 2
				mustAllow(t, l, fresh)
				fresh.Cost = 1
				decision, err := l.Allow(ctx, fresh)
				if err != nil || decision.Allowed {
					t.Fatal("wait must charge fresh quota exactly once", decision, err)
				}
			})
			t.Run("cancellation leaves every quota uncharged", func(t *testing.T) {
				domain := freshDomain()
				first := limiter(t, b, 0, domain)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				second, err := pacelock.New(cancelOnDenialDriver{b.driver[1], cancel}, "contract-"+domain, domain)
				if err != nil {
					t.Fatal(err)
				}
				blocked := rule("blocked", 1, 1)
				mustAllow(t, first, blocked)
				fresh := rule("fresh", 5, 3)
				if err := second.Wait(ctx, fresh, blocked); !errors.Is(err, context.Canceled) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
					t.Fatal(err)
				}
				fresh.Cost = fresh.Burst
				mustAllow(t, first, fresh)
			})
			t.Run("concurrent cross client waits", func(t *testing.T) {
				domain := freshDomain()
				ls := []*pacelock.Limiter{limiter(t, b, 0, domain), limiter(t, b, 1, domain)}
				r := rule("weighted", 2, 2)
				r.Period = 10 * time.Millisecond
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				const callers = 12
				errs := make(chan error, callers)
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range callers {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-start
						errs <- ls[i%len(ls)].Wait(ctx, r)
					}(i)
				}
				close(start)
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		})
	}
}
