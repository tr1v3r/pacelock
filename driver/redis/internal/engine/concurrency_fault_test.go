package engine

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testContentionCancellation(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	publishRules(t, l, contentionRule("q", 200, 1))
	var allowed atomic.Int64
	contentionWave(t, 600, func(ctx context.Context, i int) {
		current := f.caller(l, i)
		r := contentionRule("q", 200, 1)
		var err error
		var want error
		switch i % 6 {
		case 0:
			var d Decision
			d, err = current.admitRules(ctx, r)
			if err == nil && d.Allowed {
				allowed.Add(1)
			}
		case 1:
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = current.admitRules(canceled, contentionRule("untouched", 1, 1))
			want = context.Canceled
		case 2:
			expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
			defer cancel()
			_, err = current.admitRules(expired, r)
			want = context.DeadlineExceeded
		case 3:
			_, err = current.admitRules(ctx, r, r)
			want = ErrInvalid
		case 4:
			var d Decision
			d, err = current.admitRules(ctx, r)
			if err == nil && d.Allowed {
				allowed.Add(1)
			}
		case 5:
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = current.admitRules(canceled, r)
			want = context.Canceled
		}
		if want == nil {
			if err != nil {
				t.Error(err)
			}
		} else if !errors.Is(err, want) || OutcomeOf(err) != NotCharged {
			t.Errorf("want %v/not_charged, got %v", want, err)
		}
	})
	if allowed.Load() != 200 {
		t.Fatalf("admitted=%d want 200", allowed.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n, err := f.clients[0].Exists(ctx, l.key("untouched")).Result(); err != nil || n != 0 {
		t.Fatalf("pre-dispatch errors wrote %d keys: %v", n, err)
	}
	requireDenied(t, l, contentionRule("q", 200, 1))
}

func testContentionBadState(t *testing.T, f *contentionFixture) {
	for _, kind := range []string{"quota", "wrong_type"} {
		t.Run(kind, func(t *testing.T) {
			l := f.limiter(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r := contentionRule("q", 10, 1)
			publishRules(t, l, r, contentionRule("side", 10, 1))
			requireAllowed(t, l, r)
			before := quotaState(t, f, l, "q")
			rules := []Rule{r, contentionRule("side", 10, 1)}
			var seedErr error
			want, outcome := ErrState, Unknown
			switch kind {
			case "quota":
				seedErr = f.clients[0].Set(ctx, l.key("side"), "broken", time.Minute).Err()
				outcome = NotCharged
			case "wrong_type":
				seedErr = f.clients[0].LPush(ctx, l.key("side"), "wrongtype").Err()
				outcome = NotCharged
			}
			if seedErr != nil {
				t.Fatal(seedErr)
			}
			contentionWave(t, 300, func(ctx context.Context, i int) {
				current := f.caller(l, i)
				_, err := current.admitRules(ctx, rules...)
				if !errors.Is(err, want) || OutcomeOf(err) != outcome {
					t.Errorf("want %v/%s, got %v", want, outcome, err)
				}
			})
			if quotaState(t, f, l, "q") != before {
				t.Fatal("failed concurrent requests debited healthy quota")
			}
		})
	}
}

// Inject loss only after the real server has executed successfully. The wrapper
// is safe to share and counts dispatched successful commands (including denials).
type concurrentResponseLoss struct {
	redis.Scripter
	calls atomic.Int64
	loss  error
}

func (s *concurrentResponseLoss) lose(cmd *redis.Cmd) *redis.Cmd {
	if cmd.Err() == nil {
		s.calls.Add(1)
		cmd.SetErr(s.loss)
	}
	return cmd
}
func (s *concurrentResponseLoss) Eval(ctx context.Context, src string, keys []string, args ...interface{}) *redis.Cmd {
	return s.lose(s.Scripter.Eval(ctx, src, keys, args...))
}
func (s *concurrentResponseLoss) EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) *redis.Cmd {
	return s.lose(s.Scripter.EvalSha(ctx, sha, keys, args...))
}

func testContentionLostResponses(t *testing.T, f *contentionFixture) {
	for _, loss := range []struct {
		name string
		err  error
	}{
		{"eof", io.ErrUnexpectedEOF}, {"canceled_after_execution", context.Canceled}, {"deadline_after_execution", context.DeadlineExceeded},
	} {
		t.Run(loss.name, func(t *testing.T) {
			l := f.limiter(t)
			r := contentionRule("q", 100, 1)
			publishRules(t, l, r)
			wrapped := &concurrentResponseLoss{Scripter: f.clients[0], loss: loss.err}
			broken := &Limiter{client: wrapped, prefix: l.prefix}
			contentionWave(t, 400, func(ctx context.Context, i int) {
				_, err := broken.admitRules(ctx, r)
				if !errors.Is(err, loss.err) || OutcomeOf(err) != Unknown {
					t.Errorf("want unknown lost response, got %v", err)
				}
			})
			if wrapped.calls.Load() != 400 {
				t.Fatalf("unexpected internal retries: %d executions", wrapped.calls.Load())
			}
			requireDenied(t, l, r)
		})
	}
}

func testContentionSmallPool(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	small := contentionClient(f.cluster, 1)
	defer small.Close()
	current := &Limiter{client: small, prefix: l.prefix}
	publishRules(t, l, contentionRule("q", 73, 1), contentionRule("after", 1, 1))
	var admitted atomic.Int64
	contentionWave(t, 500, func(ctx context.Context, i int) {
		d, err := current.admitRules(ctx, contentionRule("q", 73, 1))
		if err != nil {
			t.Error(err)
		} else if d.Allowed {
			admitted.Add(1)
		}
	})
	if admitted.Load() != 73 {
		t.Fatalf("small pool admitted %d", admitted.Load())
	}
	// The same client remains usable after contention drains.
	requireAllowed(t, current, contentionRule("after", 1, 1))
}

func testContentionColdScripts(t *testing.T, f *contentionFixture) {
	// Dedicated disposable fixtures only: SCRIPT FLUSH affects the script cache
	// of the whole server, not quota data. Never run against shared/production Redis.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if c, ok := f.clients[0].(*redis.ClusterClient); ok {
		err = c.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error { return c.ScriptFlush(ctx).Err() })
	} else {
		err = f.clients[0].ScriptFlush(ctx).Err()
	}
	if err != nil {
		t.Fatal(err)
	}
	l := f.limiter(t)
	publishRules(t, l, contentionRule("q", 1, 1))
	var originals atomic.Int64
	contentionWave(t, 300, func(ctx context.Context, i int) {
		current := f.caller(l, i)
		d, err := current.admitRules(ctx, contentionRule("q", 1, 1))
		if err != nil {
			t.Errorf("cold script %+v %v", d, err)
		} else if d.Allowed {
			originals.Add(1)
		}
	})
	if originals.Load() != 1 {
		t.Fatalf("cold script originals=%d", originals.Load())
	}
	requireDenied(t, l, contentionRule("q", 1, 1))
}

func testContentionRefill(t *testing.T, f *contentionFixture) {
	t.Run("natural_refill", func(t *testing.T) {
		l := f.limiter(t)
		// With a fast real clock, don't assert an exact burst count: Redis service
		// time can span replenishment. Bound accepted cost by server elapsed time.
		r := Rule{Name: "q", Policy: Policy{Rate: 1, Period: 10 * time.Millisecond, Burst: 20}, Cost: 1}
		publishRules(t, l, r)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for wave := 0; wave < 3; wave++ {
			before, err := f.clients[0].Time(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			var admitted atomic.Int64
			contentionWave(t, 200, func(ctx context.Context, i int) {
				d, err := f.caller(l, i).admitRules(ctx, r)
				if err != nil {
					t.Error(err)
				} else if d.Allowed {
					admitted.Add(1)
				}
			})
			after, err := f.clients[0].Time(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			bound := r.Policy.Burst + int64(after.Sub(before)/r.Policy.Period)
			if got := admitted.Load(); got < r.Policy.Burst || got > bound {
				t.Fatalf("wave %d admitted %d outside [%d,%d]", wave, got, r.Policy.Burst, bound)
			}
			time.Sleep(time.Duration(r.Policy.Burst)*r.Policy.Period + 10*time.Millisecond) // Logical recovery requires no physical deletion.
		}
	})
}

func testContentionPoolExhaustion(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	publishRules(t, l, contentionRule("q", 1, 1))
	small := contentionClient(f.cluster, 1)
	defer small.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Pin the only connection on every master, so no admission can reach Redis.
	var mu sync.Mutex
	var held []*redis.Conn
	hold := func(ctx context.Context, c *redis.Client) error {
		conn := c.Conn()
		if err := conn.Ping(ctx).Err(); err != nil {
			conn.Close()
			return err
		}
		mu.Lock()
		held = append(held, conn)
		mu.Unlock()
		return nil
	}
	release := func() {
		for _, c := range held {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}
		held = nil
	}
	defer release()
	var err error
	if c, ok := small.(*redis.ClusterClient); ok {
		// Populate go-redis routing metadata before deliberately exhausting its pools.
		if err := c.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error { return node.Command(ctx).Err() }); err != nil {
			t.Fatal(err)
		}
		if err := c.Exists(ctx, l.key("warm-metadata")).Err(); err != nil {
			t.Fatal(err)
		}
		err = c.ForEachMaster(ctx, hold)
	} else {
		err = hold(ctx, small.(*redis.Client))
	}
	if err != nil {
		t.Fatal(err)
	}
	current := &Limiter{client: small, prefix: l.prefix}
	contentionWave(t, 100, func(parent context.Context, i int) {
		deadline, cancel := context.WithTimeout(parent, 100*time.Millisecond)
		defer cancel()
		_, err := current.admitRules(deadline, contentionRule("q", 1, 1))
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrBackend) || OutcomeOf(err) != Unknown {
			t.Errorf("pool wait deadline: %v", err)
		}
	})
	// The SDK conservatively reports Unknown after dispatch; the fixture can
	// independently prove that these particular requests never got a connection.
	if n, err := f.clients[0].Exists(ctx, l.key("q")).Result(); err != nil || n != 0 {
		t.Fatalf("blocked pool wrote %d keys: %v", n, err)
	}
	release()
	if d, err := current.admitRules(ctx, contentionRule("q", 1, 1)); err != nil || !d.Allowed {
		t.Fatalf("pool did not recover: %+v %v", d, err)
	}
}
