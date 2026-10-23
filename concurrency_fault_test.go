package pacelock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testContentionCancellation(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	var mu sync.Mutex
	events := make(map[string]int)
	l.observer = func(_ context.Context, e Event) {
		mu.Lock()
		defer mu.Unlock()
		events[e.Operation+"/"+e.Result]++
		if e.Duration < 0 {
			t.Error("negative observation duration")
		}
	}
	var allowed atomic.Int64
	contentionWave(t, 600, func(ctx context.Context, i int) {
		current := f.caller(l, i)
		r := contentionRule("q", 200, 1)
		var err error
		var want error
		switch i % 6 {
		case 0:
			var d Decision
			d, err = current.Allow(ctx, r)
			if err == nil && d.Allowed {
				allowed.Add(1)
			}
		case 1:
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = current.Allow(canceled, contentionRule("untouched", 1, 1))
			want = context.Canceled
		case 2:
			expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
			defer cancel()
			_, err = current.AllowOnce(expired, "never", time.Minute, r)
			want = context.DeadlineExceeded
		case 3:
			_, err = current.Allow(ctx, r, r)
			want = ErrInvalid
		case 4:
			var d Receipt
			d, err = current.AllowOnce(ctx, fmt.Sprintf("valid%d", i), time.Minute, r)
			if err == nil && d.Allowed {
				allowed.Add(1)
			}
		case 5:
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, _, err = current.Lookup(canceled, "never")
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
	want := map[string]int{"allow/allowed": 100, "allow/error": 200, "allow_once/allowed": 100, "allow_once/error": 100, "lookup/error": 100}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("observer events %v want %v", events, want)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n, err := f.clients[0].Exists(ctx, l.key("untouched"), l.attemptKey("never")).Result(); err != nil || n != 0 {
		t.Fatalf("pre-dispatch errors wrote %d keys: %v", n, err)
	}
	l.observer = nil // All concurrent calls have joined.
	requireDenied(t, l, contentionRule("q", 200, 1))
}

func testContentionLookup(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	r := contentionRule("q", 100, 1)
	// A racing read may observe absence or a complete result, never torn JSON.
	contentionWave(t, 300, func(ctx context.Context, i int) {
		id := fmt.Sprintf("id%d", i%100)
		current := f.caller(l, i)
		if i >= 200 {
			d, found, err := current.Lookup(ctx, id)
			if err != nil || (found && !d.Allowed) {
				t.Errorf("racing lookup %+v %t %v", d, found, err)
			}
		} else {
			d, err := current.AllowOnce(ctx, id, time.Minute, r)
			if err != nil || !d.Allowed {
				t.Errorf("writer %+v %v", d, err)
			}
		}
	})
	before := quotaState(t, f, l, "q")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 100; i++ {
		if err := f.clients[0].PExpire(ctx, l.attemptKey(fmt.Sprintf("id%d", i)), 30*time.Second).Err(); err != nil {
			t.Fatal(err)
		}
	}
	contentionWave(t, 500, func(ctx context.Context, i int) {
		id := fmt.Sprintf("id%d", i%100)
		current := f.caller(l, i)
		if i%2 == 0 {
			d, err := current.AllowOnce(ctx, id, time.Minute, r)
			if err != nil || !d.Replayed || !d.Allowed {
				t.Errorf("replay %+v %v", d, err)
			}
		} else {
			d, found, err := current.Lookup(ctx, id)
			if err != nil || !found || !d.Allowed {
				t.Errorf("lookup %+v %t %v", d, found, err)
			}
		}
	})
	if quotaState(t, f, l, "q") != before {
		t.Fatal("replay/lookup changed quota")
	}
	for i := 0; i < 100; i++ {
		ttl, err := f.clients[0].PTTL(ctx, l.attemptKey(fmt.Sprintf("id%d", i))).Result()
		if err != nil || ttl <= 0 || ttl > 30*time.Second {
			t.Fatalf("record TTL changed: %s %v", ttl, err)
		}
	}
}

func testContentionBadState(t *testing.T, f *contentionFixture) {
	for _, kind := range []string{"pending", "record", "quota", "wrong_type"} {
		t.Run(kind, func(t *testing.T) {
			l := f.limiter(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r := contentionRule("q", 10, 1)
			requireAllowed(t, l, r)
			before := quotaState(t, f, l, "q")
			rules := []Rule{r, contentionRule("side", 10, 1)}
			var seedErr error
			want, outcome := ErrState, Unknown
			switch kind {
			case "pending":
				_, args, err := l.prepare(rules)
				if err != nil {
					t.Fatal(err)
				}
				args[0] = time.Minute.Milliseconds()
				seedErr = f.clients[0].Set(ctx, l.attemptKey("id"), "P1|"+fingerprint(args), time.Minute).Err()
				want = ErrOutcomeUnknown
			case "record":
				seedErr = f.clients[0].Set(ctx, l.attemptKey("id"), "broken", time.Minute).Err()
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
				var err error
				if (kind == "pending" || kind == "record") && i%2 == 0 {
					_, _, err = current.Lookup(ctx, "id")
				} else {
					_, err = current.AllowOnce(ctx, "id", time.Minute, rules...)
				}
				if !errors.Is(err, want) || OutcomeOf(err) != outcome {
					t.Errorf("want %v/%s, got %v", want, outcome, err)
				}
			})
			if quotaState(t, f, l, "q") != before {
				t.Fatal("failed concurrent requests debited healthy quota")
			}
			if kind == "pending" || kind == "record" {
				if n, err := f.clients[0].Exists(ctx, l.key("side")).Result(); err != nil || n != 0 {
					t.Fatalf("wrote side quota: %d %v", n, err)
				}
			} else if n, err := f.clients[0].Exists(ctx, l.attemptKey("id")).Result(); err != nil || n != 0 {
				t.Fatalf("pre-write error created record: %d %v", n, err)
			}
		})
	}
}

// Inject loss only after the real server has executed successfully. The wrapper
// is safe to share and counts dispatched successful commands (including replays).
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
			wrapped := &concurrentResponseLoss{Scripter: f.clients[0], loss: loss.err}
			broken := &Limiter{client: wrapped, prefix: l.prefix}
			contentionWave(t, 400, func(ctx context.Context, i int) {
				_, err := broken.AllowOnce(ctx, fmt.Sprintf("id%d", i%100), time.Minute, r)
				if !errors.Is(err, loss.err) || OutcomeOf(err) != Unknown {
					t.Errorf("want unknown lost response, got %v", err)
				}
			})
			if wrapped.calls.Load() != 400 {
				t.Fatalf("unexpected internal retries: %d executions", wrapped.calls.Load())
			}
			contentionWave(t, 400, func(ctx context.Context, i int) {
				current := f.caller(l, i)
				id := fmt.Sprintf("id%d", i%100)
				if i%2 == 0 {
					d, err := current.AllowOnce(ctx, id, time.Minute, r)
					if err != nil || !d.Replayed || !d.Allowed {
						t.Errorf("recovery %+v %v", d, err)
					}
				} else {
					d, found, err := current.Lookup(ctx, id)
					if err != nil || !found || !d.Allowed {
						t.Errorf("lookup %+v %t %v", d, found, err)
					}
				}
			})
			requireDenied(t, l, r)
		})
	}
}

func testContentionSmallPool(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	small := contentionClient(f.cluster, 1)
	defer small.Close()
	current := &Limiter{client: small, prefix: l.prefix}
	var admitted atomic.Int64
	contentionWave(t, 500, func(ctx context.Context, i int) {
		d, err := current.Allow(ctx, contentionRule("q", 73, 1))
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
	var originals atomic.Int64
	contentionWave(t, 300, func(ctx context.Context, i int) {
		current := f.caller(l, i)
		if i%3 == 0 {
			_, _, err := current.Lookup(ctx, "same")
			if err != nil {
				t.Error(err)
			}
			return
		}
		d, err := current.AllowOnce(ctx, "same", time.Minute, contentionRule("q", 1, 1))
		if err != nil || !d.Allowed {
			t.Errorf("cold script %+v %v", d, err)
		} else if !d.Replayed {
			originals.Add(1)
		}
	})
	if originals.Load() != 1 {
		t.Fatalf("cold script originals=%d", originals.Load())
	}
	requireDenied(t, l, contentionRule("q", 1, 1))
}

func waitForKeyExpiry(t *testing.T, f *contentionFixture, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		n, err := f.clients[0].Exists(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("key did not expire")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func testContentionRefill(t *testing.T, f *contentionFixture) {
	t.Run("natural_refill", func(t *testing.T) {
		l := f.limiter(t)
		// With a fast real clock, don't assert an exact burst count: Redis service
		// time can span replenishment. Bound accepted cost by server elapsed time.
		r := Rule{Name: "q", Rate: 1, Period: 10 * time.Millisecond, Burst: 20, Cost: 1}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for wave := 0; wave < 3; wave++ {
			before, err := f.clients[0].Time(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			var admitted atomic.Int64
			contentionWave(t, 200, func(ctx context.Context, i int) {
				d, err := f.caller(l, i).Allow(ctx, r)
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
			bound := r.Burst + int64(after.Sub(before)/r.Period)
			if got := admitted.Load(); got < r.Burst || got > bound {
				t.Fatalf("wave %d admitted %d outside [%d,%d]", wave, got, r.Burst, bound)
			}
			waitForKeyExpiry(t, f, l.key("q"))
		}
	})
	t.Run("record_expiry", func(t *testing.T) {
		l := f.limiter(t)
		r := contentionRule("q", 2, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		d, err := l.AllowOnce(ctx, "same", time.Minute, r)
		if err != nil || !d.Allowed {
			t.Fatalf("%+v %v", d, err)
		}
		if err := f.clients[0].PExpire(ctx, l.attemptKey("same"), time.Millisecond).Err(); err != nil {
			t.Fatal(err)
		}
		waitForKeyExpiry(t, f, l.attemptKey("same"))
		var originals atomic.Int64
		contentionWave(t, 500, func(ctx context.Context, i int) {
			d, err := f.caller(l, i).AllowOnce(ctx, "same", time.Minute, r)
			if err != nil || !d.Allowed {
				t.Errorf("%+v %v", d, err)
			} else if !d.Replayed {
				originals.Add(1)
			}
		})
		if originals.Load() != 1 {
			t.Fatalf("new retention window originals=%d", originals.Load())
		}
		requireDenied(t, l, r)
	})
}

func testContentionPoolExhaustion(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
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
		_, err := current.AllowOnce(deadline, "blocked", time.Minute, contentionRule("q", 1, 1))
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrBackend) || OutcomeOf(err) != Unknown {
			t.Errorf("pool wait deadline: %v", err)
		}
	})
	// The SDK conservatively reports Unknown after dispatch; the fixture can
	// independently prove that these particular requests never got a connection.
	if n, err := f.clients[0].Exists(ctx, l.key("q"), l.attemptKey("blocked")).Result(); err != nil || n != 0 {
		t.Fatalf("blocked pool wrote %d keys: %v", n, err)
	}
	release()
	if d, err := current.AllowOnce(ctx, "blocked", time.Minute, contentionRule("q", 1, 1)); err != nil || !d.Allowed || d.Replayed {
		t.Fatalf("pool did not recover: %+v %v", d, err)
	}
}
