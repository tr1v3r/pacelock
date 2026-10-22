package pacelock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisIdempotentAdmission(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	rules := []Rule{rule("rpm", 3, 1), rule("tokens", 9, 3)}
	first, err := l.AllowOnce(ctx, "request-1", time.Minute, rules...)
	if err != nil || !first.Allowed || first.Replayed {
		t.Fatalf("%+v %v", first, err)
	}
	state := c.Get(ctx, l.key("tokens")).Val()
	// Make TTL conspicuously shorter; replays and lookup must not refresh it.
	if err := c.PExpire(ctx, l.attemptKey("request-1"), 10*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	replay, err := l.AllowOnce(ctx, "request-1", time.Minute, rules...)
	if err != nil || !replay.Replayed || !reflect.DeepEqual(first.Decision, replay.Decision) {
		t.Fatalf("%+v %v", replay, err)
	}
	got, found, err := l.Lookup(ctx, "request-1")
	if err != nil || !found || !reflect.DeepEqual(got, first.Decision) {
		t.Fatalf("%+v %v %v", got, found, err)
	}
	if c.Get(ctx, l.key("tokens")).Val() != state || c.PTTL(ctx, l.attemptKey("request-1")).Val() > 10*time.Second {
		t.Fatal("replay charged or extended TTL")
	}
	if _, found, err := l.Lookup(ctx, "missing"); err != nil || found {
		t.Fatalf("%v %v", found, err)
	}
	for _, changed := range [][]Rule{{rule("rpm", 3, 2), rules[1]}, {rules[1], rules[0]}, {rules[0]}} {
		if _, err := l.AllowOnce(ctx, "request-1", time.Minute, changed...); !errors.Is(err, ErrConflict) || OutcomeOf(err) != NotCharged {
			t.Fatalf("conflict: %v", err)
		}
	}
	if _, err := l.AllowOnce(ctx, "request-1", 2*time.Minute, rules...); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if c.Get(ctx, l.key("tokens")).Val() != state {
		t.Fatal("conflict charged")
	}
	// Same effective policy is a valid replay, even with a different expression.
	rules[0].Rate = 2
	rules[0].Period = 2 * time.Hour
	if d, err := l.AllowOnce(ctx, "request-1", time.Minute, rules...); err != nil || !d.Replayed {
		t.Fatalf("%+v %v", d, err)
	}
}

func TestRedisIdempotentConcurrent(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	c2 := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer c2.Close()
	l2 := &Limiter{client: c2, prefix: l.prefix}
	var originals atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			current := l
			if i%2 == 0 {
				current = l2
			}
			d, err := current.AllowOnce(ctx, "same", time.Minute, rule("tokens", 10, 2))
			if err != nil || !d.Allowed {
				t.Errorf("%+v %v", d, err)
				return
			}
			if !d.Replayed {
				originals.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if originals.Load() != 1 {
		t.Fatalf("%d original admissions", originals.Load())
	}
	if d, err := l.Allow(ctx, rule("tokens", 10, 8)); err != nil || !d.Allowed {
		t.Fatalf("replay double charged: %+v %v", d, err)
	}
	if d, err := l.Allow(ctx, rule("tokens", 10, 1)); err != nil || d.Allowed {
		t.Fatalf("unexpected capacity: %+v %v", d, err)
	}
}

func TestRedisDeniedReplayAndExpiry(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("quota", 1, 1)
	if _, err := l.Allow(ctx, r); err != nil {
		t.Fatal(err)
	}
	first, err := l.AllowOnce(ctx, "denied", time.Minute, r)
	if err != nil || first.Allowed || first.Replayed {
		t.Fatalf("%+v %v", first, err)
	}
	// Simulate replenished quota; the retained rejected decision must stay rejected.
	if err := c.Del(ctx, l.key(r.Name)).Err(); err != nil {
		t.Fatal(err)
	}
	d, err := l.AllowOnce(ctx, "denied", time.Minute, r)
	if err != nil || !d.Replayed || !reflect.DeepEqual(d.Decision, first.Decision) {
		t.Fatalf("%+v %v", d, err)
	}
	if c.Exists(ctx, l.key(r.Name)).Val() != 0 {
		t.Fatal("denial replay wrote quota")
	}
	if d, err = l.AllowOnce(ctx, "new-attempt", 20*time.Millisecond, r); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for c.Exists(ctx, l.attemptKey("new-attempt")).Val() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("record did not expire")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, found, err := l.Lookup(ctx, "new-attempt"); err != nil || found {
		t.Fatalf("%v %v", found, err)
	}
	// Expired identity no longer deduplicates: the call evaluates current quota.
	if d, err = l.AllowOnce(ctx, "new-attempt", 20*time.Millisecond, r); err != nil || d.Allowed || d.Replayed {
		t.Fatalf("%+v %v", d, err)
	}
}

type lostResponse struct {
	redis.Scripter
	calls int
}

func (s *lostResponse) lose(c *redis.Cmd) *redis.Cmd {
	if c.Err() == nil {
		s.calls++
		c.SetErr(io.ErrUnexpectedEOF)
	}
	return c
}
func (s *lostResponse) EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) *redis.Cmd {
	return s.lose(s.Scripter.EvalSha(ctx, sha, keys, args...))
}
func (s *lostResponse) Eval(ctx context.Context, src string, keys []string, args ...interface{}) *redis.Cmd {
	return s.lose(s.Scripter.Eval(ctx, src, keys, args...))
}

func TestRedisLostResponseRecovery(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	wrapped := &lostResponse{Scripter: c}
	broken := &Limiter{client: wrapped, prefix: l.prefix}
	r := rule("q", 2, 1)
	if _, err := broken.AllowOnce(ctx, "lost", time.Minute, r); !errors.Is(err, io.ErrUnexpectedEOF) || OutcomeOf(err) != Unknown {
		t.Fatal(err)
	}
	if wrapped.calls != 1 {
		t.Fatal("retried uncertain command")
	}
	if d, found, err := l.Lookup(ctx, "lost"); err != nil || !found || !d.Allowed {
		t.Fatalf("%+v %v %v", d, found, err)
	}
	if d, err := l.AllowOnce(ctx, "lost", time.Minute, r); err != nil || !d.Replayed || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	if d, err := l.Allow(ctx, r); err != nil || !d.Allowed {
		t.Fatalf("double charge: %+v %v", d, err)
	}
	if d, err := l.Allow(ctx, r); err != nil || d.Allowed {
		t.Fatalf("lost debit: %+v %v", d, err)
	}
}

func TestRedisPendingAfterWriteFailure(t *testing.T) {
	l, admin := integration(t)
	ctx := context.Background()
	user := fmt.Sprintf("pacelock-test-%d", time.Now().UnixNano())
	password := "local-test-only"
	// The user can write the admission record and first quota, but cannot write
	// the second quota. This causes a real Redis runtime error after a partial debit.
	err := admin.Do(ctx, "ACL", "SETUSER", user, "on", ">"+password, "+eval", "+evalsha", "+get", "+set", "+time", "+hello", "+ping", "+client", "%R~"+l.prefix+"*", "%W~"+l.attemptKey("partial"), "%W~"+l.key("first"), "(+eval +evalsha ~"+l.prefix+"*)").Err()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Do(ctx, "ACL", "DELUSER", user) })
	restricted := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, Username: user, Password: password, MaxRetries: -1})
	defer restricted.Close()
	r := []Rule{rule("first", 2, 1), rule("second", 2, 1)}
	limited := &Limiter{client: restricted, prefix: l.prefix}
	if _, err := limited.AllowOnce(ctx, "partial", time.Minute, r...); err == nil || OutcomeOf(err) != Unknown {
		t.Fatalf("expected runtime failure: %v", err)
	}
	if admin.Exists(ctx, l.key("first")).Val() != 1 || admin.Exists(ctx, l.key("second")).Val() != 0 {
		t.Fatalf("test did not reach partial write; record=%q first=%q second=%q", admin.Get(ctx, l.attemptKey("partial")).Val(), admin.Get(ctx, l.key("first")).Val(), admin.Get(ctx, l.key("second")).Val())
	}
	before := admin.Get(ctx, l.key("first")).Val()
	if _, err := l.AllowOnce(ctx, "partial", time.Minute, r...); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	if _, found, err := l.Lookup(ctx, "partial"); !errors.Is(err, ErrOutcomeUnknown) || found {
		t.Fatalf("%v %v", found, err)
	}
	if admin.Get(ctx, l.key("first")).Val() != before {
		t.Fatal("pending record permitted repeat debit")
	}
}

func TestRedisMalformedStatesAndRecords(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("bad", 1, 1)
	for _, state := range []string{"broken", "1:0:1", "1:1:0", "9007199254740992:1:1", "1:86400000001:1"} {
		if err := c.Set(ctx, l.key("bad"), state, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := l.Allow(ctx, rule("fresh", 1, 1), r); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
			t.Fatalf("%q: %v", state, err)
		}
		if c.Exists(ctx, l.key("fresh")).Val() != 0 {
			t.Fatal("invalid state partially charged")
		}
	}
	c.Del(ctx, l.key("bad"))
	c.LPush(ctx, l.key("bad"), "wrongtype")
	if _, err := l.Allow(ctx, r); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	c.Del(ctx, l.key("bad"))
	for _, record := range []string{"", "garbage", "C1|" + strings.Repeat("a", 64) + "|{}"} {
		if err := c.Set(ctx, l.attemptKey("bad"), record, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if _, found, err := l.Lookup(ctx, "bad"); !errors.Is(err, ErrState) || found {
			t.Fatalf("record %q: %v %v", record, found, err)
		}
	}
}
