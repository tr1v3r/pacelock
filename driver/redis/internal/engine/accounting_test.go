package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

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

func TestRedisLostResponseAccounting(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	wrapped := &lostResponse{Scripter: c}
	broken := &Limiter{client: wrapped, prefix: l.prefix}
	r := rule("q", 2, 1)
	if _, err := broken.Allow(ctx, r); !errors.Is(err, io.ErrUnexpectedEOF) || OutcomeOf(err) != Unknown {
		t.Fatal(err)
	}
	if wrapped.calls != 1 {
		t.Fatal("retried uncertain command")
	}
	if d, err := l.Allow(ctx, r); err != nil || !d.Allowed {
		t.Fatalf("double charge: %+v %v", d, err)
	}
	if d, err := l.Allow(ctx, r); err != nil || d.Allowed {
		t.Fatalf("lost debit: %+v %v", d, err)
	}
}

// An ACL denial of any MSET key rejects the entire commit, across all quota keys.
func TestRedisMSETACLFailureIsUncharged(t *testing.T) {
	l, admin := integration(t)
	ctx := context.Background()
	user := fmt.Sprintf("pacelock-test-%d", time.Now().UnixNano())
	password := "local-test-only"
	err := admin.Do(ctx, "ACL", "SETUSER", user, "on", ">"+password, "+eval", "+evalsha", "+get", "+mset", "+time", "+hello", "+ping", "+client", "%R~"+l.prefix+"*", "%W~"+l.key("first"), "(+eval +evalsha ~"+l.prefix+"*)").Err()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Do(ctx, "ACL", "DELUSER", user) })
	restricted := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, Username: user, Password: password, MaxRetries: -1})
	defer restricted.Close()
	limited := &Limiter{client: restricted, prefix: l.prefix}
	rules := []Rule{rule("first", 2, 1), rule("second", 2, 1)}
	if _, err := limited.Allow(ctx, rules...); err == nil || OutcomeOf(err) != NotCharged {
		t.Fatalf("want rejected commit: %v", err)
	}
	if admin.Exists(ctx, l.key("first"), l.key("second")).Val() != 0 {
		t.Fatal("rejected MSET partially committed")
	}
	if r, err := l.Allow(ctx, rules...); err != nil || !r.Allowed {
		t.Fatalf("retry after known rejection: %+v %v", r, err)
	}
}

func TestRedisMalformedStates(t *testing.T) {
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
}
