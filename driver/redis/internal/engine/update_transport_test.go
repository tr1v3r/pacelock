package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisManagedLostReplyNoRetry(t *testing.T) {
	l, admin := integration(t)
	ctx := context.Background()
	r := rule("q", 2, 1)
	r.Policy.Revision = 1
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	loss := &replyLoss{}
	c := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, MaxRetries: 3, PoolSize: 1, Dialer: loss.dial})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := admissionScript.Load(ctx, c).Err(); err != nil {
		t.Fatal(err)
	}
	c.AddHook(&armReplyLoss{loss: loss, command: "evalsha"})
	broken := &Limiter{client: safeScripter(c), prefix: l.prefix}
	req := managedRequest("q", 1)
	if _, err := broken.Admit(ctx, req); !errors.Is(err, io.EOF) || OutcomeOf(err) != Unknown {
		t.Fatalf("lost reply: %v", err)
	}
	for _, allowed := range []bool{true, false} {
		d, err := l.Admit(ctx, req)
		if err != nil || d.Allowed != allowed {
			t.Fatalf("replayed debit: %+v %v", d, err)
		}
	}
}

func TestRedisUpdateLostReplyNoRetry(t *testing.T) {
	addr := os.Getenv("PACELOCK_REDIS_ADDR")
	if addr == "" {
		t.Skip("set PACELOCK_REDIS_ADDR")
	}
	for _, command := range []string{"evalsha", "eval"} {
		t.Run(command, func(t *testing.T) {
			ctx := context.Background()
			loss := &replyLoss{}
			c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: 3, PoolSize: 1, Dialer: loss.dial})
			defer c.Close()
			c.AddHook(&armReplyLoss{loss: loss, command: command})
			if err := c.Ping(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			if command == "evalsha" {
				if err := updateScript.Load(ctx, c).Err(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := c.ScriptFlush(ctx).Err(); err != nil {
					t.Fatal(err)
				}
			}
			l, err := New(c, "update-lost", fmt.Sprint(time.Now().UnixNano()))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Del(ctx, l.policyKey("q"))
			r := rule("q", 1, 1)
			r.Policy.Revision = 1
			err = l.UpdateQuotas(ctx, quotaOf(r))
			if !errors.Is(err, io.EOF) || OutcomeOf(err) != Unknown || loss.dropped.Load() != 1 {
				t.Fatalf("lost reply: %v drops=%d", err, loss.dropped.Load())
			}
			if got := c.Get(ctx, l.policyKey("q")).Val(); got != "P3|3600000000:1:1" {
				t.Fatalf("commit absent: %s", got)
			}
			if err = l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
				t.Fatalf("explicit retry: %v", err)
			}
		})
	}
}
