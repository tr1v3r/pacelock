package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisMSETCommitFaultBoundaries(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	rules := []Rule{rule("a", 3, 1), rule("b", 3, 1)}
	publishRules(t, l, rules...)
	keys, args, err := l.prepare(rules)
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"before", "encoding", "after"} {
		t.Run(fault, func(t *testing.T) {
			if err := c.Del(ctx, keys[:len(rules)]...).Err(); err != nil {
				t.Fatal(err)
			}
			injected := source
			switch fault {
			case "before":
				injected = strings.Replace(source, "local committed = redis.pcall", "error('injected before commit')\n    local committed = redis.pcall", 1)
			case "encoding":
				injected = strings.Replace(source, "string.format('D3|%.0f'", "error('injected encoding failure'", 1)
			case "after":
				injected = strings.Replace(source, "return result", "error('injected after commit')", 1)
			}
			if _, err := redis.NewScript(injected).Run(ctx, c, keys, args...).Result(); err == nil {
				t.Fatal("fault was not reached")
			}
			count := c.Exists(ctx, keys[:len(rules)]...).Val()
			if fault != "after" {
				if count != 0 {
					t.Fatalf("pre-commit fault left %d keys", count)
				}
			} else {
				if count != int64(len(rules)) {
					t.Fatalf("commit left %d keys", count)
				}
				requireAllowed(t, l, rule("a", 3, 2), rule("b", 3, 2))
			}
		})
	}
}

func TestRedisMSETOutOfMemoryNoPartialWrite(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	settings, err := c.ConfigGet(ctx, "maxmemory").Result()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.ConfigSet(ctx, "maxmemory", settings["maxmemory"]).Err(); err != nil {
			t.Error(err)
		}
	}()
	publishRules(t, l, rule("a", 2, 1), rule("b", 2, 1))
	if err := c.ConfigSet(ctx, "maxmemory", "1").Err(); err != nil {
		t.Fatal(err)
	}
	_, err = l.admitRules(ctx, rule("a", 2, 1), rule("b", 2, 1))
	if err == nil || !errors.Is(err, ErrBackend) || !strings.Contains(strings.ToLower(err.Error()), "oom") {
		t.Fatalf("expected real OOM: %v", err)
	}
	if n := c.Exists(ctx, l.key("a"), l.key("b")).Val(); n != 0 {
		t.Fatalf("OOM partially committed %d keys", n)
	}
}

func TestRedisPlainQuotaPreservesDebtAndRemovesNativeTTL(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	now, err := c.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	const step = int64(time.Hour / time.Microsecond)
	r := rule("plain", 3, 2)
	publishRules(t, l, r)
	tat := now.UnixMicro() + 2*step
	before := fmt.Sprintf("%d:%d:3", tat, step)
	if err := c.Set(ctx, l.key(r.Name), before, 3*time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if d, err := l.admitRules(ctx, r); err != nil || d.Allowed {
		t.Fatalf("plain quota debt reset: %+v %v", d, err)
	}
	if raw := c.Get(ctx, l.key(r.Name)).Val(); raw != before {
		t.Fatalf("denial mutated quota: %q", raw)
	}
	r.Cost = 1
	requireAllowed(t, l, r)
	want := fmt.Sprintf("D3|%d", tat+step)
	if raw := c.Get(ctx, l.key(r.Name)).Val(); raw != want {
		t.Fatalf("quota=%q want exact plain format %q", raw, want)
	}
	if ttl := c.PTTL(ctx, l.key(r.Name)).Val(); ttl != -1 {
		t.Fatalf("MSET retained native expiry: %s", ttl)
	}
	requireDenied(t, l, r)
}
