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
	keys, args, err := l.prepare(rules)
	if err != nil {
		t.Fatal(err)
	}
	args[0] = int64(60000)
	args[1] = fingerprint(args)
	keys = append(keys, l.attemptKey("fault"))
	for _, fault := range []string{"before", "encoding", "after"} {
		t.Run(fault, func(t *testing.T) {
			if err := c.Del(ctx, keys...).Err(); err != nil {
				t.Fatal(err)
			}
			injected := source
			switch fault {
			case "before":
				injected = strings.Replace(source, "local committed = redis.pcall", "error('injected before commit')\n    local committed = redis.pcall", 1)
			case "encoding":
				injected = strings.Replace(source, "local complete = 'C1|'", "local complete = error('injected encoding failure') .. 'C1|'", 1)
			case "after":
				injected = strings.Replace(source, "return response", "error('injected after commit')", 1)
			}
			if _, err := redis.NewScript(injected).Run(ctx, c, keys, args...).Result(); err == nil {
				t.Fatal("fault was not reached")
			}
			count := c.Exists(ctx, keys...).Val()
			if fault != "after" {
				if count != 0 {
					t.Fatalf("pre-commit fault left %d keys", count)
				}
			} else {
				if count != int64(len(keys)) {
					t.Fatalf("commit left %d keys", count)
				}
				if d, found, err := l.Lookup(ctx, "fault"); err != nil || !found || !d.Allowed {
					t.Fatalf("recover: %+v %t %v", d, found, err)
				}
				if r, err := l.AllowOnce(ctx, "fault", time.Minute, rules...); err != nil || !r.Replayed {
					t.Fatalf("replay: %+v %v", r, err)
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
	if err := c.ConfigSet(ctx, "maxmemory", "1").Err(); err != nil {
		t.Fatal(err)
	}
	_, err = l.AllowOnce(ctx, "oom", time.Minute, rule("a", 2, 1), rule("b", 2, 1))
	if err == nil || !errors.Is(err, ErrBackend) || !strings.Contains(strings.ToLower(err.Error()), "oom") {
		t.Fatalf("expected real OOM: %v", err)
	}
	if n := c.Exists(ctx, l.key("a"), l.key("b"), l.attemptKey("oom")).Val(); n != 0 {
		t.Fatalf("OOM partially committed %d keys", n)
	}
}

func TestRedisLegacyUpgradePreservesDebtAndRecords(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	now, err := c.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	const step = int64(time.Hour / time.Microsecond)
	r := rule("legacy", 3, 2)
	if err := c.Set(ctx, l.key(r.Name), fmt.Sprintf("%d:%d:3", now.UnixMicro()+2*step, step), 3*time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if d, err := l.Allow(ctx, r); err != nil || d.Allowed {
		t.Fatalf("legacy debt reset: %+v %v", d, err)
	}
	r.Cost = 1
	_, args, _ := l.prepare([]Rule{r})
	args[0] = int64(60000)
	fp := fingerprint(args)
	complete := "C1|" + fp + `|{"names":["legacy"],"values":[1,0,0]}`
	for _, entry := range []struct{ id, raw string }{{"complete", complete}, {"pending", "P1|" + fp}} {
		if err := c.Set(ctx, l.attemptKey(entry.id), entry.raw, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if receipt, err := l.AllowOnce(ctx, "complete", time.Minute, r); err != nil || !receipt.Replayed {
		t.Fatalf("legacy replay: %+v %v", receipt, err)
	}
	if _, err := l.AllowOnce(ctx, "pending", time.Minute, r); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("legacy pending: %v", err)
	}
	if _, found, err := l.Lookup(ctx, "pending"); found || !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("legacy pending lookup: %t %v", found, err)
	}
	requireAllowed(t, l, r)
	if raw := c.Get(ctx, l.key(r.Name)).Val(); !strings.HasPrefix(raw, "Q2|") {
		t.Fatalf("not upgraded: %q", raw)
	}
	if ttl := c.PTTL(ctx, l.key(r.Name)).Val(); ttl != -1 {
		t.Fatalf("MSET retained native expiry: %s", ttl)
	}
	requireDenied(t, l, r)
}

func TestRedisLogicalReceiptExpiryAndMalformedEnvelopes(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	rules := []Rule{rule("q", 5, 1)}
	original, err := l.AllowOnce(ctx, "id", time.Minute, rules...)
	if err != nil || !original.Allowed {
		t.Fatal(original, err)
	}
	raw := c.Get(ctx, l.attemptKey("id")).Val()
	if !strings.HasPrefix(raw, "E2|") || c.PTTL(ctx, l.attemptKey("id")).Val() != -1 {
		t.Fatalf("expected persistent envelope: %q", raw)
	}
	if r, err := l.AllowOnce(ctx, "id", time.Minute, rules...); err != nil || !r.Replayed {
		t.Fatal(r, err)
	}
	if c.Get(ctx, l.attemptKey("id")).Val() != raw {
		t.Fatal("replay extended logical retention")
	}
	parts := strings.SplitN(raw, "|", 3)
	raw = "E2|1|" + parts[2]
	if err := c.Set(ctx, l.attemptKey("id"), raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := l.Lookup(ctx, "id"); err != nil || found {
		t.Fatalf("expired lookup: %t %v", found, err)
	}
	if c.Get(ctx, l.attemptKey("id")).Val() != raw {
		t.Fatal("lookup mutated expired receipt")
	}
	for _, bad := range []string{"E2|0|C1|bad", "E2|9007199254740992|C1|bad", "E2|x|C1|bad", "E2|1|P1|" + strings.Repeat("a", 64), "E2|1|C1|bad"} {
		if err := c.Set(ctx, l.attemptKey("bad"), bad, 0).Err(); err != nil {
			t.Fatal(err)
		}
		if _, found, err := l.Lookup(ctx, "bad"); !errors.Is(err, ErrState) || found {
			t.Fatalf("malformed %q: %t %v", bad, found, err)
		}
		if _, err := l.AllowOnce(ctx, "bad", time.Minute, rules...); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
			t.Fatalf("malformed replay: %v", err)
		}
	}
}
