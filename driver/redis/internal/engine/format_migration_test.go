package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestRedisFormatMigration(t *testing.T) {
	for _, debtFormat := range []string{"D3|%d", "%d:3600000000:3"} {
		t.Run(debtFormat, func(t *testing.T) {
			l, c := integration(t)
			ctx := context.Background()
			now, err := c.Time(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			const step = int64(time.Hour / time.Microsecond)
			tat := now.UnixMicro() + 2*step
			debt := fmt.Sprintf(debtFormat, tat)
			const policy = "P3|3600000000:3:7"
			r := rule("migration", 3, 2)
			r.Policy.Revision = 7
			if err := c.MSet(ctx, l.key(r.Name), debt, l.policyKey(r.Name), policy).Err(); err != nil {
				t.Fatal(err)
			}
			for _, change := range []string{"stale", "conflict", "same"} {
				q := quotaOf(r)
				switch change {
				case "stale":
					q.Policy.Revision--
				case "conflict":
					q.Policy.Burst++
				case "same":
					// Equivalent normalized policy must remain a no-op, including TTL.
					q.Policy.Rate = 2
					q.Policy.Period = 2 * time.Hour
				}
				if err := c.PExpire(ctx, l.policyKey(r.Name), time.Hour).Err(); err != nil {
					t.Fatal(err)
				}
				err := l.UpdateQuotas(ctx, q)
				if change == "same" {
					if err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, ErrPolicyMismatch) || OutcomeOf(err) != NotCharged {
					t.Fatalf("%s fence: %v", change, err)
				}
				if got := c.Get(ctx, l.policyKey(r.Name)).Val(); got != policy || c.PTTL(ctx, l.policyKey(r.Name)).Val() <= 0 {
					t.Fatalf("%s rewrote old policy: %q", change, got)
				}
				if got := c.Get(ctx, l.key(r.Name)).Val(); got != debt {
					t.Fatalf("%s changed debt: %q", change, got)
				}
			}
			if err := c.PExpire(ctx, l.key(r.Name), 3*time.Hour).Err(); err != nil {
				t.Fatal(err)
			}
			if d, err := l.Admit(ctx, managedRequest(r.Name, 2)); err != nil || d.Allowed {
				t.Fatalf("old debt reset: %+v %v", d, err)
			}
			if got := c.Get(ctx, l.key(r.Name)).Val(); got != debt || c.PTTL(ctx, l.key(r.Name)).Val() <= 0 {
				t.Fatalf("denial mutated old debt: %q", got)
			}
			if d, err := l.Admit(ctx, managedRequest(r.Name, 1)); err != nil || !d.Allowed {
				t.Fatalf("migration admission: %+v %v", d, err)
			}
			wantDebt := fmt.Sprintf("D1|%d", tat+step)
			if got := c.Get(ctx, l.key(r.Name)).Val(); got != wantDebt || c.PTTL(ctx, l.key(r.Name)).Val() != -1 {
				t.Fatalf("new debt: %q want %q without native expiry", got, wantDebt)
			}
			if got := c.Get(ctx, l.policyKey(r.Name)).Val(); got != policy {
				t.Fatalf("admission rewrote old policy: %q", got)
			}
			r.Policy.Revision++
			if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
				t.Fatal(err)
			}
			if got := c.Get(ctx, l.policyKey(r.Name)).Val(); got != "P1|3600000000:3:8" || c.PTTL(ctx, l.policyKey(r.Name)).Val() != -1 {
				t.Fatalf("higher revision: %q", got)
			}
			if got := c.Get(ctx, l.key(r.Name)).Val(); got != wantDebt {
				t.Fatalf("higher revision changed debt: %q", got)
			}
		})
	}
}

func TestRedisPruneMixedFormats(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	now, err := c.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	preserved := map[string]string{}
	for i, format := range []string{"D1|%d", "D3|%d", "%d:1:1"} {
		for _, live := range []bool{false, true} {
			name := fmt.Sprintf("format-%d-live-%t", i, live)
			tat := int64(0)
			if live {
				tat = now.Add(time.Hour).UnixMicro()
			}
			key, raw := l.key(name), fmt.Sprintf(format, tat)
			keys = append(keys, key)
			if err := c.Set(ctx, key, raw, 0).Err(); err != nil {
				t.Fatal(err)
			}
			if live {
				preserved[key] = raw
			}
		}
	}
	for _, marker := range []string{"P1", "P3"} {
		for _, step := range []int{0, 1} {
			key := l.policyKey(fmt.Sprintf("%s-%d", marker, step))
			raw := fmt.Sprintf("%s|%d:1:7", marker, step)
			keys = append(keys, key)
			preserved[key] = raw
			if err := c.Set(ctx, key, raw, 0).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	cursor := PruneCursor{scope: l.prefix, position: "0", pending: keys}
	page, err := l.Prune(ctx, cursor, len(keys))
	if err != nil || page.Deleted != 3 {
		t.Fatalf("mixed prune: %+v %v", page, err)
	}
	for _, key := range keys {
		if want, ok := preserved[key]; ok {
			if got, err := c.Get(ctx, key).Result(); err != nil || got != want {
				t.Fatalf("preserved %s: %q want %q: %v", key, got, want, err)
			}
		} else if c.Exists(ctx, key).Val() != 0 {
			t.Fatalf("expired debt retained: %s", key)
		}
	}
}

func TestRedisRejectUnknownFormatVersions(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("strict", 1, 1)
	r.Policy.Revision = 1
	for _, version := range []string{"0", "2", "4", "9", "11", "13", "33", "01", "03", ""} {
		for _, role := range []string{"D", "P"} {
			t.Run(role+version, func(t *testing.T) {
				if err := c.MSet(ctx, l.key(r.Name), "D1|0", l.policyKey(r.Name), "P1|3600000000:1:1").Err(); err != nil {
					t.Fatal(err)
				}
				key, raw := l.key(r.Name), "D"+version+"|0"
				if role == "P" {
					key, raw = l.policyKey(r.Name), "P"+version+"|3600000000:1:1"
				}
				if err := c.Set(ctx, key, raw, 0).Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := l.Admit(ctx, managedRequest(r.Name, 1)); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
					t.Fatalf("admit accepted %q: %v", raw, err)
				}
				if role == "P" {
					if err := l.UpdateQuotas(ctx, quotaOf(r)); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
						t.Fatalf("update accepted %q: %v", raw, err)
					}
				}
				cursor := PruneCursor{scope: l.prefix, position: "0", pending: []string{key}}
				if _, err := l.Prune(ctx, cursor, 1); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
					t.Fatalf("prune accepted %q: %v", raw, err)
				}
				if got := c.Get(ctx, key).Val(); got != raw {
					t.Fatalf("invalid value mutated: %q", got)
				}
			})
		}
	}
}
