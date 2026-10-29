package redisdriver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tr1v3r/pacelock"
)

func TestRedisD1P1DebtAndRevisionSafety(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	now, err := c.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	const step = int64(time.Hour / time.Microsecond)
	tat := now.UnixMicro() + 2*step
	debt := fmt.Sprintf("D1|%d", tat)
	const policy = "P1|3600000000:3:7"
	r := rule("strict", 3, 2)
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
			q.Policy.Rate, q.Policy.Period = 2, 2*time.Hour
		}
		err := l.UpdateQuotas(ctx, q)
		if change == "same" {
			if err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, ErrPolicyMismatch) || OutcomeOf(err) != NotCharged {
			t.Fatalf("%s fence: %v", change, err)
		}
		if got := c.Get(ctx, l.policyKey(r.Name)).Val(); got != policy || c.PTTL(ctx, l.policyKey(r.Name)).Val() != -1 {
			t.Fatalf("%s changed permanent policy: %q", change, got)
		}
		if got := c.Get(ctx, l.key(r.Name)).Val(); got != debt {
			t.Fatalf("%s changed debt: %q", change, got)
		}
	}
	if d, err := l.Admit(ctx, managedRequest(r.Name, 2)); err != nil || d.Allowed {
		t.Fatalf("active debt reset: %+v %v", d, err)
	}
	if got := c.Get(ctx, l.key(r.Name)).Val(); got != debt {
		t.Fatalf("denial changed debt: %q", got)
	}
	if d, err := l.Admit(ctx, managedRequest(r.Name, 1)); err != nil || !d.Allowed {
		t.Fatalf("available capacity: %+v %v", d, err)
	}
	wantDebt := fmt.Sprintf("D1|%d", tat+step)
	if got := c.Get(ctx, l.key(r.Name)).Val(); got != wantDebt || c.PTTL(ctx, l.key(r.Name)).Val() != -1 {
		t.Fatalf("debt: %q want %q without native expiry", got, wantDebt)
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
}

func TestRedisRejectUnsupportedStateFormats(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("strict", 1, 1)
	r.Policy.Revision = 1
	type invalidState struct{ role, raw string }
	cases := []invalidState{{"D", "0:1:1"}, {"D", "1:3600000000:3"}, {"P", "3600000000:1:1"}}
	for _, version := range []string{"0", "2", "3", "4", "9", "11", "13", "33", "01", "03", ""} {
		cases = append(cases, invalidState{"D", "D" + version + "|0"}, invalidState{"P", "P" + version + "|3600000000:1:1"})
	}
	for _, tc := range cases {
		t.Run(tc.role+"/"+tc.raw, func(t *testing.T) {
			debtKey, policyKey := l.key(r.Name), l.policyKey(r.Name)
			peerDebt, peerPolicy := l.key("peer"), l.policyKey("peer")
			before := map[string]string{debtKey: "D1|0", policyKey: "P1|3600000000:1:1", peerDebt: "D1|0", peerPolicy: "P1|3600000000:1:1"}
			key := debtKey
			if tc.role == "P" {
				key = policyKey
			}
			before[key] = tc.raw
			if err := c.MSet(ctx, before).Err(); err != nil {
				t.Fatal(err)
			}
			assertUnchanged := func() {
				t.Helper()
				for key, want := range before {
					if got, err := c.Get(ctx, key).Result(); err != nil || got != want {
						t.Fatalf("state mutated: %s = %q want %q: %v", key, got, want, err)
					}
				}
			}
			req := managedRequest(r.Name, 1)
			req.Charges = append([]pacelock.Charge{{Name: "peer", Cost: 1}}, req.Charges...)
			if _, err := l.Admit(ctx, req); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
				t.Fatalf("admit accepted %q: %v", tc.raw, err)
			}
			assertUnchanged()
			if tc.role == "P" {
				peer := rule("peer", 1, 1)
				peer.Policy.Revision = 2
				if err := l.UpdateQuotas(ctx, quotaOf(peer), quotaOf(r)); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
					t.Fatalf("update accepted %q: %v", tc.raw, err)
				}
				assertUnchanged()
			}
			cursor := PruneCursor{scope: l.prefix, position: "0", pending: []string{peerDebt, key}}
			if _, err := l.Prune(ctx, cursor, 2); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
				t.Fatalf("prune accepted %q: %v", tc.raw, err)
			}
			assertUnchanged()
		})
	}
}
