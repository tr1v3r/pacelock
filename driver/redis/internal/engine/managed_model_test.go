package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

func TestRedisManagedNumericBoundaries(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	h := pacelock.MaxHorizon.Microseconds()
	q := pacelock.Quota{Name: "max-revision", Policy: pacelock.Policy{Rate: 1, Period: time.Microsecond, Burst: h, Revision: pacelock.MaxRevision}}
	for i := 0; i < 2; i++ {
		if err := l.UpdateQuotas(ctx, q); err != nil {
			t.Fatal(err)
		}
		if got := c.Get(ctx, l.policyKey(q.Name)).Val(); got != fmt.Sprintf("P3|1:%d:%d", h, pacelock.MaxRevision) {
			t.Fatal(got)
		}
	}
	now := pacelock.MaxRevision - 2*h
	script := redis.NewScript(strings.Replace(source, "local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])", fmt.Sprintf("local now = %d", now), 1))
	for _, tc := range []struct {
		name                    string
		step, burst, cost, debt int64
	}{
		{"max-cost", 1, h, h, 0},
		{"max-debt", 1, h, 1, h},
		{"max-interval", h, 1, 1, 0},
		{"pause", 0, h, h, 0},
		{"overburst", 1, h - 1, h, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := tc.name
			keys := []string{l.key(name), l.policyKey(name)}
			p := pacelock.QuotaState{Interval: tc.step, Burst: tc.burst, Revision: pacelock.MaxRevision}
			debt := pacelock.QuotaState{}
			before := ""
			if tc.debt > 0 {
				debt = pacelock.QuotaState{TAT: now + tc.debt, ExpiresAt: now + tc.debt, Interval: 1, Burst: h}
				before = fmt.Sprintf("D3|%d", debt.TAT)
				if err := c.Set(ctx, keys[0], before, 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Set(ctx, keys[1], fmt.Sprintf("P3|%d:%d:%d", tc.step, tc.burst, pacelock.MaxRevision), 0).Err(); err != nil {
				t.Fatal(err)
			}
			want, next, werr := pacelock.EvaluateManaged(now, []pacelock.Charge{{Name: name, Cost: tc.cost}}, []pacelock.QuotaState{p}, []pacelock.QuotaState{debt})
			cmd := script.Run(ctx, c, keys, tc.cost)
			if werr != nil {
				kind := pacelock.ErrInvalid
				if tc.step == 0 {
					kind = pacelock.ErrPaused
				}
				if err := scriptFailure("boundary", cmd.Err()); !errors.Is(err, kind) {
					t.Fatal(err, werr)
				}
			} else {
				values, err := cmd.Int64Slice()
				if err != nil {
					t.Fatal(err)
				}
				got, err := decodeDecision(values, []string{name})
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatal(got, want, err)
				}
			}
			expected := before
			if werr == nil && want.Allowed {
				expected = fmt.Sprintf("D3|%d", next[0].TAT)
			}
			if got := c.Get(ctx, keys[0]).Val(); got != expected {
				t.Fatalf("debt %s want %s", got, expected)
			}
		})
	}
}

func TestRedisManagedCorruptLastIsAtomic(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	for _, name := range []string{"first", "last"} {
		if err := c.Set(ctx, l.policyKey(name), "P3|1:10:1", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Set(ctx, l.key("last"), "D3|corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}
	a := managedRequest("first", 1)
	a.Charges = append(a.Charges, pacelock.Charge{Name: "last", Cost: 1})
	if _, err := l.Admit(ctx, a); !errors.Is(err, pacelock.ErrState) {
		t.Fatal(err)
	}
	if n := c.Exists(ctx, l.key("first")).Val(); n != 0 {
		t.Fatal("first debt written")
	}
	if got := c.Get(ctx, l.key("last")).Val(); got != "D3|corrupt" {
		t.Fatal(got)
	}
}

func TestRedisManagedModel(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	const now = int64(1700000000000000)
	script := redis.NewScript(strings.Replace(source, "local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])", fmt.Sprintf("local now = %d", now), 1))
	rng := rand.New(rand.NewSource(20261028))
	for iteration := 0; iteration < 150; iteration++ {
		n := 1 + rng.Intn(4)
		charges := make([]pacelock.Charge, n)
		policies := make([]pacelock.QuotaState, n)
		debts := make([]pacelock.QuotaState, n)
		keys := make([]string, 2*n)
		args := []interface{}{}
		names := make([]string, n)
		before := make([]string, n)
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("q%d", i)
			step := int64(1 + rng.Intn(1000000))
			burst := int64(1 + rng.Intn(8))
			cost := int64(1 + rng.Intn(10))
			policies[i] = pacelock.QuotaState{Interval: step, Burst: burst, Revision: 1}
			if iteration%7 == 0 && i == n-1 {
				policies[i].Interval = 0
			}
			tat := now + int64(rng.Intn(20))*step
			debts[i] = pacelock.QuotaState{TAT: tat, ExpiresAt: tat, Interval: 1, Burst: 86400000000}
			charges[i] = pacelock.Charge{Name: name, Cost: cost}
			names[i] = name
			keys[i] = l.key(name)
			keys[n+i] = l.policyKey(name)
			args = append(args, cost)
			before[i] = fmt.Sprintf("D3|%d", tat)
			if err := c.MSet(ctx, keys[i], before[i], keys[n+i], fmt.Sprintf("P3|%d:%d:1", policies[i].Interval, burst)).Err(); err != nil {
				t.Fatal(err)
			}
		}
		want, next, wantErr := pacelock.EvaluateManaged(now, charges, policies, debts)
		cmd := script.Run(ctx, c, keys, args...)
		if wantErr != nil {
			got := scriptFailure("model", cmd.Err())
			kind := pacelock.ErrInvalid
			if errors.Is(wantErr, pacelock.ErrPaused) {
				kind = pacelock.ErrPaused
			}
			if !errors.Is(got, kind) {
				t.Fatalf("%d want %v got %v", iteration, wantErr, got)
			}
		} else {
			values, err := cmd.Int64Slice()
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeDecision(values, names)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%d got %+v %v want %+v", iteration, got, err, want)
			}
		}
		for i := 0; i < n; i++ {
			wantRaw := before[i]
			if wantErr == nil && want.Allowed {
				wantRaw = fmt.Sprintf("D3|%d", next[i].TAT)
			}
			if got := c.Get(ctx, keys[i]).Val(); got != wantRaw {
				t.Fatalf("%d debt: %s want %s", iteration, got, wantRaw)
			}
		}
	}
}
