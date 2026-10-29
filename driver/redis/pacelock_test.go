package redisdriver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestInterval(t *testing.T) {
	base := Rule{Name: "tokens", Policy: Policy{Rate: 3, Period: time.Second, Burst: 3}, Cost: 2}
	step, err := interval(base)
	if err != nil || step != 333334 {
		t.Fatalf("step=%d err=%v", step, err)
	}
	tests := []struct {
		name   string
		change func(*Rule)
	}{
		{"empty name", func(r *Rule) { r.Name = "" }},
		{"long name", func(r *Rule) { r.Name = strings.Repeat("a", 257) }},
		{"zero rate", func(r *Rule) { r.Policy.Rate = 0 }},
		{"negative period", func(r *Rule) { r.Policy.Period = -1 }},
		{"long period", func(r *Rule) { r.Policy.Period = MaxHorizon + 1 }},
		{"zero burst", func(r *Rule) { r.Policy.Burst = 0 }},
		{"zero cost", func(r *Rule) { r.Cost = 0 }},
		{"oversized cost", func(r *Rule) { r.Cost = 4 }},
		{"overflow", func(r *Rule) { r.Policy.Burst = 1 << 62 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			tc.change(&r)
			if _, err := interval(r); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	base.Policy.Period = time.Nanosecond
	base.Policy.Rate = 1 << 62
	if got, err := interval(base); err != nil || got != 1 {
		t.Fatalf("sub-microsecond rounding: %d %v", got, err)
	}
}

func TestValidationBeforeRedis(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:1", MaxRetries: -1})
	defer client.Close()
	if _, err := newLimiter(nil, "app", "domain"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := newLimiter(client, "", "domain"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	l, err := newLimiter(client, "app", "domain")
	if err != nil {
		t.Fatal(err)
	}
	r := Rule{Name: "rpm", Policy: Policy{Rate: 1, Period: time.Second, Burst: 1}, Cost: 1}
	for _, rules := range [][]Rule{nil, {r, r}, make([]Rule, MaxRules+1)} {
		if _, err := l.admitRules(context.Background(), rules...); !errors.Is(err, ErrInvalid) {
			t.Fatalf("err=%v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.admitRules(ctx, r); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	a, _ := newLimiter(client, "a", "bc")
	b, _ := newLimiter(client, "ab", "c")
	if a.prefix == b.prefix {
		t.Fatal("ambiguous namespace/domain encoding")
	}
	if strings.Count(l.key("{injected}:key"), "{") != 1 {
		t.Fatal("untrusted key changed hash tag")
	}
}
