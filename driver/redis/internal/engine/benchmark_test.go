package engine

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func BenchmarkValidate(b *testing.B) {
	for _, n := range []int{1, 4, 32} {
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			rules := make([]Rule, n)
			for i := range rules {
				rules[i] = Rule{Name: fmt.Sprint(i), Policy: Policy{Rate: 1000, Period: time.Second, Burst: 1000}, Cost: 1}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := Validate(rules...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRedis(b *testing.B) {
	addr := os.Getenv("PACELOCK_REDIS_ADDR")
	if addr == "" {
		b.Skip("set PACELOCK_REDIS_ADDR")
	}
	for _, n := range []int{1, 4, 32} {
		for _, mode := range []string{"allow", "deny", "once", "replay", "lookup"} {
			b.Run(fmt.Sprintf("%s/rules=%d", mode, n), func(b *testing.B) {
				c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
				defer c.Close()
				l, _ := New(c, "pacelock-bench", fmt.Sprintf("%s-%d", b.Name(), time.Now().UnixNano()))
				ctx := context.Background()
				b.Cleanup(func() {
					clean := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
					defer clean.Close()
					it := clean.Scan(ctx, 0, l.prefix+"*", 1000).Iterator()
					for it.Next(ctx) {
						clean.Del(ctx, it.Val())
					}
					if err := it.Err(); err != nil {
						b.Error(err)
					}
				})
				rules := make([]Rule, n)
				for i := range rules {
					rules[i] = Rule{Name: fmt.Sprint(i), Policy: Policy{Rate: 1000000, Period: time.Second, Burst: 1000000000}, Cost: 1}
					if mode == "deny" {
						rules[i] = rule(fmt.Sprint(i), 1, 1)
					}
				}
				if mode == "deny" {
					if _, err := l.Allow(ctx, rules...); err != nil {
						b.Fatal(err)
					}
				}
				if mode == "replay" || mode == "lookup" {
					if _, err := l.AllowOnce(ctx, "replay", time.Minute, rules...); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					switch mode {
					case "once", "replay":
						id := "replay"
						if mode == "once" {
							id = fmt.Sprint(i)
						}
						d, err := l.AllowOnce(ctx, id, time.Minute, rules...)
						if err != nil || !d.Allowed {
							b.Fatalf("%+v %v", d, err)
						}
					case "lookup":
						_, found, err := l.Lookup(ctx, "replay")
						if err != nil || !found {
							b.Fatalf("%v %v", found, err)
						}
					default:
						d, err := l.Allow(ctx, rules...)
						if err != nil || (mode == "allow" && !d.Allowed) || (mode == "deny" && d.Allowed) {
							b.Fatalf("%+v %v", d, err)
						}
					}
				}
				b.StopTimer()
			})
		}
	}
}

// BenchmarkPruneCandidates measures candidate recheck/deletion without discovery.
// Expired-state setup is outside timing; it is not a SCAN or end-to-end cleanup SLO.
func BenchmarkPruneCandidates(b *testing.B) {
	address := os.Getenv("PACELOCK_REDIS_ADDR")
	if address == "" {
		b.Skip("set PACELOCK_REDIS_ADDR")
	}
	for _, n := range []int{1, 32, MaxPruneKeys} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			c := redis.NewClient(&redis.Options{Addr: address, ContextTimeoutEnabled: true})
			defer c.Close()
			l, err := New(c, "pacelock-prune-bench", fmt.Sprint(time.Now().UnixNano()))
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			keys := make([]string, n)
			pairs := make([]interface{}, 0, 2*n)
			for i := range keys {
				keys[i] = l.key(fmt.Sprint(i))
				pairs = append(pairs, keys[i], "Q2|1:1:1")
			}
			defer c.Del(ctx, keys...)
			cursor := PruneCursor{scope: l.prefix, position: "0", pending: keys}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if err := c.MSet(ctx, pairs...).Err(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				r, err := l.Prune(ctx, cursor, n)
				if err != nil || r.Deleted != int64(n) {
					b.Fatalf("%+v %v", r, err)
				}
			}
			b.ReportMetric(float64(n), "candidates/op")
		})
	}
}
