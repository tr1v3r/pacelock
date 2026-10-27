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
				rules[i] = Rule{Name: fmt.Sprint(i), Rate: 1000, Period: time.Second, Burst: 1000, Cost: 1}
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
					rules[i] = Rule{Name: fmt.Sprint(i), Rate: 1000000, Period: time.Second, Burst: 1000000000, Cost: 1}
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
