package pacelock

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Compare the production Lua algorithm against a token-bucket oracle with exact
// integer capacity. Only Redis TIME is replaced to make boundary cases exact.
func TestRedisGCRAModel(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	now := time.Now().UnixMicro()
	deterministic := redis.NewScript(strings.Replace(source, "local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])", fmt.Sprintf("local now = %d", now), 1))
	random := rand.New(rand.NewSource(20261023))
	for iteration := 0; iteration < 250; iteration++ {
		n := 1 + random.Intn(8)
		rules := make([]Rule, n)
		want := Decision{Allowed: true}
		var waits []int64
		before := make([]string, n)
		candidates := make([]int64, n)
		for i := range rules {
			step := int64(1 + random.Intn(1000000))
			burst := int64(1 + random.Intn(20))
			cost := int64(1 + random.Intn(int(burst)))
			// Used capacity is represented in microsecond units, avoiding float rounding.
			used := random.Int63n(burst*step + 1)
			if iteration%5 == 0 {
				used = 0
			}
			if iteration%7 == 0 {
				used = burst * step
			}
			rules[i] = Rule{Name: fmt.Sprintf("q%d", i), Rate: 1, Period: time.Duration(step) * time.Microsecond, Burst: burst, Cost: cost}
			before[i] = fmt.Sprintf("%d:%d:%d", now+used, step, burst)
			if err := c.Set(ctx, l.key(rules[i].Name), before[i], 0).Err(); err != nil {
				t.Fatal(err)
			}
			free := burst*step - used
			wait := cost*step - free
			if wait < 0 {
				wait = 0
			}
			waits = append(waits, wait)
			candidates[i] = now + used + cost*step
			if wait > 0 {
				want.Allowed = false
				want.Limited = append(want.Limited, rules[i].Name)
				if time.Duration(wait)*time.Microsecond > want.RetryAfter {
					want.RetryAfter = time.Duration(wait) * time.Microsecond
				}
			}
		}
		keys, args, err := l.prepare(rules)
		if err != nil {
			t.Fatal(err)
		}
		values, err := deterministic.Run(ctx, c, keys, args...).Int64Slice()
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeDecision(values, namesOf(rules))
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration=%d waits=%v got=%+v want=%+v err=%v", iteration, waits, got, want, err)
		}
		for i, r := range rules {
			state, err := c.Get(ctx, l.key(r.Name)).Result()
			expected := before[i]
			if want.Allowed {
				expected = fmt.Sprintf("%d:%d:%d", candidates[i], r.Period.Microseconds(), r.Burst)
			}
			// A very short allowed debt can expire before this read; only then is nil valid.
			if want.Allowed && err == redis.Nil {
				continue
			}
			if err != nil || state != expected {
				t.Fatalf("iteration=%d quota=%s got=%q want=%q err=%v", iteration, r.Name, state, expected, err)
			}
		}
	}
}
