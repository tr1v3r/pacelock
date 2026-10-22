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
// integer capacity. The algorithm uses a fixed microsecond clock; Redis expiry
// still uses its real, command-scoped millisecond clock. Snapshot state and PTTL
// in the same EVAL as admission, before even a 1ms key can naturally expire.
// Cross-command expiry/refill is covered separately by TestRedisRefillAndExpiry.
func TestRedisGCRAModel(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	const now int64 = 1700000000000000
	const clock = "local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])"
	if strings.Count(source, clock) != 1 {
		t.Fatal("production clock changed; update the model clock substitution")
	}
	fixedClock := strings.Replace(source, clock, fmt.Sprintf("local now = %d", now), 1)
	// Do not replace SET or its PX argument: state persistence and the relative
	// TTL are independently checked against the oracle below. Redis freezes
	// expiration time within a script, so these reads cannot race natural expiry.
	deterministic := redis.NewScript("local function admit()\n" + fixedClock + `
end
local decision = admit()
local states, ttls = {}, {}
for i = 1, #KEYS do
    states[i] = redis.call('GET', KEYS[i])
    ttls[i] = redis.call('PTTL', KEYS[i])
end
return {decision, states, ttls}
`)
	random := rand.New(rand.NewSource(20261023))
	for iteration := 0; iteration < 250; iteration++ {
		n := 1 + random.Intn(8)
		// Always exercise successful sub-millisecond and rounding-boundary debts,
		// rather than hoping the random multi-quota cases happen to admit one.
		tinySteps := [...]int64{1, 999, 1001}
		if iteration < len(tinySteps) {
			n = 1
		}
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
			if iteration < len(tinySteps) {
				step, burst, cost, used = tinySteps[iteration], 1, 1, 0
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
		reply, err := deterministic.Run(ctx, c, keys, args...).Slice()
		if err != nil {
			t.Fatal(err)
		}
		if len(reply) != 3 {
			t.Fatalf("iteration=%d unexpected snapshot: %v", iteration, reply)
		}
		decisionReply := redis.NewCmd(ctx)
		decisionReply.SetVal(reply[0])
		values, err := decisionReply.Int64Slice()
		if err != nil {
			t.Fatal(err)
		}
		states, statesOK := reply[1].([]interface{})
		ttls, ttlsOK := reply[2].([]interface{})
		if !statesOK || !ttlsOK || len(states) != n || len(ttls) != n {
			t.Fatalf("iteration=%d invalid state/TTL snapshot: %v", iteration, reply)
		}
		got, err := decodeDecision(values, namesOf(rules))
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration=%d waits=%v got=%+v want=%+v err=%v", iteration, waits, got, want, err)
		}
		for i, r := range rules {
			state, stateOK := states[i].(string)
			ttl, ttlOK := ttls[i].(int64)
			expected := before[i]
			// Seeded states are persistent. A denial must change neither the value
			// nor its expiry; an allowance must save the exact debt with ceil(ms).
			expectedTTL := int64(-1)
			if want.Allowed {
				expected = fmt.Sprintf("%d:%d:%d", candidates[i], r.Period.Microseconds(), r.Burst)
				expectedTTL = (candidates[i] - now + 999) / 1000
			}
			if !stateOK || state != expected {
				t.Fatalf("iteration=%d quota=%s state=%v want=%q (missing state is never valid inside the admission script)", iteration, r.Name, states[i], expected)
			}
			if !ttlOK || ttl != expectedTTL {
				t.Fatalf("iteration=%d quota=%s ttl=%v want=%dms", iteration, r.Name, ttls[i], expectedTTL)
			}
		}
	}
}
