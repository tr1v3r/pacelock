package engine

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

// Compare production Lua against an integer token-bucket oracle. Capture the
// single MSET and resulting values inside the same script, including debts shorter
// than a millisecond. The fixed clock cannot be masked by physical key expiry.
func TestRedisGCRAModel(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	const now int64 = 1700000000000000
	const clock = "local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])"
	if strings.Count(source, clock) != 1 {
		t.Fatal("production clock changed")
	}
	fixedClock := strings.Replace(source, clock, fmt.Sprintf("local now = %d", now), 1)
	deterministic := redis.NewScript(`
local nativeRedis = redis
local writes = {}
local function invoke(method, command, ...)
    if command ~= 'TIME' and command ~= 'GET' then
        writes[#writes + 1] = {command, ...}
    end
    return method(command, ...)
end
local function admit()
    local redis = {
        call = function(command, ...) return invoke(nativeRedis.call, command, ...) end,
        pcall = function(command, ...) return invoke(nativeRedis.pcall, command, ...) end,
        error_reply = nativeRedis.error_reply
    }
` + fixedClock + `
end
local decision = admit()
local states, expiries = {}, {}
for i = 1, #KEYS do
    states[i] = nativeRedis.call('GET', KEYS[i])
    expiries[i] = nativeRedis.call('PEXPIRETIME', KEYS[i])
end
return {decision, states, expiries, writes}
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
			rules[i] = Rule{Name: fmt.Sprintf("q%d", i), Policy: Policy{Rate: 1, Period: time.Duration(step) * time.Microsecond, Burst: burst}, Cost: cost}
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
		if len(reply) != 4 {
			t.Fatalf("unexpected snapshot: %v", reply)
		}
		decisionReply := redis.NewCmd(ctx)
		decisionReply.SetVal(reply[0])
		values, err := decisionReply.Int64Slice()
		if err != nil {
			t.Fatal(err)
		}
		states, statesOK := reply[1].([]interface{})
		expiries, expiriesOK := reply[2].([]interface{})
		writes, writesOK := reply[3].([]interface{})
		if !statesOK || !expiriesOK || !writesOK || len(states) != n || len(expiries) != n {
			t.Fatalf("invalid snapshot: %v", reply)
		}
		got, err := decodeDecision(values, namesOf(rules))
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration=%d waits=%v got=%+v want=%+v err=%v", iteration, waits, got, want, err)
		}
		for i, r := range rules {
			state, stateOK := states[i].(string)
			expected := before[i]
			if want.Allowed {
				expected = fmt.Sprintf("Q2|%d:%d:%d", candidates[i], r.Policy.Period.Microseconds(), r.Policy.Burst)
			}
			if !stateOK || state != expected {
				t.Fatalf("iteration=%d quota=%s state=%v want=%q (missing state is never valid inside the admission script)", iteration, r.Name, states[i], expected)
			}
			if expiry, ok := expiries[i].(int64); !ok || expiry != -1 {
				t.Fatalf("unexpected physical expiry: %v", expiries[i])
			}
		}
		expectedWrites := []interface{}{}
		if want.Allowed {
			command := []interface{}{"MSET"}
			for i, r := range rules {
				command = append(command, keys[i], fmt.Sprintf("Q2|%d:%d:%d", candidates[i], r.Policy.Period.Microseconds(), r.Policy.Burst))
			}
			expectedWrites = append(expectedWrites, command)
		}
		if !reflect.DeepEqual(writes, expectedWrites) {
			t.Fatalf("writes=%v want=%v", writes, expectedWrites)
		}
	}
}
