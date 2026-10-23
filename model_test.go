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
// integer capacity. The algorithm uses a fixed microsecond clock, while Redis
// expiry uses real time. Snapshot state in the same EVAL, before keys can expire.
// Redis 7.0 uses dynamic mstime() for both SET PX and PTTL, even inside a script:
// check the exact forwarded PX argument and bracket absolute expiry instead.
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
	// The local facade records SET arguments but forwards them unchanged. The
	// production source is untouched except for the algorithm clock above.
	// Calibration SETs bracket the real expiry base on both Redis 7.0 (dynamic
	// SET time) and newer Redis (command snapshot). Exact PX checks below ensure
	// that an incorrect TTL cannot hide inside that measured time interval.
	deterministic := redis.NewScript(`
local nativeRedis = redis
local calibrationKey = table.remove(KEYS)
local function expiryBase()
    nativeRedis.call('SET', calibrationKey, 'clock', 'PX', 60000)
    return nativeRedis.call('PEXPIRETIME', calibrationKey) - 60000
end
local start = expiryBase()
local writes = {}
local function admit()
    local redis = {
        call = function(command, ...)
            if string.upper(command) == 'SET' then
                writes[#writes + 1] = {...}
            end
            return nativeRedis.call(command, ...)
        end,
        pcall = nativeRedis.pcall,
        error_reply = nativeRedis.error_reply
    }
` + fixedClock + `
end
local decision = admit()
local finish = expiryBase()
nativeRedis.call('DEL', calibrationKey)
local states, expiries = {}, {}
for i = 1, #KEYS do
    states[i] = nativeRedis.call('GET', KEYS[i])
    expiries[i] = nativeRedis.call('PEXPIRETIME', KEYS[i])
end
return {decision, states, expiries, writes, start, finish}
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
		// The wrapper removes this same-slot test key before running admission,
		// preserving the production quota KEYS and ARGV exactly.
		scriptKeys := append(append([]string(nil), keys...), l.key("model-clock"))
		reply, err := deterministic.Run(ctx, c, scriptKeys, args...).Slice()
		if err != nil {
			t.Fatal(err)
		}
		if len(reply) != 6 {
			t.Fatalf("iteration=%d unexpected snapshot: %v", iteration, reply)
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
		start, startOK := reply[4].(int64)
		finish, finishOK := reply[5].(int64)
		if !statesOK || !expiriesOK || !writesOK || len(states) != n || len(expiries) != n || !startOK || !finishOK || start <= 0 || finish < start {
			t.Fatalf("iteration=%d invalid state/expiry snapshot: %v", iteration, reply)
		}
		got, err := decodeDecision(values, namesOf(rules))
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration=%d waits=%v got=%+v want=%+v err=%v", iteration, waits, got, want, err)
		}
		for i, r := range rules {
			state, stateOK := states[i].(string)
			expected := before[i]
			if want.Allowed {
				expected = fmt.Sprintf("%d:%d:%d", candidates[i], r.Period.Microseconds(), r.Burst)
			}
			if !stateOK || state != expected {
				t.Fatalf("iteration=%d quota=%s state=%v want=%q (missing state is never valid inside the admission script)", iteration, r.Name, states[i], expected)
			}
			expiry, expiryOK := expiries[i].(int64)
			if !want.Allowed {
				// Seeded states are persistent; a denial must not write or expire them.
				if len(writes) != 0 || !expiryOK || expiry != -1 {
					t.Fatalf("iteration=%d quota=%s denial wrote state or expiry: writes=%v expiry=%v", iteration, r.Name, writes, expiries[i])
				}
				continue
			}
			if len(writes) != n {
				t.Fatalf("iteration=%d got %d SETs, want exactly %d", iteration, len(writes), n)
			}
			expectedTTL := (candidates[i] - now + 999) / 1000
			write, writeOK := writes[i].([]interface{})
			if !writeOK || !reflect.DeepEqual(write, []interface{}{keys[i], expected, "PX", expectedTTL}) {
				t.Fatalf("iteration=%d quota=%s SET arguments=%v want key/value/PX/%dms", iteration, r.Name, writes[i], expectedTTL)
			}
			if !expiryOK || expiry < start+expectedTTL || expiry > finish+expectedTTL {
				t.Fatalf("iteration=%d quota=%s expiry=%v want measured [%d,%d]ms", iteration, r.Name, expiries[i], start+expectedTTL, finish+expectedTTL)
			}
		}
	}
}
