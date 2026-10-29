package redisdriver

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	"io"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRedisPlainQuotaExpiryBoundaries(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	const now int64 = 1700000000000000
	const clock = "local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])"
	if strings.Count(source, clock) != 1 {
		t.Fatal("production clock changed")
	}
	fixedClock := redis.NewScript(strings.Replace(source, clock, fmt.Sprintf("local now = %d", now), 1))
	for _, offset := range []int64{-1, 0, 1} {
		for _, policy := range []string{"same", "step", "burst"} {
			t.Run(fmt.Sprintf("tat_offset=%d/policy=%s", offset, policy), func(t *testing.T) {
				r := Rule{Name: "boundary", Policy: Policy{Rate: 1, Period: 7 * time.Microsecond, Burst: 3}, Cost: 1}
				before := fmt.Sprintf("D1|%d", now+offset)
				if policy == "step" {
					r.Policy.Period = 11 * time.Microsecond
				} else if policy == "burst" {
					r.Policy.Burst = 4
				}
				if err := c.Set(ctx, l.key(r.Name), before, 0).Err(); err != nil {
					t.Fatal(err)
				}
				r.Policy.Revision = int64(1 + (offset+1)*3)
				if policy == "step" {
					r.Policy.Revision++
				}
				if policy == "burst" {
					r.Policy.Revision += 2
				}
				publishRules(t, l, r)
				keys, args, err := l.prepare([]Rule{r})
				if err != nil {
					t.Fatal(err)
				}
				values, err := fixedClock.Run(ctx, c, keys, args...).Int64Slice()

				if err != nil {
					t.Fatal(err)
				}
				d, err := decodeDecision(values, []string{r.Name})
				if err != nil || !d.Allowed {
					t.Fatalf("plain quota boundary: %+v %v", d, err)
				}
				base := now
				if offset > 0 {
					base += offset
				}
				step := r.Policy.Period.Microseconds()
				want := fmt.Sprintf("D1|%d", base+step)
				if raw := c.Get(ctx, l.key(r.Name)).Val(); raw != want {
					t.Fatalf("quota=%q want=%q", raw, want)
				}
				if ttl := c.PTTL(ctx, l.key(r.Name)).Val(); ttl != -1 {
					t.Fatalf("quota has native TTL: %s", ttl)
				}
			})
		}
	}
}

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
for i = 1, #ARGV do
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
			before[i] = fmt.Sprintf("D1|%d", now+used)
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
		for i := range rules {
			rules[i].Policy.Revision = int64(iteration + 1)
		}
		publishRules(t, l, rules...)
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
				expected = fmt.Sprintf("D1|%d", candidates[i])
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
			for i := range rules {
				command = append(command, keys[i], fmt.Sprintf("D1|%d", candidates[i]))
			}
			expectedWrites = append(expectedWrites, command)
		}
		if !reflect.DeepEqual(writes, expectedWrites) {
			t.Fatalf("writes=%v want=%v", writes, expectedWrites)
		}
	}
}

func TestRedisManagedNumericBoundaries(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	h := pacelock.MaxHorizon.Microseconds()
	q := pacelock.Quota{Name: "max-revision", Policy: pacelock.Policy{Rate: 1, Period: time.Microsecond, Burst: h, Revision: pacelock.MaxRevision}}
	for i := 0; i < 2; i++ {
		if err := l.UpdateQuotas(ctx, q); err != nil {
			t.Fatal(err)
		}
		if got := c.Get(ctx, l.policyKey(q.Name)).Val(); got != fmt.Sprintf("P1|1:%d:%d", h, pacelock.MaxRevision) {
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
				before = fmt.Sprintf("D1|%d", debt.TAT)
				if err := c.Set(ctx, keys[0], before, 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Set(ctx, keys[1], fmt.Sprintf("P1|%d:%d:%d", tc.step, tc.burst, pacelock.MaxRevision), 0).Err(); err != nil {
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
				expected = fmt.Sprintf("D1|%d", next[0].TAT)
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
		if err := c.Set(ctx, l.policyKey(name), "P1|1:10:1", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Set(ctx, l.key("last"), "D1|corrupt", 0).Err(); err != nil {
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
	if got := c.Get(ctx, l.key("last")).Val(); got != "D1|corrupt" {
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
			before[i] = fmt.Sprintf("D1|%d", tat)
			if err := c.MSet(ctx, keys[i], before[i], keys[n+i], fmt.Sprintf("P1|%d:%d:1", policies[i].Interval, burst)).Err(); err != nil {
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
				wantRaw = fmt.Sprintf("D1|%d", next[i].TAT)
			}
			if got := c.Get(ctx, keys[i]).Val(); got != wantRaw {
				t.Fatalf("%d debt: %s want %s", iteration, got, wantRaw)
			}
		}
	}
}

func TestRedisMSETCommitFaultBoundaries(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	rules := []Rule{rule("a", 3, 1), rule("b", 3, 1)}
	publishRules(t, l, rules...)
	keys, args, err := l.prepare(rules)
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"before", "encoding", "after"} {
		t.Run(fault, func(t *testing.T) {
			if err := c.Del(ctx, keys[:len(rules)]...).Err(); err != nil {
				t.Fatal(err)
			}
			injected := source
			switch fault {
			case "before":
				injected = strings.Replace(source, "local committed = redis.pcall", "error('injected before commit')\n    local committed = redis.pcall", 1)
			case "encoding":
				injected = strings.Replace(source, "string.format('D1|%.0f'", "error('injected encoding failure'", 1)
			case "after":
				injected = strings.Replace(source, "return result", "error('injected after commit')", 1)
			}
			if _, err := redis.NewScript(injected).Run(ctx, c, keys, args...).Result(); err == nil {
				t.Fatal("fault was not reached")
			}
			count := c.Exists(ctx, keys[:len(rules)]...).Val()
			if fault != "after" {
				if count != 0 {
					t.Fatalf("pre-commit fault left %d keys", count)
				}
			} else {
				if count != int64(len(rules)) {
					t.Fatalf("commit left %d keys", count)
				}
				requireAllowed(t, l, rule("a", 3, 2), rule("b", 3, 2))
			}
		})
	}
}

func TestRedisMSETOutOfMemoryNoPartialWrite(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	settings, err := c.ConfigGet(ctx, "maxmemory").Result()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.ConfigSet(ctx, "maxmemory", settings["maxmemory"]).Err(); err != nil {
			t.Error(err)
		}
	}()
	publishRules(t, l, rule("a", 2, 1), rule("b", 2, 1))
	if err := c.ConfigSet(ctx, "maxmemory", "1").Err(); err != nil {
		t.Fatal(err)
	}
	_, err = l.admitRules(ctx, rule("a", 2, 1), rule("b", 2, 1))
	if err == nil || !errors.Is(err, ErrBackend) || !strings.Contains(strings.ToLower(err.Error()), "oom") {
		t.Fatalf("expected real OOM: %v", err)
	}
	if n := c.Exists(ctx, l.key("a"), l.key("b")).Val(); n != 0 {
		t.Fatalf("OOM partially committed %d keys", n)
	}
}

func TestRedisPlainQuotaPreservesDebtAndRemovesNativeTTL(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	now, err := c.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	const step = int64(time.Hour / time.Microsecond)
	r := rule("plain", 3, 2)
	publishRules(t, l, r)
	tat := now.UnixMicro() + 2*step
	before := fmt.Sprintf("D1|%d", tat)
	if err := c.Set(ctx, l.key(r.Name), before, 3*time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if d, err := l.admitRules(ctx, r); err != nil || d.Allowed {
		t.Fatalf("plain quota debt reset: %+v %v", d, err)
	}
	if raw := c.Get(ctx, l.key(r.Name)).Val(); raw != before {
		t.Fatalf("denial mutated quota: %q", raw)
	}
	r.Cost = 1
	requireAllowed(t, l, r)
	want := fmt.Sprintf("D1|%d", tat+step)
	if raw := c.Get(ctx, l.key(r.Name)).Val(); raw != want {
		t.Fatalf("quota=%q want exact plain format %q", raw, want)
	}
	if ttl := c.PTTL(ctx, l.key(r.Name)).Val(); ttl != -1 {
		t.Fatalf("MSET retained native expiry: %s", ttl)
	}
	requireDenied(t, l, r)
}

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

func quotaOf(r Rule) pacelock.Quota { return pacelock.Quota{Name: r.Name, Policy: r.Policy} }
func managedRequest(name string, cost int64) pacelock.Admission {
	return pacelock.Admission{Scope: pacelock.Scope{Namespace: "test", Domain: "test"}, Charges: []pacelock.Charge{{Name: name, Cost: cost}}}
}

func TestRedisManagedPolicies(t *testing.T) {
	l, c := integration(t)
	exerciseManaged(t, l, c)
}
func TestRedisClusterManagedPolicies(t *testing.T) {
	addresses := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS")
	if addresses == "" {
		t.Skip("set PACELOCK_REDIS_CLUSTER_ADDRS")
	}
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ","), MaxRetries: 3})
	defer c.Close()
	l, err := newLimiter(c, "managed-cluster", fmt.Sprint(time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.ForEachMaster(context.Background(), func(ctx context.Context, node *redis.Client) error {
		it := node.Scan(ctx, 0, l.prefix+"*", 100).Iterator()
		for it.Next(ctx) {
			node.Del(ctx, it.Val())
		}
		return it.Err()
	})
	exerciseManaged(t, l, c)
}
func exerciseManaged(t *testing.T, l *limiter, c redis.UniversalClient) {
	t.Helper()
	ctx := context.Background()
	r := rule("quota", 4, 4)
	r.Policy.Revision = 1
	req := managedRequest(r.Name, 4)
	if _, err := l.Admit(ctx, req); !errors.Is(err, pacelock.ErrPolicyMissing) || OutcomeOf(err) != NotCharged {
		t.Fatalf("missing: %v", err)
	}
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	if c.Exists(ctx, l.key(r.Name)).Val() != 0 {
		t.Fatal("update created debt")
	}
	if d, err := l.Admit(ctx, req); err != nil || !d.Allowed {
		t.Fatalf("initial: %+v %v", d, err)
	}
	debt := c.Get(ctx, l.key(r.Name)).Val()
	if !strings.HasPrefix(debt, "D1|") {
		t.Fatal(debt)
	}
	r.Policy.Revision = 2
	r.Policy.Burst = 1
	r.Policy.Period = time.Minute
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	if c.Get(ctx, l.key(r.Name)).Val() != debt {
		t.Fatal("update changed debt")
	}
	if _, err := l.Admit(ctx, req); !errors.Is(err, ErrInvalid) {
		t.Fatalf("over-burst: %v", err)
	}
	req.Charges[0].Cost = 1
	if d, err := l.Admit(ctx, req); err != nil || d.Allowed || d.RetryAfter < 3*time.Hour {
		t.Fatalf("shrink: %+v %v", d, err)
	}
	equivalent := r
	equivalent.Policy.Rate = 2
	equivalent.Policy.Period = 2 * time.Minute
	// A same-revision update must not rewrite even TTL (test sentinel only).
	if err := c.PExpire(ctx, l.policyKey(r.Name), time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if err := l.UpdateQuotas(ctx, quotaOf(equivalent)); err != nil {
		t.Fatal(err)
	}
	if c.PTTL(ctx, l.policyKey(r.Name)).Val() <= 0 {
		t.Fatal("same revision rewrote policy")
	}
	c.Persist(ctx, l.policyKey(r.Name))
	fresh := rule("fresh", 1, 1)
	fresh.Policy.Revision = 1
	bad := r
	bad.Policy.Burst = 2
	if err := l.UpdateQuotas(ctx, quotaOf(fresh), quotaOf(bad)); !errors.Is(err, ErrPolicyMismatch) {
		t.Fatal(err)
	}
	if c.Exists(ctx, l.policyKey(fresh.Name)).Val() != 0 {
		t.Fatal("partial update")
	}
	r.Policy.Revision = 3
	r.Policy.Rate = 0
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	req.Charges[0].Cost = 4
	if _, err := l.Admit(ctx, req); !errors.Is(err, pacelock.ErrPaused) || OutcomeOf(err) != NotCharged {
		t.Fatalf("paused: %v", err)
	}
	if c.Get(ctx, l.key(r.Name)).Val() != debt {
		t.Fatal("pause mutated state")
	}
	// Expired debt is reclaimable, policy/paused state remains.
	c.Set(ctx, l.key(r.Name), "D1|0", 0)
	var cursor PruneCursor
	for {
		page, err := l.Prune(ctx, cursor, 64)
		if err != nil {
			t.Fatal(err)
		}
		cursor = page.Cursor
		if cursor.Done() {
			break
		}
	}
	if c.Exists(ctx, l.key(r.Name)).Val() != 0 || c.Get(ctx, l.policyKey(r.Name)).Val() != "P1|0:1:3" {
		t.Fatal("prune failed separation")
	}
	if err := l.UpdateQuotas(ctx, quotaOf(equivalent)); !errors.Is(err, ErrPolicyMismatch) {
		t.Fatalf("stale after prune: %v", err)
	}
	r.Policy.Revision = 4
	r.Policy.Rate = 1
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	req.Charges[0].Cost = 1
	if d, err := l.Admit(ctx, req); err != nil || !d.Allowed {
		t.Fatalf("resume: %+v %v", d, err)
	}
	r.Policy.Revision = 5
	r.Policy.Rate = 0
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Admit(ctx, req); !errors.Is(err, pacelock.ErrPaused) || OutcomeOf(err) != NotCharged {
		t.Fatalf("latest policy: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := l.UpdateQuotas(canceled, quotaOf(fresh)); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
}

func TestRedisManagedDebtSurvivesPauseAndResume(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("debt", 2, 1)
	now, err := c.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	debt := fmt.Sprintf("D1|%d", now.UnixMicro()+int64(time.Hour/time.Microsecond))
	if err := c.Set(ctx, l.key(r.Name), debt, 0).Err(); err != nil {
		t.Fatal(err)
	}
	r.Policy.Revision = 1
	r.Policy.Rate = 0
	if e := l.UpdateQuotas(ctx, quotaOf(r)); e != nil {
		t.Fatal(e)
	}
	if c.Get(ctx, l.key(r.Name)).Val() != debt {
		t.Fatal("active debt touched")
	}
	req := managedRequest(r.Name, 1)
	if _, e := l.Admit(ctx, req); !errors.Is(e, pacelock.ErrPaused) {
		t.Fatal(e)
	}
	r.Policy.Revision = 2
	r.Policy.Rate = 1
	r.Policy.Burst = 1
	if e := l.UpdateQuotas(ctx, quotaOf(r)); e != nil {
		t.Fatal(e)
	}
	if d, e := l.Admit(ctx, req); e != nil || d.Allowed {
		t.Fatalf("active debt reset: %+v %v", d, e)
	}
}

func TestRedisManagedMalformed(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("bad", 1, 1)
	r.Policy.Revision = 1
	for _, raw := range []string{"P1|0:1:0", "P1|0:1:9007199254740992", "P1|0:86400000001:1", "P1|1:0:1", "P1|0:1:1:extra", "D1|0"} {
		c.Set(ctx, l.policyKey(r.Name), raw, 0)
		if err := l.UpdateQuotas(ctx, quotaOf(r)); !errors.Is(err, ErrState) {
			t.Fatalf("%s update: %v", raw, err)
		}
		if _, err := l.Admit(ctx, managedRequest(r.Name, 1)); !errors.Is(err, ErrState) {
			t.Fatalf("%s admit: %v", raw, err)
		}
		// SCAN may return an empty first page even with matching keys present.
		cursor := PruneCursor{}
		for {
			result, err := l.Prune(ctx, cursor, MaxPruneKeys)
			if errors.Is(err, ErrState) {
				break
			}
			if err != nil || result.Cursor.Done() {
				t.Fatalf("%s prune: %v", raw, err)
			}
			cursor = result.Cursor
		}
	}
	c.Del(ctx, l.policyKey(r.Name))
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"D1|-1", "D1|9007199254740991", "D1|bad", "Q2|1:1:1", "P1|1:1:1", "E2|1|C1|removed"} {
		c.Set(ctx, l.key(r.Name), raw, 0)
		if _, err := l.Admit(ctx, managedRequest(r.Name, 1)); !errors.Is(err, ErrState) {
			t.Fatalf("%s debt: %v", raw, err)
		}
	}
	// Configuration writes must not read malformed debt.
	r.Policy.Revision = 2
	if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil {
		t.Fatal(err)
	}
}
func TestRedisManagedConcurrentUpdates(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for rev := int64(1); rev <= 50; rev++ {
		wg.Add(1)
		go func(rev int64) {
			defer wg.Done()
			r := rule("concurrent", 1, 1)
			r.Policy.Revision = rev
			if err := l.UpdateQuotas(ctx, quotaOf(r)); err != nil && !errors.Is(err, ErrPolicyMismatch) {
				t.Error(err)
			}
		}(rev)
	}
	wg.Wait()
	if got := c.Get(ctx, l.policyKey("concurrent")).Val(); got != "P1|3600000000:1:50" {
		t.Fatal(got)
	}
}

func TestRedisPruneContracts(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprintf("cluster=%t", cluster), func(t *testing.T) {
			f := newContentionFixture(t, cluster)
			l := f.limiter(t)
			ctx := context.Background()
			now, err := f.clients[0].Time(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			const expired = 137
			for i := 0; i < expired; i++ {
				key, raw := l.key(fmt.Sprint(i)), fmt.Sprintf("D1|%d", now.UnixMicro()-1)
				if err := f.clients[0].Set(ctx, key, raw, 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			live := contentionRule("live", 1000, 1)
			publishRules(t, l, live)
			requireAllowed(t, l, live)
			preservedUnknown := map[string]string{
				l.prefix + "unknown:" + strings.Repeat("b", 64):       "D1|0",
				l.prefix + "policy:" + strings.Repeat("z", 64):        "malformed unknown value",
				l.prefix + strings.Repeat("a", 63):                    "D1|0",
				l.prefix + "policy:policy:" + strings.Repeat("a", 64): "D1|0",
			}
			for key, raw := range preservedUnknown {
				if err := f.clients[0].Set(ctx, key, raw, time.Minute).Err(); err != nil {
					t.Fatal(err)
				}
			}
			before := quotaState(t, f, l, "live")
			cursor := PruneCursor{}
			var deleted int64
			for calls := 0; !cursor.Done(); calls++ {
				if calls > 10000 {
					t.Fatal("scan did not complete")
				}
				r, err := l.Prune(ctx, cursor, 3)
				if err != nil {
					t.Fatal(err)
				}
				if r.Examined > 3 || r.Deleted > int64(r.Examined) {
					t.Fatalf("budget: %+v", r)
				}
				deleted += r.Deleted
				cursor = r.Cursor
			}
			if deleted != expired {
				t.Fatalf("deleted=%d want=%d", deleted, expired)
			}
			if quotaState(t, f, l, "live") != before {
				t.Fatal("cleanup mutated active quota")
			}
			for key, want := range preservedUnknown {
				if raw, err := f.clients[0].Get(ctx, key).Result(); err != nil || raw != want {
					t.Fatalf("cleanup mutated unknown key: got=%q want=%q err=%v", raw, want, err)
				}
			}
			if r, err := l.Prune(ctx, cursor, 3); err != nil || r.Examined != 0 || !r.Cursor.Done() {
				t.Fatalf("completed cursor: %+v %v", r, err)
			}
			// A candidate selected before a concurrent update must be re-read.
			stale := PruneCursor{scope: l.prefix, position: "0", pending: []string{l.key("live")}}
			requireAllowed(t, l, live)
			if r, err := l.Prune(ctx, stale, 1); err != nil || r.Deleted != 0 {
				t.Fatalf("stale candidate deleted fresh debt: %+v %v", r, err)
			}

			// Concurrent cleanup of an old candidate cannot reset newly admitted debt.
			racing := f.limiter(t)
			active := contentionRule("q", 37, 1)
			publishRules(t, racing, active)
			if err := f.clients[0].Set(ctx, racing.key("q"), "D1|1", 0).Err(); err != nil {
				t.Fatal(err)
			}
			selected := PruneCursor{scope: racing.prefix, position: "0", pending: []string{racing.key("q")}}
			var accepted atomic.Int64
			contentionWave(t, 200, func(ctx context.Context, i int) {
				if i%2 == 0 {
					d, err := racing.admitRules(ctx, active)
					if err != nil {
						t.Error(err)
					} else if d.Allowed {
						accepted.Add(1)
					}
				} else if _, err := racing.Prune(ctx, selected, 1); err != nil {
					t.Error(err)
				}
			})
			if accepted.Load() != 37 {
				t.Fatalf("cleanup changed capacity: %d", accepted.Load())
			}
			requireDenied(t, racing, active)

			// Validate every candidate before deleting anything, including expired
			// values whose payload has the wrong format or protocol role.
			for _, entry := range []struct{ name, key, raw string }{
				{"stale Q2", l.key("bad"), "Q2|1:1:1"},
				{"active Q2", l.key("bad"), fmt.Sprintf("Q2|%d:1:1", now.Add(time.Hour).UnixMicro())},
				{"unknown quota", l.key("bad"), "broken"},
				{"missing tat", l.key("bad"), "D1|"},
				{"negative tat", l.key("bad"), "D1|-1"},
				{"fractional tat", l.key("bad"), "D1|1.5"},
				{"inexact tat", l.key("bad"), "D1|9007199254740992"},
				{"D3 debt", l.key("bad"), "D3|0"},
				{"tuple debt", l.key("bad"), "1:1:1"},
				{"policy on quota", l.key("bad"), "P1|1:1:1"},
				{"extra debt field", l.key("bad"), "D1|0:1"},
			} {
				t.Run(entry.name, func(t *testing.T) {
					expiredQuota := l.key("expired-peer")
					before := map[string]string{
						expiredQuota: "D1|1", entry.key: entry.raw,
					}
					if err := f.clients[0].MSet(ctx, before).Err(); err != nil {
						t.Fatal(err)
					}
					cursor := PruneCursor{scope: l.prefix, position: "0", pending: []string{expiredQuota, entry.key}}
					if _, err := l.Prune(ctx, cursor, 3); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
						t.Fatalf("invalid candidate: %v", err)
					}
					for key, want := range before {
						if raw, err := f.clients[0].Get(ctx, key).Result(); err != nil || raw != want {
							t.Fatalf("cleanup mutated %s: got=%q want=%q err=%v", key, raw, want, err)
						}
					}
				})
			}

		})
	}
}

func TestRedisPrunePlainQuotaExpiryBoundaries(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	const now int64 = 1700000000000000
	const clock = "local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])"
	scriptSource := protocolSource + pruneSource
	if strings.Count(scriptSource, clock) != 1 {
		t.Fatal("production clock changed")
	}
	fixedClock := redis.NewScript(strings.Replace(scriptSource, clock, fmt.Sprintf("local now = %d", now), 1))
	var keys []string
	for _, offset := range []int64{-1, 0, 1} {
		key := l.key(fmt.Sprint(offset))
		keys = append(keys, key)
		if err := c.Set(ctx, key, fmt.Sprintf("D1|%d", now+offset), 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if deleted, err := fixedClock.Run(ctx, c, keys).Int64(); err != nil || deleted != 2 {
		t.Fatalf("expiry boundary: deleted=%d err=%v", deleted, err)
	}
	if n := c.Exists(ctx, keys[:2]...).Val(); n != 0 {
		t.Fatalf("tat <= now left %d quota keys", n)
	}
	if raw := c.Get(ctx, keys[2]).Val(); raw != fmt.Sprintf("D1|%d", now+1) {
		t.Fatalf("tat > now was mutated: %q", raw)
	}
}

func TestRedisPruneLostReplyIsSafeToRepeat(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	key := l.key("expired")
	if err := c.Set(ctx, key, "D1|1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	cursor := PruneCursor{scope: l.prefix, position: "0", pending: []string{key}}
	wrapped := &lostResponse{Scripter: c}
	broken := &limiter{client: wrapped, prefix: l.prefix}
	if _, err := broken.Prune(ctx, cursor, 1); !errors.Is(err, io.ErrUnexpectedEOF) || wrapped.calls != 1 {
		t.Fatalf("lost cleanup reply: %v", err)
	}
	if r, err := l.Prune(ctx, cursor, 1); err != nil || r.Deleted != 0 || !r.Cursor.Done() {
		t.Fatalf("repeat cleanup: %+v %v", r, err)
	}
}

// pageScripter returns controlled scan/cleanup replies and records bounded keys.
type pageScripter struct {
	redis.Scripter
	replies []interface{}
	keys    [][]string
}

func (s *pageScripter) EvalSha(ctx context.Context, _ string, keys []string, _ ...interface{}) *redis.Cmd {
	s.keys = append(s.keys, append([]string(nil), keys...))
	cmd := redis.NewCmd(ctx)
	if len(s.replies) == 0 {
		cmd.SetErr(io.ErrUnexpectedEOF)
		return cmd
	}
	cmd.SetVal(s.replies[0])
	s.replies = s.replies[1:]
	return cmd
}

func TestPruneBoundsAndScanOverflow(t *testing.T) {
	s := &pageScripter{}
	l, _ := newLimiter(s, "test", "prune")
	keys := []interface{}{}
	for i := 0; i < 5; i++ {
		keys = append(keys, l.key(fmt.Sprint(i)))
	}
	keys = append(keys, "foreign", l.prefix+"not-an-sdk-key", l.prefix+strings.Repeat("z", 64), l.prefix+"unknown:"+strings.Repeat("a", 64))
	s.replies = []interface{}{[]interface{}{"0", keys}, int64(2), int64(2), int64(1)}
	cursor := PruneCursor{}
	total := int64(0)
	for !cursor.Done() {
		r, err := l.Prune(context.Background(), cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if r.Examined > 2 {
			t.Fatal("overflow exceeded budget")
		}
		total += r.Deleted
		cursor = r.Cursor
	}
	if total != 5 || len(s.keys) != 4 || len(s.keys[1]) != 2 || len(s.keys[3]) != 1 {
		t.Fatalf("overflow lost keys: total=%d calls=%v", total, s.keys)
	}
	calls := len(s.keys)
	for _, current := range []*limiter{nil, {}, l} {
		if _, err := current.Prune(nil, PruneCursor{}, 1); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, limit := range []int{0, MaxPruneKeys + 1} {
		if _, err := l.Prune(context.Background(), PruneCursor{}, limit); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := l.Prune(context.Background(), PruneCursor{scope: "other"}, 1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.Prune(ctx, PruneCursor{}, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(s.keys) != calls {
		t.Fatal("invalid prune reached backend")
	}
}

func TestPruneMalformedReplies(t *testing.T) {
	for _, reply := range []interface{}{nil, []interface{}{}, []interface{}{"-1", []interface{}{}}, []interface{}{"0", "bad"}, []interface{}{"0", []interface{}{1}}} {
		s := &pageScripter{replies: []interface{}{reply}}
		l, _ := newLimiter(s, "test", "malformed")
		if _, err := l.Prune(context.Background(), PruneCursor{}, 1); !errors.Is(err, ErrState) {
			t.Fatalf("reply=%v err=%v", reply, err)
		}
	}
	for _, reply := range []interface{}{int64(-1), int64(2), "bad"} {
		s := &pageScripter{replies: []interface{}{reply}}
		l, _ := newLimiter(s, "test", "delete")
		if _, err := l.Prune(context.Background(), PruneCursor{scope: l.prefix, position: "0", pending: []string{l.key("q")}}, 1); !errors.Is(err, ErrState) {
			t.Fatal(err)
		}
	}
}
