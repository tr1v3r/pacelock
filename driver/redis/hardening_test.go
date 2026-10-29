package redisdriver

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	"io"
	"math/big"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type serverError string

func (e serverError) Error() string { return string(e) }
func (e serverError) RedisError()   {}

type stubScripter struct {
	redis.Scripter
	calls     int
	evalCalls int
	reply     interface{}
	err       error
	shaErr    error
}

func (s *stubScripter) EvalSha(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	s.calls++
	c := redis.NewCmd(ctx)
	c.SetVal(s.reply)
	if s.shaErr != nil {
		c.SetErr(s.shaErr)
	} else {
		c.SetErr(s.err)
	}
	return c
}
func (s *stubScripter) Eval(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	s.evalCalls++
	c := redis.NewCmd(ctx)
	c.SetVal(s.reply)
	c.SetErr(s.err)
	return c
}

func TestDefensiveInputs(t *testing.T) {
	var nilRedis *redis.Client
	if _, err := newLimiter(nilRedis, "app", "domain"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("typed nil: %v", err)
	}
	s := &stubScripter{}
	l, _ := newLimiter(s, "app", "domain")
	r := rule("q", 1, 1)
	for _, current := range []*limiter{nil, {}, l} {
		if _, err := current.admitRules(nil, r); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatalf("nil context/limiter: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.admitRules(ctx, r); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if s.calls != 0 {
		t.Fatalf("invalid call reached Redis: %d", s.calls)
	}
}

func TestTransportAndReplyErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reply   interface{}
		err     error
		want    error
		outcome Outcome
	}{
		{"transport", nil, io.ErrUnexpectedEOF, ErrBackend, Unknown},
		{"rejected commit", nil, serverError("PACE_COMMIT NOPERM"), ErrBackend, NotCharged},
		{"canceled after dispatch", nil, context.Canceled, context.Canceled, Unknown},
		{"policy", nil, serverError("PACE_POLICY conflict"), ErrPolicyMismatch, NotCharged},
		{"state", nil, serverError("PACE_STATE wrong type"), ErrState, NotCharged},
		{"non-server error", nil, errors.New("PACE_STATE not a Redis error"), ErrBackend, Unknown},
		{"prefix lookalike", nil, serverError("PACE_STATE_OTHER not ours"), ErrBackend, Unknown},
		{"flag", []interface{}{int64(1), int64(0), int64(2)}, nil, ErrState, Unknown},
		{"allowed with delay", []interface{}{int64(1), int64(1), int64(0)}, nil, ErrState, Unknown},
		{"denied without limit", []interface{}{int64(0), int64(1), int64(0)}, nil, ErrState, Unknown},
		{"denied without delay", []interface{}{int64(0), int64(0), int64(1)}, nil, ErrState, Unknown},
		{"short reply", []interface{}{}, nil, ErrState, Unknown},
		{"reply type", "invalid", nil, ErrState, Unknown},
	} {
		{
			op := "allow"
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				s := &stubScripter{reply: tc.reply, err: tc.err}
				l, _ := newLimiter(s, "a", "b")
				_, err := l.Admit(context.Background(), managedRequest("q", 1))
				if !errors.Is(err, tc.want) || OutcomeOf(err) != tc.outcome {
					t.Fatalf("%v: %s", err, OutcomeOf(err))
				}
				var sdk *Error
				if !errors.As(err, &sdk) || sdk.Op != op {
					t.Fatal(err)
				}
				if s.calls != 1 || s.evalCalls != 0 {
					t.Fatal("transport or malformed reply retried")
				}
			})
		}
	}
	if OutcomeOf(nil) != Unknown || OutcomeOf(io.EOF) != Unknown {
		t.Fatal("untyped error must be conservative")
	}
}

func TestOnlyNoScriptFallsBack(t *testing.T) {
	s := &stubScripter{shaErr: serverError("NOSCRIPT No matching script"), reply: []interface{}{int64(1), int64(0), int64(0)}}
	l, _ := newLimiter(s, "a", "b")
	d, err := l.admitRules(context.Background(), rule("q", 1, 1))
	if err != nil || !d.Allowed || s.calls != 1 || s.evalCalls != 1 {
		t.Fatalf("%+v %v calls=%d/%d", d, err, s.calls, s.evalCalls)
	}
}

func FuzzInterval(f *testing.F) {
	f.Add(int64(3), int64(time.Second), int64(3), int64(2))
	f.Add(int64(1<<62), int64(1), int64(1), int64(1))
	f.Fuzz(func(t *testing.T, rate, period, burst, cost int64) {
		r := Rule{Name: "q", Policy: Policy{Rate: rate, Period: time.Duration(period), Burst: burst}, Cost: cost}
		step, err := interval(r)
		if err != nil {
			return
		}
		if step < 1 || burst < cost || cost < 1 || period <= 0 || period > int64(MaxHorizon) {
			t.Fatal("invalid accepted rule")
		}
		// Independent exact integer oracle: ceil(period_ns / (rate * 1000)).
		n := big.NewInt(period)
		den := new(big.Int).Mul(big.NewInt(rate), big.NewInt(1000))
		expected := new(big.Int).Quo(new(big.Int).Add(n, new(big.Int).Sub(den, big.NewInt(1))), den)
		if step != expected.Int64() || burst > MaxHorizon.Microseconds()/step {
			t.Fatalf("step=%d expected=%s", step, expected)
		}
	})
}

type lostResponse struct {
	redis.Scripter
	calls int
}

func (s *lostResponse) lose(c *redis.Cmd) *redis.Cmd {
	if c.Err() == nil {
		s.calls++
		c.SetErr(io.ErrUnexpectedEOF)
	}
	return c
}
func (s *lostResponse) EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) *redis.Cmd {
	return s.lose(s.Scripter.EvalSha(ctx, sha, keys, args...))
}
func (s *lostResponse) Eval(ctx context.Context, src string, keys []string, args ...interface{}) *redis.Cmd {
	return s.lose(s.Scripter.Eval(ctx, src, keys, args...))
}

func TestRedisLostResponseAccounting(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	wrapped := &lostResponse{Scripter: c}
	broken := &limiter{client: wrapped, prefix: l.prefix}
	r := rule("q", 2, 1)
	publishRules(t, l, r)
	if _, err := broken.admitRules(ctx, r); !errors.Is(err, io.ErrUnexpectedEOF) || OutcomeOf(err) != Unknown {
		t.Fatal(err)
	}
	if wrapped.calls != 1 {
		t.Fatal("retried uncertain command")
	}
	if d, err := l.admitRules(ctx, r); err != nil || !d.Allowed {
		t.Fatalf("double charge: %+v %v", d, err)
	}
	if d, err := l.admitRules(ctx, r); err != nil || d.Allowed {
		t.Fatalf("lost debit: %+v %v", d, err)
	}
}

// An ACL denial of any MSET key rejects the entire commit, across all quota keys.
func TestRedisMSETACLFailureIsUncharged(t *testing.T) {
	l, admin := integration(t)
	ctx := context.Background()
	user := fmt.Sprintf("pacelock-test-%d", time.Now().UnixNano())
	password := "local-test-only"
	err := admin.Do(ctx, "ACL", "SETUSER", user, "on", ">"+password, "+eval", "+evalsha", "+get", "+mset", "+time", "+hello", "+ping", "+client", "%R~"+l.prefix+"*", "%W~"+l.key("first"), "(+eval +evalsha ~"+l.prefix+"*)").Err()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Do(ctx, "ACL", "DELUSER", user) })
	restricted := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, Username: user, Password: password, MaxRetries: -1})
	defer restricted.Close()
	limited := &limiter{client: restricted, prefix: l.prefix}
	rules := []Rule{rule("first", 2, 1), rule("second", 2, 1)}
	publishRules(t, l, rules...)
	if _, err := limited.admitRules(ctx, rules...); err == nil || OutcomeOf(err) != NotCharged {
		t.Fatalf("want rejected commit: %v", err)
	}
	if admin.Exists(ctx, l.key("first"), l.key("second")).Val() != 0 {
		t.Fatal("rejected MSET partially committed")
	}
	if r, err := l.admitRules(ctx, rules...); err != nil || !r.Allowed {
		t.Fatalf("retry after known rejection: %+v %v", r, err)
	}
}

func TestRedisMalformedStates(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("bad", 1, 1)
	publishRules(t, l, r, rule("fresh", 1, 1))
	for _, state := range []string{"broken", "D1|-1", "D1|0:1", "D1|9007199254740992", "D1|1.5", "D1|1e3"} {
		if err := c.Set(ctx, l.key("bad"), state, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := l.admitRules(ctx, rule("fresh", 1, 1), r); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
			t.Fatalf("%q: %v", state, err)
		}
		if c.Exists(ctx, l.key("fresh")).Val() != 0 {
			t.Fatal("invalid state partially charged")
		}
	}
	c.Del(ctx, l.key("bad"))
	c.LPush(ctx, l.key("bad"), "wrongtype")
	if _, err := l.admitRules(ctx, r); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	c.Del(ctx, l.key("bad"))
}

// Same effective policy at increasing revisions must never reset active debt,
// even when publishers and admissions race on different connections.
func testContentionPolicyUpdates(t *testing.T, f *contentionFixture) {
	l := f.limiter(t)
	r := contentionRule("q", 37, 1)
	publishRules(t, l, r)
	var admitted atomic.Int64
	contentionWave(t, 400, func(ctx context.Context, i int) {
		current := f.caller(l, i)
		if i%2 == 0 {
			q := r
			q.Policy.Revision = int64(i + 2)
			if err := current.UpdateQuotas(ctx, quotaOf(q)); err != nil && !errors.Is(err, ErrPolicyMismatch) {
				t.Error(err)
			}
			return
		}
		d, err := current.Admit(ctx, managedRequest("q", 1))
		if err != nil {
			t.Error(err)
		} else if d.Allowed {
			admitted.Add(1)
		}
	})
	if admitted.Load() != 37 {
		t.Fatalf("policy publication reset capacity: %d", admitted.Load())
	}
	requireDenied(t, l, r)
}

func TestRedisAdmissionIgnoresUnknownKeys(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	r := rule("q", 2, 1)
	publishRules(t, l, r)
	key := l.prefix + "unknown:" + strings.Repeat("a", 64)
	const unrelated = "malformed unknown value"
	if err := c.Set(ctx, key, unrelated, 0).Err(); err != nil {
		t.Fatal(err)
	}
	for _, allowed := range []bool{true, true, false} {
		d, err := l.Admit(ctx, managedRequest("q", 1))
		if err != nil || d.Allowed != allowed {
			t.Fatalf("%+v %v", d, err)
		}
	}
	if got := c.Get(ctx, key).Val(); got != unrelated {
		t.Fatalf("unknown key changed: %q", got)
	}
}

// Assert there is no caller-policy payload or mode selector on the wire.
type admissionProtocolSpy struct {
	stubScripter
	keys []string
	args []interface{}
}

func (s *admissionProtocolSpy) EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) *redis.Cmd {
	s.keys = append([]string(nil), keys...)
	s.args = append([]interface{}(nil), args...)
	return s.stubScripter.EvalSha(ctx, sha, keys, args...)
}
func TestAdmissionProtocol(t *testing.T) {
	spy := &admissionProtocolSpy{stubScripter: stubScripter{reply: []interface{}{int64(1), int64(0), int64(0), int64(0)}}}
	l, err := newLimiter(spy, "test", "protocol")
	if err != nil {
		t.Fatal(err)
	}
	request := managedRequest("a", 2)
	request.Charges = append(request.Charges, pacelock.Charge{Name: "b", Cost: 3})
	d, err := l.Admit(context.Background(), request)
	if err != nil || !d.Allowed {
		t.Fatal(d, err)
	}
	if !reflect.DeepEqual(spy.keys, []string{l.key("a"), l.key("b"), l.policyKey("a"), l.policyKey("b")}) || !reflect.DeepEqual(spy.args, []interface{}{int64(2), int64(3)}) {
		t.Fatal(spy.keys, spy.args)
	}
}

type Policy = pacelock.Policy

func OutcomeOf(err error) Outcome { return pacelock.OutcomeOf(err) }

// Rules are fixture descriptions only. Runtime admission carries names and costs.
func (l *limiter) admitRules(ctx context.Context, rules ...Rule) (Decision, error) {
	request := pacelock.Admission{Scope: pacelock.Scope{Namespace: "test", Domain: "test"}}
	for _, r := range rules {
		request.Charges = append(request.Charges, pacelock.Charge{Name: r.Name, Cost: r.Cost})
	}
	return l.Admit(ctx, request)
}

func publishRules(t testing.TB, l *limiter, rules ...Rule) {
	t.Helper()
	quotas := make([]pacelock.Quota, len(rules))
	for i, r := range rules {
		p := r.Policy
		if p.Revision == 0 {
			p.Revision = 1
		}
		quotas[i] = pacelock.Quota{Name: r.Name, Policy: p}
	}
	if err := l.UpdateQuotas(context.Background(), quotas...); err != nil {
		t.Fatal(err)
	}
}

func (l *limiter) prepare(rules []Rule) ([]string, []interface{}, error) {
	if err := pacelock.Validate(rules...); err != nil {
		return nil, nil, err
	}
	keys := make([]string, 2*len(rules))
	args := make([]interface{}, len(rules))
	for i, r := range rules {
		keys[i] = l.key(r.Name)
		keys[len(rules)+i] = l.policyKey(r.Name)
		args[i] = r.Cost
	}
	return keys, args, nil
}
func namesOf(rules []Rule) []string {
	names := make([]string, len(rules))
	for i, r := range rules {
		names[i] = r.Name
	}
	return names
}
func interval(r Rule) (int64, error) { d, err := r.Interval(); return d.Microseconds(), err }
