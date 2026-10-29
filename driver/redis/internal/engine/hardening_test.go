package engine

import (
	"context"
	"errors"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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
	if _, err := New(nilRedis, "app", "domain"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("typed nil: %v", err)
	}
	s := &stubScripter{}
	l, _ := New(s, "app", "domain")
	r := rule("q", 1, 1)
	for _, current := range []*Limiter{nil, {}, l} {
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
				l, _ := New(s, "a", "b")
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
	l, _ := New(s, "a", "b")
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
