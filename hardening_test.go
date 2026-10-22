package pacelock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
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
		if _, err := current.Allow(nil, r); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatalf("nil context/limiter: %v", err)
		}
	}
	for _, tc := range []struct {
		id    string
		ttl   time.Duration
		rules []Rule
	}{
		{"", time.Minute, []Rule{r}},
		{strings.Repeat("x", 257), time.Minute, []Rule{r}},
		{"a", 0, []Rule{r}}, {"a", time.Millisecond + 1, []Rule{r}},
		{"a", MaxHorizon + time.Millisecond, []Rule{r}},
		{"a", time.Minute, []Rule{rule(string([]byte{255}), 1, 1)}},
	} {
		if _, err := l.AllowOnce(context.Background(), tc.id, tc.ttl, tc.rules...); !errors.Is(err, ErrInvalid) || OutcomeOf(err) != NotCharged {
			t.Fatalf("invalid: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.AllowOnce(ctx, "a", time.Minute, r); !errors.Is(err, context.Canceled) || OutcomeOf(err) != NotCharged {
		t.Fatal(err)
	}
	if _, _, err := l.Lookup(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := l.Lookup(context.Background(), ""); !errors.Is(err, ErrInvalid) {
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
		{"canceled after dispatch", nil, context.Canceled, context.Canceled, Unknown},
		{"policy", nil, serverError("PACE_POLICY conflict"), ErrPolicyMismatch, NotCharged},
		{"state", nil, serverError("PACE_STATE wrong type"), ErrState, NotCharged},
		{"record", nil, serverError("PACE_RECORD bad record"), ErrState, Unknown},
		{"pending", nil, serverError("PACE_PENDING incomplete"), ErrOutcomeUnknown, Unknown},
		{"conflict", nil, serverError("PACE_CONFLICT mismatch"), ErrConflict, NotCharged},
		{"non-server error", nil, errors.New("PACE_STATE not a Redis error"), ErrBackend, Unknown},
		{"prefix lookalike", nil, serverError("PACE_STATE_OTHER not ours"), ErrBackend, Unknown},
		{"flag", []interface{}{int64(1), int64(0), int64(2)}, nil, ErrState, Unknown},
		{"allowed with delay", []interface{}{int64(1), int64(1), int64(0)}, nil, ErrState, Unknown},
		{"denied without limit", []interface{}{int64(0), int64(1), int64(0)}, nil, ErrState, Unknown},
		{"denied without delay", []interface{}{int64(0), int64(0), int64(1)}, nil, ErrState, Unknown},
		{"short reply", []interface{}{}, nil, ErrState, Unknown},
		{"reply type", "invalid", nil, ErrState, Unknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &stubScripter{reply: tc.reply, err: tc.err}
			l, _ := New(s, "a", "b")
			_, err := l.Allow(context.Background(), rule("q", 1, 1))
			if !errors.Is(err, tc.want) || OutcomeOf(err) != tc.outcome {
				t.Fatalf("%v: %s", err, OutcomeOf(err))
			}
			var sdk *Error
			if !errors.As(err, &sdk) || sdk.Op != "allow" {
				t.Fatal(err)
			}
			if s.calls != 1 || s.evalCalls != 0 {
				t.Fatal("transport or malformed reply retried")
			}
		})
	}
	if OutcomeOf(nil) != Unknown || OutcomeOf(io.EOF) != Unknown {
		t.Fatal("untyped error must be conservative")
	}
}

func TestOnlyNoScriptFallsBack(t *testing.T) {
	s := &stubScripter{shaErr: serverError("NOSCRIPT No matching script"), reply: []interface{}{int64(1), int64(0), int64(0)}}
	l, _ := New(s, "a", "b")
	d, err := l.Allow(context.Background(), rule("q", 1, 1))
	if err != nil || !d.Allowed || s.calls != 1 || s.evalCalls != 1 {
		t.Fatalf("%+v %v calls=%d/%d", d, err, s.calls, s.evalCalls)
	}
}

func TestObserver(t *testing.T) {
	var events []Event
	s := &stubScripter{reply: []interface{}{int64(1), int64(0), int64(0)}}
	l, _ := NewWithOptions(s, "a", "b", Options{Observer: func(_ context.Context, e Event) { events = append(events, e) }})
	l.Allow(context.Background(), rule("q", 1, 1))
	l.Allow(context.Background())
	s.reply = nil
	s.err = redis.Nil
	l.Lookup(context.Background(), "missing")
	if len(events) != 3 || events[0].Result != "allowed" || events[1].Result != "error" || events[2].Result != "missing" || events[0].RuleCount != 1 {
		t.Fatalf("%+v", events)
	}
	for _, e := range events {
		if e.Duration < 0 {
			t.Fatal(e)
		}
	}
}

func TestRecordValidation(t *testing.T) {
	fp := strings.Repeat("a", 64)
	good := "C1|" + fp + `|{"names":["q"],"values":[1,0,0]}`
	for _, raw := range []string{"", "C1|", "P1|" + fp + "x", "C1|" + strings.Repeat("z", 64) + "|{}", "C1|" + fp + "|{}", "C1|" + fp + `|{"names":["q"],"values":[0,1,2]}`} {
		if _, err := decodeRecord(raw, "", nil); !errors.Is(err, ErrState) {
			t.Fatalf("%q: %v", raw, err)
		}
	}
	if d, err := decodeRecord(good, fp, []string{"q"}); err != nil || !d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	if _, err := decodeRecord(good, fp, []string{"other"}); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	if _, err := decodeRecord("P1|"+fp, "", nil); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
}

func FuzzInterval(f *testing.F) {
	f.Add(int64(3), int64(time.Second), int64(3), int64(2))
	f.Add(int64(1<<62), int64(1), int64(1), int64(1))
	f.Fuzz(func(t *testing.T, rate, period, burst, cost int64) {
		r := Rule{Name: "q", Rate: rate, Period: time.Duration(period), Burst: burst, Cost: cost}
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

func FuzzRecord(f *testing.F) {
	f.Add("C1|" + strings.Repeat("a", 64) + `|{"names":["q"],"values":[1,0,0]}`)
	f.Add("")
	f.Fuzz(func(t *testing.T, raw string) {
		d, err := decodeRecord(raw, "", nil)
		if err == nil && ((!d.Allowed && (len(d.Limited) == 0 || d.RetryAfter <= 0)) || (d.Allowed && (len(d.Limited) != 0 || d.RetryAfter != 0))) {
			t.Fatalf("inconsistent decision: %+v", d)
		}
	})
}

func TestFingerprintBindsParameters(t *testing.T) {
	l, _ := New(&stubScripter{}, "a", "b")
	a := rule("a", 2, 1)
	b := rule("b", 2, 1)
	_, first, _ := l.prepare([]Rule{a, b})
	first[0] = int64(1000)
	_, second, _ := l.prepare([]Rule{b, a})
	second[0] = int64(1000)
	if fingerprint(first) == fingerprint(second) {
		t.Fatal("order not bound")
	}
	a.Rate = 2
	a.Period = 2 * time.Hour
	_, equivalent, _ := l.prepare([]Rule{a, b})
	equivalent[0] = int64(1000)
	if fingerprint(first) != fingerprint(equivalent) {
		t.Fatal("effective policies should match")
	}
	if reflect.DeepEqual(first, second) {
		t.Fatal(fmt.Sprint(first))
	}
}

func TestMalformedReceiptDoesNotAuthorize(t *testing.T) {
	for _, reply := range []interface{}{"bad", []interface{}{}, []interface{}{int64(2), "bad"}, []interface{}{int64(0), "bad"}} {
		s := &stubScripter{reply: reply}
		l, _ := New(s, "a", "b")
		d, err := l.AllowOnce(context.Background(), "id", time.Minute, rule("q", 1, 1))
		if !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown || d.Allowed {
			t.Fatalf("%+v %v", d, err)
		}
		if err.Error() == "" {
			t.Fatal("empty error")
		}
	}
}

func TestLookupMalformedReplyAndObserverContext(t *testing.T) {
	s := &stubScripter{reply: int64(1)}
	var seen Event
	l, _ := NewWithOptions(s, "a", "b", Options{Observer: func(ctx context.Context, e Event) {
		if ctx == nil {
			t.Fatal("nil observer context")
		}
		seen = e
	}})
	if _, _, err := l.Lookup(context.Background(), "id"); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
		t.Fatal(err)
	}
	if seen.Result != "error" {
		t.Fatal(seen)
	}
	if _, err := l.Allow(nil, rule("q", 1, 1)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
