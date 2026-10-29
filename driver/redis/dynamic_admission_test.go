package redisdriver

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

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
