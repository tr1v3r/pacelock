package pacelock

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Capture the public API's wire output, without depending on private backend
// layout. Fixed v1 samples also run against the pre-refactor implementation.
type protocolCapture struct {
	redis.Scripter
	keys  []string
	args  []interface{}
	reply interface{}
}

func (c *protocolCapture) EvalSha(ctx context.Context, _ string, keys []string, args ...interface{}) *redis.Cmd {
	c.keys = append([]string(nil), keys...)
	c.args = append([]interface{}(nil), args...)
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(c.reply)
	return cmd
}

func TestRedisV1BoundaryCompatibility(t *testing.T) {
	const quotaKey = "pacelock:v1:{13895c2b0cd0f34a51c48a59df6d54bf60b8ed5e5795d2f8ea247a6e7371392f}:c51e455b41df6c017327e16001dd064b8b6733faeaa69b23d9bd79c8079237d5"
	const attemptKey = "pacelock:v1:{13895c2b0cd0f34a51c48a59df6d54bf60b8ed5e5795d2f8ea247a6e7371392f}:attempt:19f1064b619d49d35392eac7261cd7266c720671fc594f4b226f32bf0bee74ba"
	const fingerprint = "9f13c51ebc96a185f9c40df73615b22b45c4921b341088dd3aaa1d84bd4b223a"
	const record = `C1|` + fingerprint + `|{"names":["tokens"],"values":[1,0,0]}`
	ctx := context.Background()
	c := &protocolCapture{reply: []interface{}{int64(1), int64(0), int64(0)}}
	l, err := New(c, "app", "domain")
	if err != nil {
		t.Fatal(err)
	}
	rule := Rule{Name: "tokens", Rate: 3, Period: time.Second, Burst: 3, Cost: 2}
	d, err := l.Allow(ctx, rule)
	if err != nil || !d.Allowed {
		t.Fatalf("allow: %+v %v", d, err)
	}
	want := []interface{}{int64(0), "", int64(333334), int64(3), int64(2), "tokens"}
	if !reflect.DeepEqual(c.keys, []string{quotaKey}) || !reflect.DeepEqual(c.args, want) {
		t.Fatalf("allow wire changed: %v %v", c.keys, c.args)
	}
	c.reply = []interface{}{int64(1), record}
	receipt, err := l.AllowOnce(ctx, "request-1", time.Minute, rule)
	if err != nil || !receipt.Allowed || !receipt.Replayed {
		t.Fatalf("retained receipt: %+v %v", receipt, err)
	}
	want[0], want[1] = int64(60000), fingerprint
	if !reflect.DeepEqual(c.keys, []string{quotaKey, attemptKey}) || !reflect.DeepEqual(c.args, want) {
		t.Fatalf("idempotent wire changed: %v %v", c.keys, c.args)
	}
	c.reply = record
	d, found, err := l.Lookup(ctx, "request-1")
	if err != nil || !found || !d.Allowed {
		t.Fatalf("retained lookup: %+v found=%t %v", d, found, err)
	}
	if !reflect.DeepEqual(c.keys, []string{attemptKey}) || len(c.args) != 0 {
		t.Fatalf("lookup wire changed: %v %v", c.keys, c.args)
	}
}
