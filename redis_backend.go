package pacelock

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"fmt"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisBackend owns Redis keys, script dispatch and reply interpretation. Its
// operations receive validated inputs and return domain results with accounting
// outcomes. It never retries ambiguous failures or owns the supplied client.
// Keep this concrete until another backend establishes a useful shared contract.
type redisBackend struct {
	client redis.Scripter
	prefix string
}

func newRedisBackend(client redis.Scripter, namespace, domain string) (redisBackend, error) {
	if err := validateTransport(client); err != nil {
		return redisBackend{}, err
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(namespace), namespace, domain)))
	return redisBackend{client: client, prefix: fmt.Sprintf("pacelock:v1:{%x}:", digest)}, nil
}

func (b redisBackend) initialized() bool { return b.prefix != "" && !nilClient(b.client) }

func nilClient(client redis.Scripter) bool {
	if client == nil {
		return true
	}
	v := reflect.ValueOf(client)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

//go:embed gcra.lua
var source string
var admissionScript = redis.NewScript(source)

// Keep EVAL routing to the primary, even though the script does not write.
var lookupScript = redis.NewScript(`local v = redis.pcall('GET', KEYS[1]); if type(v) == 'table' and v.err then return redis.error_reply('PACE_RECORD unreadable admission record') end; return v`)

func (b redisBackend) key(name string) string {
	return fmt.Sprintf("%s%x", b.prefix, sha256.Sum256([]byte(name)))
}

func (b redisBackend) attemptKey(id string) string {
	return fmt.Sprintf("%sattempt:%x", b.prefix, sha256.Sum256([]byte(id)))
}

// prepare encodes already validated rules in the existing Lua argument order.
func (b redisBackend) prepare(rules []Rule) ([]string, []interface{}) {
	keys := make([]string, len(rules))
	args := make([]interface{}, 0, 2+len(rules)*4)
	args = append(args, int64(0), "")
	for i, r := range rules {
		step, _ := interval(r)
		keys[i] = b.key(r.Name)
		args = append(args, step, r.Burst, r.Cost, r.Name)
	}
	return keys, args
}

func (b redisBackend) allow(ctx context.Context, rules []Rule) (Decision, error) {
	keys, args := b.prepare(rules)
	cmd := admissionScript.Run(ctx, b.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return Decision{}, scriptFailure("allow", err)
	}
	values, err := cmd.Int64Slice()
	if err != nil {
		return Decision{}, failure("allow", Unknown, fmt.Errorf("%w: invalid script reply: %v", ErrState, err))
	}
	d, err := decodeDecision(values, namesOf(rules))
	if err != nil {
		return Decision{}, failure("allow", Unknown, err)
	}
	return d, nil
}

func (b redisBackend) allowOnce(ctx context.Context, id string, retention time.Duration, rules []Rule) (Receipt, error) {
	keys, args := b.prepare(rules)
	args[0] = retention.Milliseconds()
	fp := fingerprint(args)
	args[1] = fp
	keys = append(keys, b.attemptKey(id))
	cmd := admissionScript.Run(ctx, b.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return Receipt{}, scriptFailure("allow_once", err)
	}
	values, err := cmd.Slice()
	if err != nil {
		return Receipt{}, failure("allow_once", Unknown, fmt.Errorf("%w: invalid receipt reply: %v", ErrState, err))
	}
	if len(values) != 2 {
		return Receipt{}, failure("allow_once", Unknown, fmt.Errorf("%w: invalid receipt", ErrState))
	}
	replay, ok := values[0].(int64)
	raw, textOK := values[1].(string)
	if !ok || !textOK || (replay != 0 && replay != 1) {
		return Receipt{}, failure("allow_once", Unknown, fmt.Errorf("%w: invalid receipt", ErrState))
	}
	d, err := decodeRecord(raw, fp, namesOf(rules))
	if err != nil {
		return Receipt{}, failure("allow_once", Unknown, err)
	}
	return Receipt{Decision: d, Replayed: replay == 1}, nil
}

func (b redisBackend) lookup(ctx context.Context, id string) (Decision, bool, error) {
	cmd := lookupScript.Run(ctx, b.client, []string{b.attemptKey(id)})
	if cmd.Err() == redis.Nil {
		return Decision{}, false, nil
	}
	if err := cmd.Err(); err != nil {
		return Decision{}, false, scriptFailure("lookup", err)
	}
	raw, err := cmd.Text()
	if err != nil {
		return Decision{}, false, failure("lookup", Unknown, fmt.Errorf("%w: invalid lookup reply: %v", ErrState, err))
	}
	d, err := decodeRecord(raw, "", nil)
	if err != nil {
		return Decision{}, false, failure("lookup", Unknown, err)
	}
	return d, true, nil
}
