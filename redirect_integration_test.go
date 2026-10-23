package pacelock

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type askLossHook struct {
	lose      atomic.Bool
	successes atomic.Int64
}

func (*askLossHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (*askLossHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h *askLossHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			if (cmd.Name() == "eval" || cmd.Name() == "evalsha") && cmd.Err() == nil {
				if !cmd.NoRetry() {
					return errors.New("ASK script lost NoRetry marker")
				}
				h.successes.Add(1)
				if h.lose.Load() {
					cmd.SetErr(io.EOF)
					return io.EOF
				}
			}
		}
		return err
	}
}

// Requires disposable masters: temporarily migrates one empty slot, then restores
// its owner. Actual ASK/ASKING, MOVED and cold-cache paths exercise node routing.
func TestClusterLiveRedirects(t *testing.T) {
	addresses := os.Getenv("PACELOCK_REDIS_CLUSTER_ADDRS")
	if addresses == "" {
		t.Skip("Cluster fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ",")})
	t.Cleanup(func() { admin.Close() })
	l, err := New(redis.NewClient(&redis.Options{Addr: "unused", MaxRetries: -1}), "redirect-test", time.Now().String())
	if err != nil {
		t.Fatal(err)
	}
	// The temporary client above is used only to derive keys.
	l.client.(markedScripter).Scripter.(*redis.Client).Close()
	slot, err := admin.ClusterKeySlot(ctx, l.key("q")).Result()
	if err != nil {
		t.Fatal(err)
	}
	slots, err := admin.ClusterSlots(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	var sourceAddr, targetAddr string
	for _, s := range slots {
		if int(slot) >= s.Start && int(slot) <= s.End {
			sourceAddr = s.Nodes[0].Addr
		}
	}
	for _, s := range slots {
		if s.Nodes[0].Addr != sourceAddr {
			targetAddr = s.Nodes[0].Addr
			break
		}
	}
	if sourceAddr == "" || targetAddr == "" {
		t.Fatal("need multiple masters")
	}
	source := redis.NewClient(&redis.Options{Addr: sourceAddr, MaxRetries: -1})
	t.Cleanup(func() { source.Close() })
	target := redis.NewClient(&redis.Options{Addr: targetAddr, MaxRetries: -1})
	t.Cleanup(func() { target.Close() })
	sourceID, err := source.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := target.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Target owns every test key; delete before returning the empty slot.
		target.Do(cleanup, "cluster", "setslot", slot, "node", targetID)
		keys, _ := target.ClusterGetKeysInSlot(cleanup, int(slot), 100).Result()
		if len(keys) > 0 {
			target.Del(cleanup, keys...)
		}
		if err := admin.ForEachMaster(cleanup, func(ctx context.Context, c *redis.Client) error {
			return c.Do(ctx, "cluster", "setslot", slot, "node", sourceID).Err()
		}); err != nil {
			t.Error(err)
		}
	})
	hook := &askLossHook{}
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ","), MaxRetries: -1, MaxRedirects: 3,
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) { return slots, nil },
		NewClient:    func(o *redis.Options) *redis.Client { node := redis.NewClient(o); node.AddHook(hook); return node },
	})
	defer c.Close()
	l.client = safeScripter(c)
	if err := target.Do(ctx, "cluster", "setslot", slot, "importing", sourceID).Err(); err != nil {
		t.Fatal(err)
	}
	if err := source.Do(ctx, "cluster", "setslot", slot, "migrating", targetID).Err(); err != nil {
		t.Fatal(err)
	}
	r := rule("q", 2, 1)
	// Force NOSCRIPT on all masters to combine fallback with ASK.
	if err := admin.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error { return c.ScriptFlush(ctx).Err() }); err != nil {
		t.Fatal(err)
	}
	d, err := l.Allow(ctx, r)
	if err != nil || !d.Allowed {
		t.Fatalf("ASK: %+v %v", d, err)
	}
	if hook.successes.Load() != 1 {
		t.Fatalf("ASK successful executions=%d", hook.successes.Load())
	}
	hook.lose.Store(true)
	_, err = l.Allow(ctx, r)
	if !errors.Is(err, io.EOF) || OutcomeOf(err) != Unknown {
		t.Fatalf("ASK lost reply: %v", err)
	}
	if hook.successes.Load() != 2 {
		t.Fatal("lost ASK response was replayed")
	}
	hook.lose.Store(false)
	if err := admin.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error {
		return c.Do(ctx, "cluster", "setslot", slot, "node", targetID).Err()
	}); err != nil {
		t.Fatal(err)
	}
	// Static old slot map forces MOVED. Denial proves exactly two prior debits.
	d, err = l.Allow(ctx, r)
	if err != nil || d.Allowed {
		t.Fatalf("MOVED accounting: %+v %v", d, err)
	}
}
