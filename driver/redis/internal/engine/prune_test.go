package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

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
	broken := &Limiter{client: wrapped, prefix: l.prefix}
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
	l, _ := New(s, "test", "prune")
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
	for _, current := range []*Limiter{nil, {}, l} {
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
		l, _ := New(s, "test", "malformed")
		if _, err := l.Prune(context.Background(), PruneCursor{}, 1); !errors.Is(err, ErrState) {
			t.Fatalf("reply=%v err=%v", reply, err)
		}
	}
	for _, reply := range []interface{}{int64(-1), int64(2), "bad"} {
		s := &pageScripter{replies: []interface{}{reply}}
		l, _ := New(s, "test", "delete")
		if _, err := l.Prune(context.Background(), PruneCursor{scope: l.prefix, position: "0", pending: []string{l.key("q")}}, 1); !errors.Is(err, ErrState) {
			t.Fatal(err)
		}
	}
}
