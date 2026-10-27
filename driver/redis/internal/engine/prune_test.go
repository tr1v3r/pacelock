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
			complete := "C1|" + strings.Repeat("a", 64) + `|{"names":["q"],"values":[1,0,0]}`
			const expired = 137
			for i := 0; i < expired; i++ {
				key, raw := l.key(fmt.Sprint(i)), fmt.Sprintf("Q2|%d:1:1", now.UnixMicro()-1)
				if i%2 == 0 {
					key = l.attemptKey(fmt.Sprint(i))
					raw = fmt.Sprintf("E2|%d|%s", now.UnixMicro()-1, complete)
				}
				if err := f.clients[0].Set(ctx, key, raw, 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			live := contentionRule("live", 1000, 1)
			requireAllowed(t, l, live)
			pending := l.attemptKey("legacy-pending")
			if err := f.clients[0].Set(ctx, pending, "P1|"+strings.Repeat("a", 64), time.Minute).Err(); err != nil {
				t.Fatal(err)
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
			if quotaState(t, f, l, "live") != before || f.clients[0].Exists(ctx, pending).Val() != 1 {
				t.Fatal("cleanup removed active or legacy state")
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
			if err := f.clients[0].Set(ctx, racing.key("q"), "Q2|1:1:1", 0).Err(); err != nil {
				t.Fatal(err)
			}
			selected := PruneCursor{scope: racing.prefix, position: "0", pending: []string{racing.key("q")}}
			var accepted atomic.Int64
			contentionWave(t, 200, func(ctx context.Context, i int) {
				if i%2 == 0 {
					d, err := racing.Allow(ctx, active)
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

			// An unknown encoding remains untouched; malformed v2 expiry errors.
			bad := l.key("bad")
			if err := f.clients[0].Set(ctx, bad, "Q2|invalid", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if _, err := l.Prune(ctx, PruneCursor{scope: l.prefix, position: "0", pending: []string{bad}}, 1); !errors.Is(err, ErrState) {
				t.Fatalf("malformed candidate: %v", err)
			}
			if f.clients[0].Get(ctx, bad).Val() != "Q2|invalid" {
				t.Fatal("cleanup deleted malformed state")
			}

			for _, entry := range []struct{ key, raw string }{
				{l.key("wrong-role"), "E2|1|" + complete},
				{l.attemptKey("wrong-role"), "Q2|1:1:1"},
			} {
				if err := f.clients[0].Set(ctx, entry.key, entry.raw, 0).Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := l.Prune(ctx, PruneCursor{scope: l.prefix, position: "0", pending: []string{entry.key}}, 1); !errors.Is(err, ErrState) {
					t.Fatalf("wrong protocol role: %v", err)
				}
				if f.clients[0].Get(ctx, entry.key).Val() != entry.raw {
					t.Fatal("cleanup deleted corrupt protocol role")
				}
			}

		})
	}
}

func TestRedisPruneLostReplyIsSafeToRepeat(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	key := l.key("expired")
	if err := c.Set(ctx, key, "Q2|1:1:1", 0).Err(); err != nil {
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
	keys = append(keys, "foreign", l.prefix+"not-an-sdk-key", l.prefix+strings.Repeat("z", 64))
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
