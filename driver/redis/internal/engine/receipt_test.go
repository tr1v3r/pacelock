package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

func TestCurrentNamespaceAndFingerprint(t *testing.T) {
	l, err := New(&stubScripter{}, "ns", "domain")
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := fmt.Sprintf("pacelock:{%x}:", sha256.Sum256([]byte("2:nsdomain")))
	if l.prefix != wantPrefix {
		t.Fatalf("prefix=%q want=%q", l.prefix, wantPrefix)
	}
	rules := []Rule{rule("q|quoted\"", 2, 1)}
	_, args, err := l.prepare(rules)
	if err != nil {
		t.Fatal(err)
	}
	args[0] = int64(60000)
	before := append([]interface{}(nil), args...)
	want := (pacelock.StaticAdmission{Scope: pacelock.Scope{Namespace: "ns", Domain: "domain"}, ID: "id", Retention: time.Minute, Rules: rules}).Fingerprint()
	if got := fingerprint(args); got != want {
		t.Fatalf("engine fingerprint=%s core=%s", got, want)
	}
	if !reflect.DeepEqual(args, before) {
		t.Fatal("fingerprint modified arguments")
	}
}

func TestRedisReceiptExpiryBoundary(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	const now int64 = 1700000000000000
	const clock = "local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])"
	fixed := func(tail string) *redis.Script {
		t.Helper()
		text := protocolSource + tail
		if strings.Count(text, clock) != 1 {
			t.Fatal("production clock changed")
		}
		return redis.NewScript(strings.Replace(text, clock, fmt.Sprintf("local now = %d", now), 1))
	}
	body := strings.Repeat("a", 64) + `|{"names":["q"],"values":[1,0,0]}`
	for _, offset := range []int64{-1, 0, 1} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			key := l.attemptKey(fmt.Sprint(offset))
			raw := fmt.Sprintf("%d|%s", now+offset, body)
			if err := c.Set(ctx, key, raw, 0).Err(); err != nil {
				t.Fatal(err)
			}
			got, err := fixed(lookupSource).Run(ctx, c, []string{key}).Text()
			if offset <= 0 {
				if err != redis.Nil {
					t.Fatalf("expired lookup: %q %v", got, err)
				}
			} else if err != nil || got != body {
				t.Fatalf("live lookup: %q %v", got, err)
			}
			if c.Get(ctx, key).Val() != raw {
				t.Fatal("lookup mutated receipt")
			}
			deleted, err := fixed(pruneSource).Run(ctx, c, []string{key}).Int64()
			want := int64(0)
			if offset <= 0 {
				want = 1
			}
			if err != nil || deleted != want {
				t.Fatalf("prune deleted=%d want=%d: %v", deleted, want, err)
			}
		})
	}
}

func malformedReceiptPayloads() []string {
	payloads := []string{
		`{"Names":["q"],"values":[1,0,0]}`,
		`{"names":["q"],"Values":[1,0,0]}`,
		`{"names":["q"],"values":[1,0,0],"Values":[1,0,0]}`,
		`{"names":["q"],"names":["q"],"values":[1,0,0]}`,
		`{"names":["q"],"values":[1,0,0],"values":[1,0,0]}`,
		`{"names":["q"],"values":[1,0,0],"extra":0}`,
		`{"values":[1,0,0],"names":["q"]}`,
		`{"names":["q"]}`,
		`{"values":[1,0,0]}`,
		`{"names":["q"],"values":[1.0,0,0]}`,
		`{"names":["q"],"values":[1e0,0,0]}`,
		`{"names":["q"],"values":[01,0,0]}`,
		`{"names":["q"],"values":[0x1,0,0]}`,
		`{"names":["q"],"values":[1,NaN,0]}`,
		`{"names":["q"],"values":[0,86400000001,1]}`,
	}
	for _, name := range []string{
		"\x80", "\xff", "\xc0\x80", "\xc2", "\xc2q", "\xe0\x80\x80",
		"\xed\xa0\x80", "\xe2\x82", "\xf0\x80\x80\x80", "\xf4\x90\x80\x80",
		"\xf5\x80\x80\x80", "\xf0\x9f\x98", "valid中文\xff",
	} {
		payloads = append(payloads, `{"names":["`+name+`"],"values":[1,0,0]}`)
	}
	return payloads
}

func TestRedisCanonicalReceiptRejectsBeforeMutation(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	fp := strings.Repeat("a", 64)
	payloads := append(malformedReceiptPayloads(),
		`{ "names":["q"],"values":[1,0,0]}`,
		`{"names":["\u0071"],"values":[1,0,0]}`,
	)
	for _, expires := range []string{"1", "9007199254740991"} {
		for i, payload := range payloads {
			t.Run(fmt.Sprintf("%s/%d", expires, i), func(t *testing.T) {
				badKey, peerKey := l.attemptKey("bad"), l.attemptKey("expired-peer")
				raw := expires + "|" + fp + "|" + payload
				peer := "1|" + fp + `|{"names":["peer"],"values":[1,0,0]}`
				if err := c.MSet(ctx, badKey, raw, peerKey, peer, l.key("q"), "1:3600000000:3").Err(); err != nil {
					t.Fatal(err)
				}
				if _, found, err := l.Lookup(ctx, "bad"); found || !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
					t.Fatalf("lookup: found=%t err=%v", found, err)
				}
				if _, err := l.AllowOnce(ctx, "bad", time.Minute, rule("q", 3, 1), rule("side", 3, 1)); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
					t.Fatalf("admit: %v", err)
				}
				managed := managedRequest("q", 1)
				managed.ID, managed.Retention = "bad", time.Minute
				if _, err := l.AdmitManaged(ctx, managed); !errors.Is(err, ErrState) || OutcomeOf(err) != Unknown {
					t.Fatalf("managed admit: %v", err)
				}
				cursor := PruneCursor{scope: l.prefix, position: "0", pending: []string{peerKey, l.key("q"), badKey}}
				if _, err := l.Prune(ctx, cursor, 3); !errors.Is(err, ErrState) || OutcomeOf(err) != NotCharged {
					t.Fatalf("prune: %v", err)
				}
				for key, want := range map[string]string{badKey: raw, peerKey: peer, l.key("q"): "1:3600000000:3"} {
					if got, err := c.Get(ctx, key).Result(); err != nil || got != want {
						t.Fatalf("mutation of %s: %q %v", key, got, err)
					}
				}
				if n, err := c.Exists(ctx, l.key("side")).Result(); err != nil || n != 0 {
					t.Fatalf("peer quota written: %d %v", n, err)
				}
			})
		}
	}
}

func TestRedisCanonicalReceiptUnicodeAndMaximumRetry(t *testing.T) {
	l, c := integration(t)
	ctx := context.Background()
	for i, name := range []string{"ascii", "é中文😀", "\u0080\u07ff\u0800\ud7ff\ue000\uffff\U00010000\U0010ffff", "quote\"slash/\\\n\t\x00"} {
		id := fmt.Sprint(i)
		r := rule(name, 1, 1)
		first, err := l.AllowOnce(ctx, id, time.Minute, r)
		if err != nil || !first.Allowed {
			t.Fatalf("first %q: %+v %v", name, first, err)
		}
		if replay, err := l.AllowOnce(ctx, id, time.Minute, r); err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Decision, first.Decision) {
			t.Fatalf("replay %q: %+v %v", name, replay, err)
		}
		if got, found, err := l.Lookup(ctx, id); err != nil || !found || !reflect.DeepEqual(got, first.Decision) {
			t.Fatalf("lookup %q: %+v %t %v", name, got, found, err)
		}
	}
	// Drive the production encoder at the largest retry bound, independently of
	// elapsed wall time between requests, then read it through the full protocol.
	const payloadScript = `return '9007199254740991|' .. ARGV[1] .. '|' .. encode_decision({'q'}, {0, horizon, 1})`
	raw, err := redis.NewScript(protocolSource+payloadScript).Run(ctx, c, nil, strings.Repeat("a", 64)).Text()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, l.attemptKey("max-retry"), raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if d, found, err := l.Lookup(ctx, "max-retry"); err != nil || !found || d.Allowed || d.RetryAfter != MaxHorizon || !reflect.DeepEqual(d.Limited, []string{"q"}) {
		t.Fatalf("maximum retry: %+v %t %v", d, found, err)
	}
}
