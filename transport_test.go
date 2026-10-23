package pacelock

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

func transportConstructor(client redis.Scripter, withOptions bool) (*Limiter, error) {
	if withOptions {
		return NewWithOptions(client, "transport-test", "domain", Options{})
	}
	return New(client, "transport-test", "domain")
}

func TestNewClientRetryOptions(t *testing.T) {
	for _, withOptions := range []bool{false, true} {
		for _, retries := range []int{-2, -1, 0, 1, 3} {
			t.Run(fmt.Sprintf("options=%t/retries=%d", withOptions, retries), func(t *testing.T) {
				var dials atomic.Int64
				options := &redis.Options{Addr: "127.0.0.1:1", MaxRetries: retries,
					Dialer: func(context.Context, string, string) (net.Conn, error) {
						dials.Add(1)
						return nil, errors.New("constructor must not dial")
					},
				}
				client := redis.NewClient(options)
				defer client.Close()
				normalized := client.Options().MaxRetries
				wantNormalized := retries
				if retries == -1 {
					wantNormalized = 0
				} else if retries == 0 {
					wantNormalized = 3
				}
				if normalized != wantNormalized {
					t.Fatalf("unexpected go-redis normalization: %d", normalized)
				}
				l, err := transportConstructor(client, withOptions)
				if retries == -1 {
					if err != nil || l == nil {
						t.Fatalf("safe client rejected: %v", err)
					}
				} else if !errors.Is(err, ErrInvalid) || l != nil {
					t.Fatalf("unsafe client accepted: limiter=%v err=%v", l, err)
				}
				if dials.Load() != 0 || client.Options().MaxRetries != normalized || options.MaxRetries != retries {
					t.Fatal("constructor dialed or changed retry options")
				}
			})
		}
	}
}

func TestNewClusterRetryOptions(t *testing.T) {
	for _, withOptions := range []bool{false, true} {
		for _, redirects := range []int{-2, -1, 0, 1, 3} {
			for _, retries := range []int{-2, -1, 0, 1} {
				t.Run(fmt.Sprintf("options=%t/redirects=%d/retries=%d", withOptions, redirects, retries), func(t *testing.T) {
					var dials, slots, nodes atomic.Int64
					options := &redis.ClusterOptions{
						Addrs: []string{"127.0.0.1:1"}, MaxRedirects: redirects, MaxRetries: retries,
						Dialer: func(context.Context, string, string) (net.Conn, error) {
							dials.Add(1)
							return nil, errors.New("constructor must not dial")
						},
						ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
							slots.Add(1)
							return nil, errors.New("constructor must not resolve slots")
						},
						NewClient: func(opt *redis.Options) *redis.Client {
							nodes.Add(1)
							return redis.NewClient(opt)
						},
					}
					client := redis.NewClusterClient(options)
					defer client.Close()
					normalizedRedirects, normalizedRetries := client.Options().MaxRedirects, client.Options().MaxRetries
					wantRedirects, wantRetries := redirects, retries
					if redirects == -1 {
						wantRedirects = 0
					} else if redirects == 0 {
						wantRedirects = 3
					}
					if retries == 0 {
						wantRetries = -1
					}
					if normalizedRedirects != wantRedirects || normalizedRetries != wantRetries {
						t.Fatalf("unexpected go-redis normalization: redirects=%d retries=%d", normalizedRedirects, normalizedRetries)
					}
					l, err := transportConstructor(client, withOptions)
					if redirects == -1 && (retries == -1 || retries == 0) {
						if err != nil || l == nil {
							t.Fatalf("safe client rejected: %v", err)
						}
					} else if !errors.Is(err, ErrInvalid) || l != nil {
						t.Fatalf("unsafe client accepted: limiter=%v err=%v", l, err)
					}
					if dials.Load() != 0 || slots.Load() != 0 || nodes.Load() != 0 {
						t.Fatalf("constructor performed I/O: dials=%d slots=%d nodes=%d", dials.Load(), slots.Load(), nodes.Load())
					}
					if client.Options().MaxRedirects != normalizedRedirects || client.Options().MaxRetries != normalizedRetries {
						t.Fatal("constructor changed Cluster retry options")
					}
				})
			}
		}
	}
}

func TestNewCustomTransport(t *testing.T) {
	for _, withOptions := range []bool{false, true} {
		if l, err := transportConstructor(&stubScripter{}, withOptions); err != nil || l == nil {
			t.Fatalf("custom Scripter rejected: %v", err)
		}
		var nilCluster *redis.ClusterClient
		if l, err := transportConstructor(nilCluster, withOptions); l != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("typed-nil Cluster accepted: %v", err)
		}
	}
}

// Directly supply a redirect reply at the node hook so this test needs no Redis.
// With redirects disabled, an application must handle the error rather than
// assume transparent MOVED/ASK recovery remains available.
type redirectHook struct {
	err   error
	calls atomic.Int64
}

func (h *redirectHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *redirectHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *redirectHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(context.Context, redis.Cmder) error {
		h.calls.Add(1)
		return h.err
	}
}

func TestClusterRedirectsDisabled(t *testing.T) {
	for _, kind := range []string{"MOVED", "ASK"} {
		t.Run(kind, func(t *testing.T) {
			hook := &redirectHook{err: serverError(kind + " 1 127.0.0.1:2")}
			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: []string{"127.0.0.1:1"}, MaxRetries: -1, MaxRedirects: -1,
				ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
					return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: "127.0.0.1:1"}}}}, nil
				},
				NewClient: func(opt *redis.Options) *redis.Client {
					c := redis.NewClient(opt)
					c.AddHook(hook)
					return c
				},
			})
			defer client.Close()
			l, err := New(client, "transport-test", kind)
			if err != nil {
				t.Fatal(err)
			}
			_, err = l.Allow(context.Background(), rule("q", 1, 1))
			if !errors.Is(err, ErrBackend) || !errors.Is(err, hook.err) || OutcomeOf(err) != Unknown || hook.calls.Load() != 1 {
				t.Fatalf("redirect not surfaced once: calls=%d err=%v", hook.calls.Load(), err)
			}
		})
	}
}
