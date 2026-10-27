package redisdriver

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

func TestPrunePublicValidation(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer client.Close()
	driver, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range []*Driver{nil, {}, driver} {
		if _, err := current.Prune(nil, pacelock.Scope{Namespace: "test", Domain: "q"}, PruneCursor{}, 1); !errors.Is(err, pacelock.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := driver.Prune(context.Background(), pacelock.Scope{}, PruneCursor{}, 1); !errors.Is(err, pacelock.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := driver.Prune(context.Background(), pacelock.Scope{Namespace: "test", Domain: "q"}, PruneCursor{}, MaxPruneKeys+1); !errors.Is(err, pacelock.ErrInvalid) {
		t.Fatal(err)
	}
}
