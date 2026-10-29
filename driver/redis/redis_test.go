package redisdriver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

func TestUpdateQuotasPublicValidation(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer client.Close()
	driver, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	request := pacelock.QuotaUpdate{Scope: pacelock.Scope{Namespace: "test", Domain: "q"}, Quotas: []pacelock.Quota{{Name: "q", Policy: pacelock.Policy{Rate: 1, Period: time.Second, Burst: 1, Revision: 1}}}}
	for _, current := range []*Driver{nil, {}, driver} {
		if err := current.UpdateQuotas(nil, request); !errors.Is(err, pacelock.ErrInvalid) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := driver.UpdateQuotas(ctx, request); !errors.Is(err, context.Canceled) || pacelock.OutcomeOf(err) != pacelock.NotCharged {
		t.Fatal(err)
	}
	request.Quotas[0].Policy.Revision = 0
	if err := driver.UpdateQuotas(context.Background(), request); !errors.Is(err, pacelock.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestAdmissionOperationNames(t *testing.T) {
	request := pacelock.Admission{Scope: pacelock.Scope{Namespace: "test", Domain: "q"}, Charges: []pacelock.Charge{{Name: "q", Cost: 1}}}
	var driver *Driver
	_, err := driver.Admit(context.Background(), request)
	var sdk *pacelock.Error
	if !errors.As(err, &sdk) || sdk.Op != "allow" || sdk.Outcome != pacelock.NotCharged || !errors.Is(err, pacelock.ErrInvalid) {
		t.Fatal(err)
	}
}

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
