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
	scope := pacelock.Scope{Namespace: "test", Domain: "q"}
	for _, once := range []bool{false, true} {
		dynamic := pacelock.Admission{Scope: scope, Charges: []pacelock.Charge{{Name: "q", Cost: 1}}}
		static := pacelock.StaticAdmission{Scope: scope, Rules: []pacelock.Rule{{Name: "q", Policy: pacelock.Policy{Rate: 1, Period: time.Second, Burst: 1}, Cost: 1}}}
		dynamicOp, staticOp := "allow", "allow_static"
		if once {
			dynamic.ID, static.ID = "id", "id"
			dynamic.Retention, static.Retention = time.Minute, time.Minute
			dynamicOp, staticOp = "allow_once", "allow_static_once"
		}
		var driver *Driver
		_, dynamicErr := driver.Admit(context.Background(), dynamic)
		_, staticErr := driver.AdmitStatic(context.Background(), static)
		for _, tc := range []struct {
			err error
			op  string
		}{{dynamicErr, dynamicOp}, {staticErr, staticOp}} {
			var sdk *pacelock.Error
			if !errors.As(tc.err, &sdk) || sdk.Op != tc.op || sdk.Outcome != pacelock.NotCharged || !errors.Is(tc.err, pacelock.ErrInvalid) {
				t.Fatalf("want %s NotCharged invalid, got %v", tc.op, tc.err)
			}
		}
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
