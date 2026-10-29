package pacelock_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/tr1v3r/pacelock"
)

// Both entry points accept the same costs and expose scope-based recovery.
type admissionAPI interface {
	Allow(context.Context, ...pacelock.Charge) (pacelock.Decision, error)
	AllowOnce(context.Context, string, time.Duration, ...pacelock.Charge) (pacelock.Receipt, error)
	Lookup(context.Context, string) (pacelock.Decision, bool, error)
	Wait(context.Context, ...pacelock.Charge) error
}

var (
	_ admissionAPI = (*pacelock.Limiter)(nil)
	_ admissionAPI = (*pacelock.StaticLimiter)(nil)
)

func TestLimiterMethodSets(t *testing.T) {
	for _, tc := range []struct {
		typ  reflect.Type
		want []string
	}{
		{reflect.TypeFor[*pacelock.Limiter](), []string{"Allow", "AllowOnce", "Lookup", "UpdateQuotas", "Wait"}},
		{reflect.TypeFor[*pacelock.StaticLimiter](), []string{"Allow", "AllowOnce", "Lookup", "Wait"}},
	} {
		var names []string
		for i := 0; i < tc.typ.NumMethod(); i++ {
			names = append(names, tc.typ.Method(i).Name)
		}
		if !reflect.DeepEqual(names, tc.want) {
			t.Errorf("%s methods = %v, want %v", tc.typ, names, tc.want)
		}
	}
}
