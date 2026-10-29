package pacelock_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/tr1v3r/pacelock"
)

// Both entry points accept the same costs and support immediate and blocking admission.
type admissionAPI interface {
	Allow(context.Context, ...pacelock.Charge) (pacelock.Decision, error)
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
		{reflect.TypeFor[*pacelock.Limiter](), []string{"Allow", "UpdateQuotas", "Wait"}},
		{reflect.TypeFor[*pacelock.StaticLimiter](), []string{"Allow", "Wait"}},
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

func TestAdmissionDriverMethodSets(t *testing.T) {
	for _, tc := range []struct {
		typ  reflect.Type
		want []string
	}{
		{reflect.TypeFor[pacelock.Driver](), []string{"Admit"}},
		{reflect.TypeFor[pacelock.StaticDriver](), []string{"AdmitStatic"}},
		{reflect.TypeFor[pacelock.Admission](), []string{"Validate"}},
		{reflect.TypeFor[pacelock.StaticAdmission](), []string{"Validate"}},
	} {
		var got []string
		for i := 0; i < tc.typ.NumMethod(); i++ {
			got = append(got, tc.typ.Method(i).Name)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s methods = %v, want %v", tc.typ, got, tc.want)
		}
	}
}

func TestAdmissionFieldSets(t *testing.T) {
	for _, tc := range []struct {
		typ  reflect.Type
		want []string
	}{
		{reflect.TypeFor[pacelock.Admission](), []string{"Scope", "Charges"}},
		{reflect.TypeFor[pacelock.StaticAdmission](), []string{"Scope", "Rules"}},
		{reflect.TypeFor[pacelock.Event](), []string{"Operation", "Duration", "RuleCount", "Result", "RetryAfter", "Err"}},
	} {
		var got []string
		for i := 0; i < tc.typ.NumField(); i++ {
			got = append(got, tc.typ.Field(i).Name)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s fields = %v, want %v", tc.typ, got, tc.want)
		}
	}
}
