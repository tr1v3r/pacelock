package pacelock_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/tr1v3r/pacelock"
)

// The limiter supports immediate and blocking admission with the same charges.
type admissionAPI interface {
	Allow(context.Context, ...pacelock.Charge) (pacelock.Decision, error)
	Wait(context.Context, ...pacelock.Charge) error
}

var (
	_ admissionAPI = (*pacelock.Limiter)(nil)
)

func TestLimiterMethodSets(t *testing.T) {
	for _, tc := range []struct {
		typ  reflect.Type
		want []string
	}{
		{reflect.TypeFor[*pacelock.Limiter](), []string{"Allow", "UpdateQuotas", "Wait"}},
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
		{reflect.TypeFor[pacelock.Admission](), []string{"Validate"}},
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

// Guard against restoring a second backend entry point, including aliases.
func TestNoStaticEntryPointDeclarations(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{"StaticLimiter": true, "StaticDriver": true, "StaticAdmission": true, "NewStatic": true, "NewStaticWithOptions": true, "ValidateStatic": true, "admitStatic": true, "AdmitStatic": true}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			var name string
			switch n := n.(type) {
			case *ast.TypeSpec:
				name = n.Name.Name
			case *ast.FuncDecl:
				name = n.Name.Name
			case *ast.ValueSpec:
				for _, id := range n.Names {
					if forbidden[id.Name] {
						t.Errorf("obsolete declaration %s", id.Name)
					}
				}
			}
			if forbidden[name] {
				t.Errorf("obsolete declaration %s", name)
			}
			return true
		})
	}
}
