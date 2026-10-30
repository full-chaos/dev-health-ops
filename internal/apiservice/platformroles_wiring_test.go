package apiservice

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// The request scope's impersonation gate reads the switch startup parsed.
func TestTheRequestScopeGetsThePlatformRolesSwitch(t *testing.T) {
	for _, owned := range []bool{false, true} {
		protected, err := newProtection(apiTestConfig(), nil, owned, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		if got := protected.scope.ReadsPlatformRoles(); got != owned {
			t.Errorf("startup switch %v reached the scope as %v", owned, got)
		}
	}
}

// The read every route set gets is the startup switch and schema.
func TestDepsPlatformRolesIsTheStartupSwitch(t *testing.T) {
	for _, owned := range []bool{false, true} {
		deps := Deps{PlatformRolesOwned: owned, AuthSchema: "auth_x"}
		if got, want := deps.platformRoles(), (policy.PlatformRoleRead{Owned: owned, AuthSchema: "auth_x"}); got != want {
			t.Errorf("owned %v: %+v, want %+v", owned, got, want)
		}
	}
}

// Every go-api route set that reads public.users gets the platform roles
// switch from startup: session, sso and billing take PlatformRoles, admin
// takes PlatformRolesOwned and AuthSchema, each from deps. A Deps literal
// without them, or with any other value, would read the public is_superuser
// column while the switch is on.
func TestEveryUsersReaderGetsThePlatformRoleRead(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "service.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	read := map[string]string{"PlatformRoles": "deps.platformRoles()"}
	want := map[string]map[string]string{
		"session": read, "sso": read, "billing": read,
		"admin": {"PlatformRolesOwned": "deps.PlatformRolesOwned", "AuthSchema": "deps.AuthSchema"},
	}
	seen := map[string]int{}
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Deps" {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || want[pkg.Name] == nil {
			return true
		}
		seen[pkg.Name]++
		values := map[string]string{}
		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				if key, ok := kv.Key.(*ast.Ident); ok {
					var text bytes.Buffer
					if err := printer.Fprint(&text, fset, kv.Value); err != nil {
						t.Fatal(err)
					}
					values[key.Name] = text.String()
				}
			}
		}
		for key, value := range want[pkg.Name] {
			if got, ok := values[key]; !ok || got != value {
				t.Errorf("%s.Deps literal at %s: %s is %q, want %q", pkg.Name, fset.Position(lit.Pos()), key, got, value)
			}
		}
		return true
	})
	for pkg := range want {
		if seen[pkg] == 0 {
			t.Errorf("service.go builds no %s.Deps", pkg)
		}
	}
}
