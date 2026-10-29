package apiservice

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
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

// Every go-api route set that reads public.users gets the platform roles
// switch from startup: session, sso and billing take PlatformRoles, admin
// takes PlatformRolesOwned and AuthSchema. A Deps literal without them would
// read the public is_superuser column while the switch is on.
func TestEveryUsersReaderGetsThePlatformRoleRead(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "service.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"session": {"PlatformRoles"}, "sso": {"PlatformRoles"}, "billing": {"PlatformRoles"},
		"admin": {"PlatformRolesOwned", "AuthSchema"},
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
		keys := map[string]bool{}
		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				if key, ok := kv.Key.(*ast.Ident); ok {
					keys[key.Name] = true
				}
			}
		}
		for _, key := range want[pkg.Name] {
			if !keys[key] {
				t.Errorf("%s.Deps literal at offset %d has no %s", pkg.Name, lit.Pos(), key)
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
