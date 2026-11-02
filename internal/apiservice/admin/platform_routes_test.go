package admin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// platformRouteActions is every admin route whose guard names a platform
// action, with the wrap that must carry it and the action. A route listed
// here must be wrapped by exactly that call; a route that is wrapped by a
// platform call and is not listed here fails too.
var platformRouteActions = map[string][2]string{
	"GET /orgs":                                             {"platform", "ActionAdminOrgs"},
	"GET /orgs/{org_id}":                                    {"platform", "ActionAdminOrgs"},
	"POST /orgs":                                            {"platform", "ActionAdminOrgs"},
	"PATCH /orgs/{org_id}":                                  {"platform", "ActionAdminOrgs"},
	"DELETE /orgs/{org_id}":                                 {"platform", "ActionAdminOrgs"},
	"GET /feature-flags":                                    {"platform", "ActionAdminFlags"},
	"PATCH /feature-flags/{flag_id}":                        {"platform", "ActionAdminFlags"},
	"GET /orgs/{org_id}/feature-overrides":                  {"platform", "ActionAdminFlags"},
	"POST /orgs/{org_id}/feature-overrides":                 {"platform", "ActionAdminFlags"},
	"PATCH /orgs/{org_id}/feature-overrides/{override_id}":  {"platform", "ActionAdminFlags"},
	"DELETE /orgs/{org_id}/feature-overrides/{override_id}": {"platform", "ActionAdminFlags"},
	"GET /platform/audit-logs":                              {"platform", "ActionAdminAudit"},
	"GET /platform/stats":                                   {"platform", "ActionAdminStats"},
	"POST /impersonate":                                     {"platform", "ActionAdminImpersonate"},
	"POST /impersonate/stop":                                {"platform", "ActionAdminImpersonate"},
	"GET /users":                                            {"adminor", "ActionAdminUsers"},
	"GET /users/{user_id}":                                  {"adminor", "ActionAdminUsers"},
	"POST /users":                                           {"adminor", "ActionAdminUsers"},
	"PATCH /users/{user_id}":                                {"adminor", "ActionAdminUsers"},
	"POST /users/{user_id}/password":                        {"adminor", "ActionAdminUsers"},
	"DELETE /users/{user_id}":                               {"adminor", "ActionAdminUsers"},
}

// wrapKind names the platform wrap a Handler expression's outermost call is
// ("platform" or "adminor") and the policy.Action* constant it passes.
func wrapKind(handler ast.Expr) (kind, action string) {
	call, ok := handler.(*ast.CallExpr)
	if !ok || len(call.Args) == 0 {
		return "", ""
	}
	switch calleeName(call.Fun) {
	case "WrapPlatform", "bodyFirstPlatform":
		kind = "platform"
	case "WrapAdminOr", "bodyFirstAdminOr":
		kind = "adminor"
	default:
		return "", ""
	}
	if sel, ok := call.Args[0].(*ast.SelectorExpr); ok {
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "policy" {
			action = sel.Sel.Name
		}
	}
	return kind, action
}

// TestPlatformAction_AdminRoutesAreWrappedByTheirAction reads the real route
// tables: each route of platformRouteActions is wrapped by the platform wrap
// of its action, and no other route is.
func TestPlatformAction_AdminRoutesAreWrappedByTheirAction(t *testing.T) {
	found := map[string][2]string{}
	fset := token.NewFileSet()
	for _, file := range adminRouteGuardFiles {
		tree, err := parser.ParseFile(fset, filepath.Join(".", file), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		local, dot := httpapiNames(tree)
		for _, decl := range tree.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !adminRouteGuardFuncs[fn.Name.Name] {
				continue
			}
			for _, lit := range routeLiterals(fn, local, dot) {
				var method, pattern string
				var handler ast.Expr
				for _, elt := range lit.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					switch key, _ := kv.Key.(*ast.Ident); key.Name {
					case "Method":
						method = methodLiteral(kv.Value)
					case "Pattern":
						pattern = patternLiteral(kv.Value)
					case "Handler":
						handler = kv.Value
					}
				}
				kind, action := wrapKind(handler)
				if kind != "" {
					found[method+" "+strings.TrimPrefix(pattern, "/api/v1/admin")] = [2]string{kind, action}
				}
			}
		}
	}
	var problems []string
	for key, want := range platformRouteActions {
		if got, ok := found[key]; !ok || got != want {
			problems = append(problems, key+": wrapped as "+got[0]+" "+got[1]+", want "+want[0]+" "+want[1])
		}
	}
	for key, got := range found {
		if _, ok := platformRouteActions[key]; !ok {
			problems = append(problems, key+": wrapped as "+got[0]+" "+got[1]+" but not listed")
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Error(p)
	}
}
