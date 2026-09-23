package admin

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// adminRouteGuardFiles are the ONLY files this test reads: the three
// files whose Routes-returning function builds part of this package's
// route table (impersonationRoutes, userRoutes, orgRoutes). Every
// httpapi.Route these functions return has its Pattern under
// "/api/v1/admin" -- this is the whole route table this Service mounts
// under that prefix, so this file list is also this test's own coverage
// statement.
var adminRouteGuardFiles = []string{"impersonation.go", "users.go", "orgs.go"}

// adminRouteGuardFuncs names the exact functions parsed for a Route
// table -- a route registered anywhere else in this package is invisible
// to this test by construction, so the file list above is what actually
// makes this exhaustive, not this name list.
var adminRouteGuardFuncs = map[string]bool{
	"impersonationRoutes": true,
	"userRoutes":          true,
	"orgRoutes":           true,
}

// adminRouteGuardExceptions names every route this test permits at a
// level OTHER than Admin/Superuser, keyed by "METHOD /path-with-prefix-
// stripped", with the level it is expected to actually carry. Every entry
// here needs its own justification in the comment beside it; this is not
// an escape hatch for a route that just happens not to be Admin/Superuser
// yet.
var adminRouteGuardExceptions = map[string]string{
	// GET /impersonate/status: checks whether the CALLING user is
	// themselves currently impersonating someone -- inherently
	// self-scoped, exactly like GET /me elsewhere. It reads no other
	// principal's state and grants no elevated access, so Authenticated
	// (not Admin/Superuser) is the correct level, not an oversight.
	"GET /impersonate/status": "Authenticated",
}

// discoveredRoute is one httpapi.Route literal this test found, with the
// Authz identifier its guard.Wrap/bodyFirst call was given.
type discoveredRoute struct {
	method, pattern, level, file string
}

func (r discoveredRoute) key() string {
	return r.method + " " + strings.TrimPrefix(r.pattern, "/api/v1/admin")
}

// methodLiteral resolves an http.MethodXxx selector to its verb.
func methodLiteral(e ast.Expr) string {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "http" {
		return ""
	}
	switch sel.Sel.Name {
	case "MethodGet":
		return "GET"
	case "MethodPost":
		return "POST"
	case "MethodPatch":
		return "PATCH"
	case "MethodDelete":
		return "DELETE"
	case "MethodPut":
		return "PUT"
	default:
		return ""
	}
}

// patternLiteral resolves a `xxxPrefix + "/literal/suffix"` expression to
// its full string, since every Pattern in this package is exactly that
// shape (a package-level "/api/v1/admin" prefix constant plus a string
// literal suffix) -- see orgsPrefix/usersPrefix/impersonationPrefix,
// which all equal the same literal.
func patternLiteral(e ast.Expr) string {
	bin, ok := e.(*ast.BinaryExpr)
	if !ok || bin.Op != token.ADD {
		return ""
	}
	prefixIdent, ok := bin.X.(*ast.Ident)
	if !ok {
		return ""
	}
	suffix, ok := bin.Y.(*ast.BasicLit)
	if !ok || suffix.Kind != token.STRING {
		return ""
	}
	unquoted := strings.Trim(suffix.Value, `"`)
	switch prefixIdent.Name {
	case "orgsPrefix", "usersPrefix", "impersonationPrefix":
		return "/api/v1/admin" + unquoted
	default:
		return ""
	}
}

// guardLevelOf walks handler's AST directly and returns the Authz level
// name from its guard.Wrap(...)/h.bodyFirst(...) call -- whichever of the
// two the Handler expression actually contains, found anywhere in it so
// this does not depend on how gofmt wrapped the call across lines.
func guardLevelOf(handler ast.Expr) (string, bool) {
	var level string
	found := false
	ast.Inspect(handler, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		name := calleeName(call.Fun)
		if name != "Wrap" && name != "bodyFirst" {
			return true
		}
		sel, ok := call.Args[0].(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "policy" {
			return true
		}
		level = sel.Sel.Name
		found = true
		return false
	})
	return level, found
}

func calleeName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.SelectorExpr:
		return v.Sel.Name
	case *ast.Ident:
		return v.Name
	default:
		return ""
	}
}

// discoverAdminRoutes parses adminRouteGuardFiles with go/parser (never a
// hand-authored duplicate of the route table) and returns every Route
// literal found inside impersonationRoutes/userRoutes/orgRoutes, with the
// method, full pattern, and Authz level name its guard.Wrap(...)/
// h.bodyFirst(...) call was given. It fails the test outright, rather
// than skipping, on any Route entry whose level it cannot statically
// determine -- an unparseable route is worse than an absent one, because
// it would otherwise silently drop out of every assertion below.
func discoverAdminRoutes(t *testing.T) []discoveredRoute {
	t.Helper()
	var routes []discoveredRoute
	fset := token.NewFileSet()
	for _, file := range adminRouteGuardFiles {
		tree, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		ast.Inspect(tree, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !adminRouteGuardFuncs[fn.Name.Name] {
				return true
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				var method, pattern string
				var handler ast.Expr
				for _, elt := range lit.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						return true
					}
					key, ok := kv.Key.(*ast.Ident)
					if !ok {
						return true
					}
					switch key.Name {
					case "Method":
						method = methodLiteral(kv.Value)
					case "Pattern":
						pattern = patternLiteral(kv.Value)
					case "Handler":
						handler = kv.Value
					}
				}
				if method == "" || pattern == "" || handler == nil {
					return true
				}
				level, ok := guardLevelOf(handler)
				if !ok {
					t.Fatalf("%s: route %s %s: could not determine its Authz level from the Handler expression", file, method, pattern)
				}
				routes = append(routes, discoveredRoute{method: method, pattern: pattern, level: level, file: file})
				return true
			})
			return false
		})
	}
	return routes
}

// TestAdminRoutesAreNeverMountedBelowAdmin is R396 (chris's class-level
// ruling from #2843's createUser P1: bodyFirst(policy.Public, ...)
// shipped on a route this Service's own doc comment describes as
// Superuser/Admin-guarded). It parses the real route tables
// (impersonation.go, users.go, orgs.go) and fails loud if any route
// under /api/v1/admin is mounted at a level weaker than Admin/Superuser,
// except the one named, justified exception in
// adminRouteGuardExceptions.
//
// Verified to fail: temporarily changed listOrganizations' own
// guard.Wrap(policy.Superuser, ...) to guard.Wrap(policy.Public, ...) in
// this worktree and confirmed this test failed, naming
// "GET /orgs (orgs.go): guarded at \"Public\", want Admin or Superuser";
// restored the file and confirmed `git diff` was empty before this test
// was committed.
func TestAdminRoutesAreNeverMountedBelowAdmin(t *testing.T) {
	routes := discoverAdminRoutes(t)
	if len(routes) == 0 {
		t.Fatal("discovered zero routes across impersonation.go/users.go/orgs.go -- the AST walk itself is broken, not a passing result")
	}

	sort.Slice(routes, func(i, j int) bool { return routes[i].key() < routes[j].key() })
	seenExceptions := map[string]bool{}
	var violations []string
	for _, route := range routes {
		if !strings.HasPrefix(route.pattern, "/api/v1/admin") {
			continue
		}
		if route.level == "Admin" || route.level == "Superuser" {
			continue
		}
		if wantLevel, exempt := adminRouteGuardExceptions[route.key()]; exempt {
			seenExceptions[route.key()] = true
			if route.level != wantLevel {
				violations = append(violations, fmt.Sprintf("%s %s (%s): exception expects level %q, found %q",
					route.method, route.pattern, route.file, wantLevel, route.level))
			}
			continue
		}
		violations = append(violations, fmt.Sprintf("%s %s (%s): guarded at %q, want Admin or Superuser", route.method, route.pattern, route.file, route.level))
	}
	for key := range adminRouteGuardExceptions {
		if !seenExceptions[key] {
			t.Errorf("adminRouteGuardExceptions names %q, but no such route was discovered -- the exception is stale, remove it", key)
		}
	}
	if len(violations) > 0 {
		t.Fatalf("route(s) under /api/v1/admin not guarded at Admin/Superuser:\n%s", strings.Join(violations, "\n"))
	}
}
