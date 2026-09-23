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
var adminRouteGuardFiles = []string{"impersonation.go", "users.go", "orgs.go", "platformstats.go", "featureflags.go", "auditlogs.go", "ipallowlist.go"}

// adminRouteGuardFuncs names the exact functions parsed for a Route
// table -- a route registered anywhere else in this package is invisible
// to this test by construction, so the file list above is what actually
// makes this exhaustive, not this name list.
var adminRouteGuardFuncs = map[string]bool{
	"impersonationRoutes": true,
	"userRoutes":          true,
	"orgRoutes":           true,
	"platformRoutes":      true,
	"featureRoutes":       true,
	"auditLogRoutes":      true,
	"ipAllowlistRoutes":   true,
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
	case "orgsPrefix", "usersPrefix", "impersonationPrefix", "governancePrefix":
		return "/api/v1/admin" + unquoted
	default:
		return ""
	}
}

// guardLevelOf requires the Handler expression's OWN, TOP-LEVEL call to be
// literally h.guard.Wrap(...)/h.bodyFirst(...) -- never a Wrap/bodyFirst
// call found ANYWHERE inside the expression's subtree, no matter how deep.
//
// The first version of this function used ast.Inspect to walk the whole
// subtree, which accepts a Wrap call whose RESULT is discarded as long as
// one appears somewhere in the expression -- a codex-review round proved
// this by rewriting a route's Handler to an IIFE that calls
// guard.Wrap(policy.Superuser, real) for its side effect and then returns
// the unwrapped `real` handler; that walk still reported "Superuser" and
// the test still passed. Requiring the OUTERMOST call to be the guard
// call closes that gap: an IIFE's Handler value is a call to the IIFE
// itself, not to Wrap/bodyFirst, so it no longer matches at all and this
// function correctly reports "not found" for it.
func guardLevelOf(handler ast.Expr) (string, bool) {
	call, ok := handler.(*ast.CallExpr)
	if !ok {
		return "", false
	}
	name := calleeName(call.Fun)
	// checkOrMethodNotAllowed is the one dispatcher (see its doc comment in
	// ipallowlist.go): it wraps the check handler in bodyFirst(policy.Admin)
	// itself and answers the 405 for other paths before any guard, as
	// FastAPI's router does. TestDispatcherGuardsAtAdmin pins that.
	if name == "checkOrMethodNotAllowed" && len(call.Args) == 0 {
		return "Admin", true
	}
	if len(call.Args) == 0 {
		return "", false
	}
	if name != "Wrap" && name != "bodyFirst" {
		return "", false
	}
	sel, ok := call.Args[0].(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "policy" {
		return "", false
	}
	return sel.Sel.Name, true
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
// Verified to fail, twice, both restored with a matching file digest
// before/after: (1) flipped listOrganizations' own
// guard.Wrap(policy.Superuser, ...) to guard.Wrap(policy.Public, ...),
// which failed naming "GET /orgs (orgs.go): guarded at \"Public\", want
// Admin or Superuser"; (2) a codex-review round's own repro -- rewrote
// the same route's Handler to an IIFE that calls
// guard.Wrap(policy.Superuser, real) for its side effect and returns the
// unwrapped real handler -- which the FIRST version of guardLevelOf
// (ast.Inspect over the whole subtree) missed entirely; the current,
// outermost-call-only version correctly fails it with "could not
// determine its Authz level from the Handler expression".
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

// TestDispatcherGuardsAtAdmin pins the level guardLevelOf assumes for
// checkOrMethodNotAllowed: its source must wrap the check handler in
// bodyFirst(policy.Admin, ...).
func TestDispatcherGuardsAtAdmin(t *testing.T) {
	tree, err := parser.ParseFile(token.NewFileSet(), "ipallowlist.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	ast.Inspect(tree, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "checkOrMethodNotAllowed" {
			return true
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if level, guarded := guardLevelOf(call); guarded && level == "Admin" && calleeName(call.Fun) == "bodyFirst" {
					found = true
				}
			}
			return true
		})
		return false
	})
	if !found {
		t.Fatal("checkOrMethodNotAllowed no longer wraps the check handler in bodyFirst(policy.Admin, ...)")
	}
}
