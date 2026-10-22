package admin

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// adminRouteGuardFiles are the files this test reads: every file of this package that builds a route table (a method
// holding an httpapi.Route literal). Every route such a table returns has its Pattern under "/api/v1/admin" -- this is
// the whole route table this Service mounts under that prefix, so this list is also the test's own coverage statement.
// The list is not trusted: TestAdminRouteGuardListsCoverEveryRouteTable parses every non-test file of the package and
// fails for a table in a file that is not listed, and for a listed file that holds none.
var adminRouteGuardFiles = []string{"impersonation.go", "users.go", "orgs.go", "platformstats.go", "featureflags.go", "auditlogs.go", "ipallowlist.go", "retention.go", "settings.go", "llmsettings.go", "pagerduty.go", "pagerduty_disconnect.go", "pagerduty_authorize.go", "pagerduty_callback.go", "pagerduty_manual.go", "pagerduty_bindings.go", "credentialdelete.go", "pagerduty_services.go", "ownmodel.go", "promptcache.go", "servicemodel.go", "setupstatus.go"}

// adminRouteGuardFuncs names the methods parsed for a Route table. Like the file list it is checked against the package
// source (every method holding a Route literal must be named, every name must match one): a route table registered
// anywhere else -- an unlisted method, a function without a receiver, a package-level table -- fails the membership
// test instead of being invisible to this one.
var adminRouteGuardFuncs = map[string]bool{
	"impersonationRoutes":       true,
	"userRoutes":                true,
	"orgRoutes":                 true,
	"platformRoutes":            true,
	"featureRoutes":             true,
	"auditLogRoutes":            true,
	"ipAllowlistRoutes":         true,
	"retentionRoutes":           true,
	"settingsRoutes":            true,
	"llmSettingsRoutes":         true,
	"pagerDutyRoutes":           true,
	"pagerDutyDisconnectRoutes": true,
	"pagerDutyAuthorizeRoutes":  true,
	"pagerDutyCallbackRoutes":   true,
	"pagerDutyManualRoutes":     true,
	"pagerDutyBindingRoutes":    true,
	"credentialDeleteRoutes":    true,
	"pagerDutyServicesRoutes":   true,
	"ownModelRoutes":            true,
	"promptCacheRoutes":         true,
	"serviceModelRoutes":        true,
	"setupStatusRoutes":         true,
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
	prefixValue := map[string]string{
		"orgsPrefix": orgsPrefix, "usersPrefix": usersPrefix, "impersonationPrefix": impersonationPrefix, "governancePrefix": governancePrefix,
		"pagerDutyPrefix": pagerDutyPrefix, "pagerDutyBindingsPrefix": pagerDutyBindingsPrefix,
	}
	if ident, ok := e.(*ast.Ident); ok {
		return prefixValue[ident.Name]
	}
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
	if value, known := prefixValue[prefixIdent.Name]; known {
		return value + unquoted
	}
	return ""
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
	// The dispatchers (dispatcherFiles) wrap their handlers in a guard at
	// Admin themselves and answer the 405 for other paths before any guard,
	// as FastAPI's router does. TestDispatchersGuardAtAdmin pins that.
	if dispatcherFiles[name] != "" && len(call.Args) == 0 {
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
// literal found inside the listed route-table methods, with the
// method, full pattern, and Authz level name its guard.Wrap(...)/
// h.bodyFirst(...) call was given. It fails the test outright, rather
// than skipping, on any Route entry whose level it cannot statically
// determine -- an unparseable route is worse than an absent one, because
// it would otherwise silently drop out of every assertion below.
func discoverAdminRoutesIn(dir string, files []string, funcs map[string]bool) ([]discoveredRoute, []string) {
	var routes []discoveredRoute
	var problems []string
	fset := token.NewFileSet()
	for _, file := range files {
		tree, err := parser.ParseFile(fset, filepath.Join(dir, file), nil, 0)
		if err != nil {
			problems = append(problems, fmt.Sprintf("parse %s: %v", file, err))
			continue
		}
		local, dot := httpapiNames(tree)
		for _, decl := range tree.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !funcs[fn.Name.Name] {
				continue
			}
			for _, lit := range routeLiterals(fn, local, dot) {
				var method, pattern string
				var hasMethod, hasPattern, hasHandler, keyed bool
				keyed = true
				var handler ast.Expr
				for _, elt := range lit.Elts {
					kv, isKV := elt.(*ast.KeyValueExpr)
					if !isKV {
						keyed = false
						break
					}
					key, isIdent := kv.Key.(*ast.Ident)
					if !isIdent {
						keyed = false
						break
					}
					switch key.Name {
					case "Method":
						hasMethod = true
						method = methodLiteral(kv.Value)
					case "Pattern":
						hasPattern = true
						pattern = patternLiteral(kv.Value)
					case "Handler":
						hasHandler = true
						handler = kv.Value
					}
				}
				if !keyed || !hasMethod || !hasPattern || !hasHandler {
					problems = append(problems, fmt.Sprintf("%s: a Route literal in %s that is positional or lacks a keyed Method, Pattern or Handler: discovery reads keyed fields only, so it would drop every guard check for it", file, fn.Name.Name))
					continue
				}
				if method == "" || pattern == "" || handler == nil {
					problems = append(problems, fmt.Sprintf("%s: a Route literal in %s whose Method or Pattern could not be resolved (method %q, pattern %q): dropping it would drop every guard check for it", file, fn.Name.Name, method, pattern))
					continue
				}
				level, found := guardLevelOf(handler)
				if !found {
					problems = append(problems, fmt.Sprintf("%s: route %s %s: could not determine its Authz level from the Handler expression", file, method, pattern))
					continue
				}
				routes = append(routes, discoveredRoute{method: method, pattern: pattern, level: level, file: file})
			}
		}
	}
	return routes, problems
}

// discoverAdminRoutes parses the real package's listed files; any route it cannot resolve fails the test.
func discoverAdminRoutes(t *testing.T) []discoveredRoute {
	t.Helper()
	routes, problems := discoverAdminRoutesIn(".", adminRouteGuardFiles, adminRouteGuardFuncs)
	if len(problems) != 0 {
		t.Fatalf("%s", strings.Join(problems, "\n"))
	}
	return routes
}

// TestAdminRoutesAreNeverMountedBelowAdmin is R396 (chris's class-level
// ruling from #2843's createUser P1: bodyFirst(policy.Public, ...)
// shipped on a route this Service's own doc comment describes as
// Superuser/Admin-guarded). It parses the real route tables
// (the files in adminRouteGuardFiles) and fails loud if any route
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
		t.Fatal("discovered zero routes across adminRouteGuardFiles -- the AST walk itself is broken, not a passing result")
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

// dispatcherFiles names each route-table dispatcher (a handler that tells a
// literal path segment from a wildcard one, because the route table cannot
// register the two side by side) and the file that defines it.
var dispatcherFiles = map[string]string{
	"checkOrMethodNotAllowed":  "ipallowlist.go",
	"resourceTypesOrGetPolicy": "retention.go",
	"categoriesOrListSettings": "settings.go",
}

// TestDispatchersGuardAtAdmin pins the level guardLevelOf assumes for every
// dispatcher: its source must call a guard at policy.Admin, every guard call
// inside it must be at Admin, and every guarded handler must actually be the
// one served -- each guard call's result is assigned to a variable that is
// later called through .ServeHTTP, so a guard whose result is discarded (and
// an unguarded handler returned instead) fails.
func TestDispatchersGuardAtAdmin(t *testing.T) {
	for name, file := range dispatcherFiles {
		tree, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		guards, wrong := 0, 0
		var assigned []string
		served := map[string]bool{}
		ast.Inspect(tree, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Name.Name != name {
				return true
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.AssignStmt:
					for index, rhs := range node.Rhs {
						call, isCall := rhs.(*ast.CallExpr)
						if !isCall || index >= len(node.Lhs) {
							continue
						}
						level, guarded := guardLevelOf(call)
						if !guarded || (calleeName(call.Fun) != "bodyFirst" && calleeName(call.Fun) != "Wrap") {
							continue
						}
						guards++
						if level != "Admin" {
							wrong++
						}
						if ident, isIdent := node.Lhs[index].(*ast.Ident); isIdent {
							assigned = append(assigned, ident.Name)
						}
					}
				case *ast.CallExpr:
					if sel, isSel := node.Fun.(*ast.SelectorExpr); isSel && sel.Sel.Name == "ServeHTTP" {
						if ident, isIdent := sel.X.(*ast.Ident); isIdent {
							served[ident.Name] = true
						}
					}
				}
				return true
			})
			return false
		})
		if guards == 0 || wrong != 0 {
			t.Errorf("%s: %d guard calls, %d not at Admin; it must guard every handler it dispatches at Admin", name, guards, wrong)
		}
		for _, variable := range assigned {
			if !served[variable] {
				t.Errorf("%s: the guarded handler %q is never served through .ServeHTTP; a discarded guard leaves the route unprotected", name, variable)
			}
		}
	}
}

// adminRouteTable is one place in the package that builds an httpapi.Route table: a composite literal of
// httpapi.Route, or of []httpapi.Route, found by parsing every non-test file of the package.
type adminRouteTable struct {
	file     string
	function string // "" when the literal is not inside a function (a package-level table)
	receiver bool
}

// httpapiNames reports how a file spells the httpapi package: the local name it is imported under ("" when it is not
// imported) and whether it is dot-imported. A route table written through an import alias is the same type.
func httpapiNames(tree *ast.File) (local string, dot bool) {
	for _, spec := range tree.Imports {
		path := strings.Trim(spec.Path.Value, `"`)
		if !strings.HasSuffix(path, "/auth/httpapi") {
			continue
		}
		switch {
		case spec.Name == nil:
			return "httpapi", false
		case spec.Name.Name == ".":
			return "", true
		default:
			return spec.Name.Name, false
		}
	}
	return "", false
}

func isHTTPAPIRoute(e ast.Expr, local string, dot bool) bool {
	switch v := e.(type) {
	case *ast.SelectorExpr:
		pkg, ok := v.X.(*ast.Ident)
		return ok && local != "" && pkg.Name == local && v.Sel.Name == "Route"
	case *ast.Ident:
		return dot && v.Name == "Route"
	}
	return false
}

// routeLiterals returns every composite literal under root that is an httpapi.Route: a literal of the Route type
// itself, and the elements of a []httpapi.Route literal (whose own type is elided).
func routeLiterals(root ast.Node, local string, dot bool) []*ast.CompositeLit {
	var out []*ast.CompositeLit
	ast.Inspect(root, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if lit.Type != nil && isHTTPAPIRoute(lit.Type, local, dot) {
			out = append(out, lit)
		}
		if arr, isArr := lit.Type.(*ast.ArrayType); isArr && isHTTPAPIRoute(arr.Elt, local, dot) {
			for _, elt := range lit.Elts {
				if el, isLit := elt.(*ast.CompositeLit); isLit && el.Type == nil {
					out = append(out, el)
				}
			}
		}
		return true
	})
	return out
}

// adminRouteTables returns every place under dir (non-test .go files only) that builds a route table. A function
// that only concatenates other tables (routes, governanceRoutes, Routes) holds no httpapi.Route literal and is not a
// table.
func adminRouteTables(dir string) ([]adminRouteTable, error) {
	names, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	var out []adminRouteTable
	fset := token.NewFileSet()
	for _, path := range names {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		tree, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil, err
		}
		file := filepath.Base(path)
		local, dot := httpapiNames(tree)
		for _, decl := range tree.Decls {
			if len(routeLiterals(decl, local, dot)) == 0 {
				continue
			}
			table := adminRouteTable{file: file}
			if fn, isFn := decl.(*ast.FuncDecl); isFn {
				table.function = fn.Name.Name
				table.receiver = fn.Recv != nil
			}
			out = append(out, table)
		}
	}
	return out, nil
}

// adminRouteMembershipViolations checks the two hand lists above against the package source. Every route table the
// package builds must be a method on its handler type, named in funcs and defined in a file named in files; a list
// entry that no table matches is stale. A table the lists omit is a table every guard in this file cannot see.
func adminRouteMembershipViolations(dir string, files []string, funcs map[string]bool) []string {
	tables, err := adminRouteTables(dir)
	if err != nil {
		return []string{"parse the package: " + err.Error()}
	}
	listedFile := map[string]bool{}
	for _, f := range files {
		listedFile[f] = true
	}
	var out []string
	foundFunc := map[string]bool{}
	foundFile := map[string]bool{}
	for _, tb := range tables {
		switch {
		case tb.function == "":
			out = append(out, fmt.Sprintf("%s: a route table outside any function is invisible to this test's discovery, which parses methods only", tb.file))
			continue
		case !tb.receiver:
			out = append(out, fmt.Sprintf("%s: %s builds a route table without a receiver; discovery parses methods only, so it is invisible here", tb.file, tb.function))
			continue
		}
		foundFunc[tb.function] = true
		foundFile[tb.file] = true
		if !funcs[tb.function] {
			out = append(out, fmt.Sprintf("%s: %s builds a route table but is not in adminRouteGuardFuncs", tb.file, tb.function))
		}
		if !listedFile[tb.file] {
			out = append(out, fmt.Sprintf("%s: %s is defined in a file that is not in adminRouteGuardFiles", tb.file, tb.function))
		}
	}
	for _, f := range files {
		if !foundFile[f] {
			out = append(out, fmt.Sprintf("adminRouteGuardFiles names %s, which holds no route table (or does not exist): the entry is stale", f))
		}
	}
	for fn := range funcs {
		if !foundFunc[fn] {
			out = append(out, fmt.Sprintf("adminRouteGuardFuncs names %s, which no file builds as a route table: the entry is stale", fn))
		}
	}
	sort.Strings(out)
	return out
}

func TestAdminRouteGuardListsCoverEveryRouteTable(t *testing.T) {
	if got := adminRouteMembershipViolations(".", adminRouteGuardFiles, adminRouteGuardFuncs); len(got) != 0 {
		t.Fatalf("the route guard lists do not match the package:\n%s", strings.Join(got, "\n"))
	}
}

const adminExtraHeader = "package admin\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/httpapi\"\n\n"

// copyAdminPackage copies the package's non-test sources into a temp directory so a plant can edit the tree and the
// lists without touching the real package.
func copyAdminPackage(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestAdminRouteMembershipCheckFiresPerPlantedDefect(t *testing.T) {
	without := func(list []string, drop string) []string {
		var out []string
		for _, v := range list {
			if v != drop {
				out = append(out, v)
			}
		}
		if len(out) == len(list) {
			t.Fatalf("%s is not in the list: the plant measures nothing", drop)
		}
		return out
	}
	withoutFunc := func(drop string) map[string]bool {
		out := map[string]bool{}
		for k, v := range adminRouteGuardFuncs {
			if k != drop {
				out[k] = v
			}
		}
		if len(out) == len(adminRouteGuardFuncs) {
			t.Fatalf("%s is not in the funcs: the plant measures nothing", drop)
		}
		return out
	}
	withFunc := func(add string) map[string]bool {
		out := map[string]bool{add: true}
		for k, v := range adminRouteGuardFuncs {
			out[k] = v
		}
		return out
	}
	for _, tc := range []struct {
		name  string
		files []string
		funcs map[string]bool
		extra string // a source file added to the copy
		want  string
	}{
		{"ownmodel.go is dropped from the files list", without(adminRouteGuardFiles, "ownmodel.go"), adminRouteGuardFuncs, "",
			"ownmodel.go: ownModelRoutes is defined in a file that is not in adminRouteGuardFiles"},
		{"ownModelRoutes is dropped from the funcs list", adminRouteGuardFiles, withoutFunc("ownModelRoutes"), "",
			"ownmodel.go: ownModelRoutes builds a route table but is not in adminRouteGuardFuncs"},
		{"a file entry names no table", append(append([]string{}, adminRouteGuardFiles...), "gone.go"), adminRouteGuardFuncs, "",
			"adminRouteGuardFiles names gone.go"},
		{"a func entry names no table", adminRouteGuardFiles, withFunc("ghostRoutes"), "",
			"adminRouteGuardFuncs names ghostRoutes"},
		{"a new method builds a table in an unlisted file", adminRouteGuardFiles, adminRouteGuardFuncs,
			adminExtraHeader + "func (h *handlers) extraRoutes() []httpapi.Route {\n\treturn []httpapi.Route{{Method: \"GET\"}}\n}\n",
			"zz_extra.go: extraRoutes builds a route table but is not in adminRouteGuardFuncs"},
		{"a function without a receiver builds a table", adminRouteGuardFiles, adminRouteGuardFuncs,
			adminExtraHeader + "func freeRoutes() []httpapi.Route {\n\treturn []httpapi.Route{{Method: \"GET\"}}\n}\n",
			"zz_extra.go: freeRoutes builds a route table without a receiver"},
		{"a package-level table", adminRouteGuardFiles, adminRouteGuardFuncs,
			adminExtraHeader + "var extraTable = []httpapi.Route{{Method: \"GET\"}}\n",
			"zz_extra.go: a route table outside any function"},
		{"a method builds a table through an import alias", adminRouteGuardFiles, adminRouteGuardFuncs,
			"package admin\n\nimport api \"github.com/full-chaos/dev-health-ops/internal/auth/httpapi\"\n\nfunc (h *handlers) aliasRoutes() []api.Route {\n\treturn []api.Route{{Method: \"GET\"}}\n}\n",
			"zz_extra.go: aliasRoutes builds a route table but is not in adminRouteGuardFuncs"},
		{"a method builds a table through a dot import", adminRouteGuardFiles, adminRouteGuardFuncs,
			"package admin\n\nimport . \"github.com/full-chaos/dev-health-ops/internal/auth/httpapi\"\n\nfunc (h *handlers) dotRoutes() []Route {\n\treturn []Route{{Method: \"GET\"}}\n}\n",
			"zz_extra.go: dotRoutes builds a route table but is not in adminRouteGuardFuncs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := copyAdminPackage(t)
			if tc.extra != "" {
				if err := os.WriteFile(filepath.Join(dir, "zz_extra.go"), []byte(tc.extra), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			got := strings.Join(adminRouteMembershipViolations(dir, tc.files, tc.funcs), "\n")
			if !strings.Contains(got, tc.want) {
				t.Fatalf("violations do not contain %q:\n%s", tc.want, got)
			}
		})
	}
}

// TestEveryListedRouteFileContributesRoutes pins the coverage statement of the lists: a listed file whose tables
// resolve to no route at all is a file the guards above silently read nothing from.
func TestEveryListedRouteFileContributesRoutes(t *testing.T) {
	perFile := map[string]int{}
	for _, route := range discoverAdminRoutes(t) {
		perFile[route.file]++
	}
	for _, file := range adminRouteGuardFiles {
		if perFile[file] == 0 {
			t.Errorf("%s is listed but contributed no discovered route: its tables are invisible to the level guard", file)
		}
	}
}

func TestAdminRouteDiscoveryRefusesARouteItCannotResolve(t *testing.T) {
	for _, tc := range []struct{ name, file, from, to, want string }{
		{"a route Pattern uses an unknown prefix", "pagerduty_callback.go", `pagerDutyPrefix + "/callback"`, `someOtherPrefix + "/callback"`,
			"a Route literal in pagerDutyCallbackRoutes whose Method or Pattern could not be resolved"},
		{"a route Method is not an http.MethodXxx selector", "ownmodel.go", "http.MethodPut", `"PUT"`,
			"a Route literal in ownModelRoutes whose Method or Pattern could not be resolved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := copyAdminPackage(t)
			path := filepath.Join(dir, tc.file)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(raw), tc.from) {
				t.Fatalf("%s no longer contains %q: the plant measures nothing", tc.file, tc.from)
			}
			if err := os.WriteFile(path, []byte(strings.Replace(string(raw), tc.from, tc.to, 1)), 0o644); err != nil {
				t.Fatal(err)
			}
			_, problems := discoverAdminRoutesIn(dir, adminRouteGuardFiles, adminRouteGuardFuncs)
			if got := strings.Join(problems, "\n"); !strings.Contains(got, tc.want) {
				t.Fatalf("problems do not contain %q:\n%s", tc.want, got)
			}
		})
	}
}

func TestAdminRouteDiscoveryRefusesAPositionalRouteAndFollowsAnAlias(t *testing.T) {
	edit := func(t *testing.T, file string, edits ...[2]string) string {
		t.Helper()
		dir := copyAdminPackage(t)
		path := filepath.Join(dir, file)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, e := range edits {
			if !strings.Contains(text, e[0]) {
				t.Fatalf("%s no longer contains %q: the plant measures nothing", file, e[0])
			}
			text = strings.Replace(text, e[0], e[1], -1)
		}
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	t.Run("a positional Route literal is refused, not skipped", func(t *testing.T) {
		dir := edit(t, "ownmodel.go", [2]string{"return []httpapi.Route{\n", "return []httpapi.Route{\n\t\t{http.MethodGet, governancePrefix + \"/x\", nil},\n"})
		_, problems := discoverAdminRoutesIn(dir, adminRouteGuardFiles, adminRouteGuardFuncs)
		if got := strings.Join(problems, "\n"); !strings.Contains(got, "ownmodel.go: a Route literal in ownModelRoutes that is positional or lacks a keyed Method, Pattern or Handler") {
			t.Fatalf("problems do not name the positional literal:\n%s", got)
		}
	})
	t.Run("a Route literal without a keyed Pattern is refused", func(t *testing.T) {
		dir := edit(t, "ownmodel.go", [2]string{"return []httpapi.Route{\n", "return []httpapi.Route{\n\t\t{Method: http.MethodGet, Handler: nil},\n"})
		_, problems := discoverAdminRoutesIn(dir, adminRouteGuardFiles, adminRouteGuardFuncs)
		if got := strings.Join(problems, "\n"); !strings.Contains(got, "lacks a keyed Method, Pattern or Handler") {
			t.Fatalf("problems do not name the incomplete literal:\n%s", got)
		}
	})
	t.Run("a file that imports httpapi under an alias is still discovered", func(t *testing.T) {
		const importLine = "\"github.com/full-chaos/dev-health-ops/internal/auth/httpapi\""
		dir := edit(t, "ownmodel.go", [2]string{importLine, "api " + importLine}, [2]string{"httpapi.Route", "api.Route"})
		routes, problems := discoverAdminRoutesIn(dir, adminRouteGuardFiles, adminRouteGuardFuncs)
		if len(problems) != 0 {
			t.Fatalf("an aliased import must still resolve: %v", problems)
		}
		found := 0
		for _, route := range routes {
			if route.file == "ownmodel.go" {
				found++
			}
		}
		if found != 2 {
			t.Fatalf("ownmodel.go contributed %d routes through the alias, want 2", found)
		}
	})
}
