package authflowvenue_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/session"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
)

// movedRuling retires, on the in-process run only, the steps of the go-api credential routes whose local body is gone
// and the comparisons that depend on them: the external legs run the same steps forwarded to auth-service.
const movedRuling = "acp 2026-11-09T00:07Z Q9 (a2), CHAOS-13362: go-api serves no local credential body; the external legs run these steps forwarded to auth-service"

// movedRoutes is every go-api session route ("METHOD pattern") whose local handler answers the moved-operation
// refusal with the users-owned switch off, read from session.Routes itself: each route's local handler is called with
// no store, so a handler that would read or write answers something else (or panics) and is not moved.
func movedRoutes(t *testing.T) map[string]bool {
	t.Helper()
	t.Setenv(usersowned.Env, "false")
	auth := &policy.Authenticator{}
	locals := map[string]http.Handler{}
	routes := session.Routes(session.Deps{Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(auth, slog.New(slog.DiscardHandler)), Auth: auth,
		Inner: func(pattern string, local http.Handler) http.Handler {
			locals[pattern] = local
			return local
		}})
	if len(routes) == 0 {
		t.Fatal("session.Routes mounted no route")
	}
	want := httptest.NewRecorder()
	usersowned.Write(want)
	moved := map[string]bool{}
	for _, route := range routes {
		local, ok := locals[route.Pattern]
		if !ok {
			continue
		}
		if answersMoved(local, route.Method, route.Pattern, want) {
			moved[route.Method+" "+route.Pattern] = true
		}
	}
	return moved
}

func answersMoved(local http.Handler, method, pattern string, want *httptest.ResponseRecorder) (moved bool) {
	defer func() {
		if recover() != nil {
			moved = false
		}
	}()
	got := httptest.NewRecorder()
	local.ServeHTTP(got, httptest.NewRequest(method, pattern, nil))
	return got.Code == want.Code && got.Body.String() == want.Body.String()
}

// movedProbes is the one executed in-process cell of each moved route: a request go-api's own validator and limiter
// accept, so the answer is the route's local body.
var movedProbes = map[string]string{
	"POST /api/v1/auth/register":            `{"email": "moved.probe@example.com", "password": "Moved-probe-2026"}`,
	"GET /api/v1/auth/verify":               `?token=moved-probe`,
	"POST /api/v1/auth/resend-verification": `{"email": "moved.probe@example.com"}`,
	"POST /api/v1/auth/forgot-password":     `{"email": "moved.probe@example.com"}`,
	"POST /api/v1/auth/reset-password":      `{"token": "moved-probe", "new_password": "Moved-probe-2026"}`,
}

// dependsOnMoved are the in-process steps that are on no moved route but read what a moved route's step before them
// in the same batch did (a verified address, a reset password, a refresh token the reset revoked). Each names that
// route.
var dependsOnMoved = map[string]string{
	"state: uma now verified":                      "GET /api/v1/auth/verify",
	"login: alice, new password":                   "POST /api/v1/auth/reset-password",
	"login: alice, old password":                   "POST /api/v1/auth/reset-password",
	"refresh: alice's token from before the reset": "POST /api/v1/auth/reset-password",
	"login: pia, new password":                     "POST /api/v1/auth/reset-password",
}

// inProcessRetiredRows are the row comparisons a moved route's step changes on the Python plane (sign-up users,
// organizations, memberships and audit rows, the link tokens): the in-process run retires them under movedRuling. The
// forwarded legs still compare every one of them. org_invites and refresh_tokens stay compared in process.
var inProcessRetiredRows = []string{"users", "organizations", "memberships", "email_verification_tokens", "password_reset_tokens", "audit_logs"}

// inProcessRefreshExcluded are the users whose refresh_tokens rows a retired step wrote on the Python plane (a reset
// revoked one, a sign-in with the reset password made one): the in-process run compares refresh_tokens without their
// rows, and every other user's rows (the kept sign-in, invite and onboarding steps) as they are.
var inProcessRefreshExcluded = []string{"alice@example.com", "pia@example.com"}

// refreshWritingDependents are the users of the dependent steps that sign in or refresh, the steps that write
// refresh_tokens: "login: <user>, ..." and "refresh: <user>'s ...".
func refreshWritingDependents() []string {
	seen := map[string]bool{}
	for name := range dependsOnMoved {
		for _, prefix := range []string{"login: ", "refresh: "} {
			rest, ok := strings.CutPrefix(name, prefix)
			if !ok {
				continue
			}
			user, _, _ := strings.Cut(rest, ",")
			user, _, _ = strings.Cut(user, "'")
			seen[strings.TrimSpace(user)+"@example.com"] = true
		}
	}
	users := make([]string, 0, len(seen))
	for user := range seen {
		users = append(users, user)
	}
	sort.Strings(users)
	return users
}

// withoutUsers is rows (" | " separated) without the lines of users.
func withoutUsers(rows string, users []string) string {
	var kept []string
	for _, line := range strings.Split(rows, " | ") {
		excluded := false
		for _, user := range users {
			if strings.HasPrefix(line, user) {
				excluded = true
			}
		}
		if !excluded && line != "" {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, " | ")
}

// The in-process refresh_tokens exclusion is exactly the users of the dependent sign-in and refresh steps: one user more
// or one user less fails here.
func TestInProcessRefreshExclusionIsTheDependentSignInUsers(t *testing.T) {
	got := append([]string(nil), inProcessRefreshExcluded...)
	sort.Strings(got)
	if want := refreshWritingDependents(); !slices.Equal(got, want) {
		t.Errorf("inProcessRefreshExcluded = %v, want the dependent sign-in users %v", got, want)
	}
	if slices.Contains(inProcessRetiredRows, "refresh_tokens") {
		t.Error("refresh_tokens is retired whole, but kept in-process steps write it")
	}
	rows := "alice@example.comorg-bfalse 1 | ivan@example.comorg-afalse 1 | pia@example.com<null>false 1"
	if kept := withoutUsers(rows, inProcessRefreshExcluded); kept != "ivan@example.comorg-afalse 1" {
		t.Errorf("withoutUsers kept %q", kept)
	}
}

// movedStepRoute is the moved route a step is on or depends on, and whether the in-process run retires it.
func movedStepRoute(moved map[string]bool, name, method, path string) (string, bool) {
	route, _, _ := strings.Cut(path, "?")
	if key := method + " " + route; moved[key] {
		return key, true
	}
	if key, ok := dependsOnMoved[name]; ok && moved[key] {
		return key, true
	}
	return "", false
}

// The moved set is read from go-api's own route table, each moved route is an auth-service credential route (the
// forward serves it), and each has exactly one executed in-process cell. A new route whose local body answers the
// moved refusal fails here until it has its cell.
func TestInProcessMovedRoutesAreDerivedFromGoAPIsRouteTable(t *testing.T) {
	moved := movedRoutes(t)
	if len(moved) == 0 {
		t.Fatal("no go-api session route answers the moved refusal")
	}
	names := make([]string, 0, len(moved))
	for key := range moved {
		names = append(names, key)
	}
	sort.Strings(names)
	t.Logf("moved routes: %s", strings.Join(names, ", "))
	served := []string{authapi.PatternRegister, authapi.PatternVerifyEmail, authapi.PatternResendVerification,
		authapi.PatternForgotPassword, authapi.PatternResetPassword, authapi.PatternChangePassword}
	for key := range moved {
		_, pattern, _ := strings.Cut(key, " ")
		if !slices.Contains(served, pattern) {
			t.Errorf("%s answers the moved refusal, but auth-service serves no credential route on it", key)
		}
		if _, ok := movedProbes[key]; !ok {
			t.Errorf("%s answers the moved refusal and has no executed in-process cell (movedProbes)", key)
		}
	}
	for key := range movedProbes {
		if !moved[key] {
			t.Errorf("movedProbes names %s, which does not answer the moved refusal", key)
		}
	}
}

// Each dependent step is a step of the oracle, in a batch that has a step on the moved route it names before it.
func TestInProcessDependentStepsFollowAStepOnTheirMovedRoute(t *testing.T) {
	moved := movedRoutes(t)
	batches := oracleBatches(t)
	found := map[string]bool{}
	for _, batch := range batches {
		seen := map[string]bool{}
		for _, s := range batch {
			if route, ok := dependsOnMoved[s.name]; ok {
				found[s.name] = true
				if !moved[route] {
					t.Errorf("dependent step %q names %s, which is not a moved route", s.name, route)
				} else if !seen[route] {
					t.Errorf("dependent step %q names %s, but no step on it runs before it in its batch", s.name, route)
				}
			}
			seen[s.method+" "+s.route] = true
		}
	}
	var missing []string
	for name := range dependsOnMoved {
		if !found[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("dependsOnMoved names steps the oracle does not have: %v", missing)
	}
}

type oracleStep struct{ name, method, route string }

// oracleBatches reads the literal step batches of the oracle source: each r.batch("label", []step{...}) call, the
// name, method and route (query cut) of each same, split, visit or withHeaders(same(...)) step in it.
func oracleBatches(t *testing.T) [][]oracleStep {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "auth_flow_venue_oracle_integration_test.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]string{}
	methods := map[string]string{"post": http.MethodPost, "get": http.MethodGet}
	ast.Inspect(file, func(n ast.Node) bool {
		if spec, ok := n.(*ast.ValueSpec); ok && len(spec.Names) == len(spec.Values) {
			for i, name := range spec.Names {
				if lit, ok := spec.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if value, err := strconv.Unquote(lit.Value); err == nil && strings.HasPrefix(value, "/api/") {
						routes[name.Name] = value
					}
				}
			}
		}
		return true
	})
	routeOf := func(expr ast.Expr) string {
		for {
			switch e := expr.(type) {
			case *ast.Ident:
				return routes[e.Name]
			case *ast.BinaryExpr:
				expr = e.X
			default:
				return ""
			}
		}
	}
	var stepOf func(expr ast.Expr) (oracleStep, bool)
	stepOf = func(expr ast.Expr) (oracleStep, bool) {
		call, ok := expr.(*ast.CallExpr)
		if !ok {
			return oracleStep{}, false
		}
		fun, ok := call.Fun.(*ast.Ident)
		if !ok || len(call.Args) < 2 {
			return oracleStep{}, false
		}
		if fun.Name == "withHeaders" {
			return stepOf(call.Args[0])
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return oracleStep{}, false
		}
		name, err := strconv.Unquote(lit.Value)
		if err != nil {
			return oracleStep{}, false
		}
		switch fun.Name {
		case "same", "split":
			if len(call.Args) < 3 {
				return oracleStep{}, false
			}
			method, _ := call.Args[1].(*ast.Ident)
			if method == nil {
				return oracleStep{}, false
			}
			return oracleStep{name: name, method: methods[method.Name], route: routeOf(call.Args[2])}, true
		case "visit":
			return oracleStep{name: name, method: http.MethodGet, route: routeOf(call.Args[1])}, true
		}
		return oracleStep{}, false
	}
	var batches [][]oracleStep
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "batch" || len(call.Args) != 2 {
			return true
		}
		list, ok := call.Args[1].(*ast.CompositeLit)
		if !ok {
			return true
		}
		var batch []oracleStep
		for _, element := range list.Elts {
			if s, ok := stepOf(element); ok {
				batch = append(batch, s)
			}
		}
		batches = append(batches, batch)
		return true
	})
	if len(batches) == 0 {
		t.Fatal("the oracle source holds no literal step batch")
	}
	return batches
}
