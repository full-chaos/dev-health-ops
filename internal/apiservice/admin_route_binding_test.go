package apiservice

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// The admin no-store route table (httpapi.AdminRoutes) is bound to the mux the api really serves: through NewServer and the
// full route table of Routes, the mux routes every table entry to that entry (it answers no-store), every route mounted
// under /api/v1/admin (any package) answers no-store, and no other route does.

var routeParam = regexp.MustCompile(`\{[^}]*\}`)

// concrete is a request target the pattern routes: every {param} filled.
func concrete(pattern string) string { return routeParam.ReplaceAllString(pattern, "x") }

func adminBindingViolations(t *testing.T, table []string, extra ...httpapi.Route) []string {
	t.Helper()
	saved := httpapi.AdminRoutes
	httpapi.AdminRoutes = table
	defer func() { httpapi.AdminRoutes = saved }()

	// never dialled: the admin guard refuses an unauthenticated request (401) before any query
	pool, err := pgxpool.New(context.Background(), "postgres://binding@127.0.0.1:1/binding?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	routes := append(Routes(Deps{Pool: pool, Guard: policy.NewGuard(nil, quietLogger()), ClickHouse: nilConn{}}, quietLogger()), extra...)
	server, err := NewServer(apiTestConfig(), quietLogger(), routes)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	ask := func(method, pattern string) *httptest.ResponseRecorder {
		var response *httptest.ResponseRecorder
		func() {
			defer func() { _ = recover() }()
			response = serve(handler, httptest.NewRequest(method, concrete(pattern), nil))
		}()
		return response
	}

	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	inTable := map[string]bool{}
	for _, entry := range table {
		inTable[entry] = true
		method, pattern, _ := strings.Cut(entry, " ")
		response := ask(method, pattern)
		// no-store is set only when the real mux routes the request to a table pattern; a target with "x" in every
		// wildcard can only be routed to its own pattern (no pattern has a literal "x" segment), so no-store here means
		// the mux serves this very entry. The status is the route's own (POST ip-allowlist/{entry_id} answers 405 for
		// any id but "check", by design) and is not read.
		switch {
		case response == nil:
			add("table entry %q: the request panicked through the server", entry)
		case response.Header().Get("Cache-Control") != "no-store":
			add("table entry %q: the real mux does not route it to a table pattern (status %d, no Cache-Control: no-store)", entry, response.Code)
		}
	}
	adminRoute := map[string]bool{}
	for _, route := range routes[:len(routes)-len(extra)] {
		if route.Pattern == adminPrefix || strings.HasPrefix(route.Pattern, adminPrefix+"/") {
			adminRoute[route.Method+" "+route.Pattern] = true
		}
	}
	mounted, noStore := 0, 0
	for index, route := range routes {
		key := route.Method + " " + route.Pattern
		response := ask(route.Method, route.Pattern)
		if index >= len(routes)-len(extra) && strings.HasPrefix(route.Pattern, adminPrefix+"/") {
			// a deliberate unlisted admin-prefixed sibling: the mux must route its requests to it, not to a table wildcard
			if got := response != nil && response.Header().Get("Cache-Control") == "no-store"; got {
				add("extra route %q answers Cache-Control: no-store; the table wildcard took it", key)
			}
			continue
		}
		got := response != nil && response.Header().Get("Cache-Control") == "no-store"
		if got {
			noStore++
		}
		if adminRoute[key] {
			mounted++
			if !got {
				add("admin route %q: answers without Cache-Control: no-store (not in the table the mux reads)", key)
			}
		} else if got {
			add("route %q outside %s answers Cache-Control: no-store", key, adminPrefix)
		}
	}
	if mounted != len(adminRoute) {
		add("%d of the %d admin-prefixed routes are mounted by Routes", mounted, len(adminRoute))
	}
	if noStore != len(adminRoute) {
		add("%d routes answer no-store, %d are mounted under %s", noStore, len(adminRoute), adminPrefix)
	}
	return out
}

func TestAdminNoStoreTableIsBoundToTheRealMuxInBothDirections(t *testing.T) {
	if got := adminBindingViolations(t, httpapi.AdminRoutes); len(got) != 0 {
		t.Fatalf("the admin no-store table and the real mux disagree:\n%s", strings.Join(got, "\n"))
	}
}

// The decision is the serving mux's, not a mux of the table alone: a route of another package more specific than a table
// wildcard (a literal sibling of GET /api/v1/admin/settings/{category}) takes its requests in the real mux, so it answers
// without no-store, while a mux of just the table would route it to the wildcard.
func TestAdminNoStoreFollowsTheServingMuxPastAMoreSpecificSibling(t *testing.T) {
	sibling := stubRoute(http.MethodGet, "/api/v1/admin/settings/zz-literal-sibling", http.StatusOK)
	if got := adminBindingViolations(t, httpapi.AdminRoutes, sibling); len(got) != 0 {
		t.Fatalf("a more specific sibling of a table wildcard:\n%s", strings.Join(got, "\n"))
	}
}

func TestAdminNoStoreBindingPlantedTablesAreCaught(t *testing.T) {
	real := append([]string(nil), httpapi.AdminRoutes...)
	dropped := ""
	var withoutOne []string
	for _, entry := range real {
		if dropped == "" && entry == "GET "+modelsPath {
			dropped = entry
			continue
		}
		withoutOne = append(withoutOne, entry)
	}
	if dropped == "" {
		t.Fatalf("GET %s is not in the admin table", modelsPath)
	}
	for _, c := range []struct {
		name  string
		table []string
		extra []httpapi.Route
		want  string
	}{
		{"an entry the mux does not serve", append(append([]string(nil), real...), "GET /api/v1/admin/zz-no-such-route"), nil, `"GET /api/v1/admin/zz-no-such-route": the real mux does not route it to a table pattern`},
		{"an admin route missing from the table", withoutOne, nil, `admin route "` + dropped + `": answers without Cache-Control: no-store`},
		{"a route outside the admin prefix in the table", append(append([]string(nil), real...), "GET /api/v1/zz-outside"), []httpapi.Route{stubRoute(http.MethodGet, "/api/v1/zz-outside", http.StatusOK)}, `route "GET /api/v1/zz-outside" outside /api/v1/admin answers Cache-Control: no-store`},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := adminBindingViolations(t, c.table, c.extra...)
			if !strings.Contains(strings.Join(got, "\n"), c.want) {
				t.Fatalf("planted table not caught as %q; got:\n%s", c.want, strings.Join(got, "\n"))
			}
		})
	}
}

// HEAD on an admin GET route answers 405 (FastAPI routes no HEAD there; frozen goldens "HEAD schemas" and "me: HEAD" are
// 405) with an Allow that lists every method the path serves (RFC 9110 10.2.1; Starlette named only the first route's),
// and keeps Cache-Control: no-store: the real mux matches HEAD to the GET pattern, so it falls under the declared admin
// no-store divergence. The walk covers the full mounted set (every admin-prefixed package, ClickHouse stub included).
func TestHeadOnAdminGetRoutesAnswers405WithNoStore(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://binding@127.0.0.1:1/binding?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	routes := Routes(Deps{Pool: pool, Guard: policy.NewGuard(nil, quietLogger()), ClickHouse: nilConn{}}, quietLogger())
	server, err := NewServer(apiTestConfig(), quietLogger(), routes)
	if err != nil {
		t.Fatal(err)
	}
	methods := map[string][]string{}
	for _, route := range routes {
		if route.Pattern == adminPrefix || strings.HasPrefix(route.Pattern, adminPrefix+"/") {
			methods[route.Pattern] = append(methods[route.Pattern], route.Method)
		}
	}
	checked, tableGets := 0, 0
	for _, entry := range httpapi.AdminRoutes {
		method, pattern, _ := strings.Cut(entry, " ")
		if method != http.MethodGet {
			continue
		}
		tableGets++
		set := append([]string(nil), methods[pattern]...)
		if len(set) == 0 {
			t.Errorf("GET %s is in the admin table but not mounted", pattern)
			continue
		}
		sort.Strings(set)
		want := strings.Join(set, ", ")
		checked++
		response := serve(server.Handler(), httptest.NewRequest(http.MethodHead, concrete(pattern), nil))
		if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != want || response.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("HEAD %s: status %d Allow %q Cache-Control %q; want 405, Allow %q, no-store", pattern, response.Code, response.Header().Get("Allow"), response.Header().Get("Cache-Control"), want)
		}
	}
	if checked != tableGets || checked < headCheckedFloor {
		t.Fatalf("%d admin GET routes checked of %d in the table, want all and at least %d", checked, tableGets, headCheckedFloor)
	}
}

// headCheckedFloor keeps the walk from passing on a table that shrank; the table grows with main, so the count is not pinned.
const headCheckedFloor = 60

// The all-methods Allow is the HEAD refusal's only: the 405 of any other method keeps the path's declared Allow, the
// first FastAPI route's (frozen goldens pin it).
func TestAdminDeclaredAllowIsKeptForNonHeadMethods(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://binding@127.0.0.1:1/binding?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	server, err := NewServer(apiTestConfig(), quietLogger(), Routes(Deps{Pool: pool, Guard: policy.NewGuard(nil, quietLogger()), ClickHouse: nilConn{}}, quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	for target, c := range map[string]struct{ method, allow string }{
		"/api/v1/admin/credentials":                {http.MethodPut, "GET"},
		"/api/v1/admin/customer-push/sources":      {http.MethodPut, "POST"},
		"/api/v1/admin/settings/general/site_name": {http.MethodPost, "GET"},
		"/api/v1/admin/teams/eng":                  {http.MethodPut, "DELETE"},
	} {
		want := c.allow
		response := serve(server.Handler(), httptest.NewRequest(c.method, target, nil))
		if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != want {
			t.Errorf("%s: status %d Allow %q; want 405, Allow %q", c.method+" "+target, response.Code, response.Header().Get("Allow"), want)
		}
	}
}
