package apiservice

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const adminPrefix = "/api/v1/admin"

// nilConn is a ClickHouse conn that answers every probe with an error, so routes
// that touch it (readiness) fail cleanly instead of dereferencing nil.
type nilConn struct{ driver.Conn }

type failedRow struct{}

func (failedRow) Err() error           { return errNilConn }
func (failedRow) Scan(...any) error    { return errNilConn }
func (failedRow) ScanStruct(any) error { return errNilConn }

var errNilConn = errors.New("no clickhouse in this test")

func (nilConn) QueryRow(context.Context, string, ...any) driver.Row { return failedRow{} }
func (nilConn) Ping(context.Context) error                          { return errNilConn }

// mountedAdminRoutes is the real Routes() set with every optional area mounted
// (guard, a lazy never-connecting pool, a ClickHouse conn), so no package that
// serves the admin prefix is skipped.
func mountedAdminRoutes(t *testing.T) []httpapi.Route {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var out []httpapi.Route
	for _, route := range Routes(Deps{Guard: policy.NewGuard(nil, nil), Pool: pool, ClickHouse: nilConn{}}, quietLogger()) {
		if route.Pattern == adminPrefix || strings.HasPrefix(route.Pattern, adminPrefix+"/") {
			out = append(out, route)
		}
	}
	return out
}

// The next package that serves /api/v1/admin must be added to
// httpapi.AdminRoutes: this walks the real route set and fails on any drift in
// either direction.
func TestEveryMountedAdminPrefixRouteIsInTheNoStoreTable(t *testing.T) {
	mounted := map[string]bool{}
	for _, route := range mountedAdminRoutes(t) {
		mounted[route.Method+" "+route.Pattern] = true
	}
	inTable := map[string]bool{}
	for _, entry := range httpapi.AdminRoutes {
		inTable[entry] = true
	}
	var missing, stale []string
	for entry := range mounted {
		if !inTable[entry] {
			missing = append(missing, entry)
		}
	}
	for entry := range inTable {
		if !mounted[entry] {
			stale = append(stale, entry)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 || len(stale) > 0 {
		t.Fatalf("httpapi.AdminRoutes drifted from the mounted %s routes\nmissing from the table (%d): %v\nin the table, not mounted (%d): %v", adminPrefix, len(missing), missing, len(stale), stale)
	}
	if len(mounted) < 130 {
		t.Fatalf("only %d admin routes mounted; the walk lost a package", len(mounted))
	}
}

// Every mounted admin route, unauthenticated, answers no-store through the real
// server chain, whichever package serves it; non-admin routes do not.
func TestEveryMountedAdminRouteAnswersNoStoreThroughTheServer(t *testing.T) {
	routes := mountedAdminRoutes(t)
	handler := noStoreServer(t, routes)
	for _, route := range routes {
		path := strings.NewReplacer("{", "", "}", "").Replace(route.Pattern)
		var response *httptest.ResponseRecorder
		func() {
			defer func() { _ = recover() }()
			response = serve(handler, httptest.NewRequest(route.Method, path, nil))
		}()
		if response == nil || response.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("%s %s: no Cache-Control: no-store", route.Method, route.Pattern)
		}
	}
}

func TestNoStoreNegativeControlsOutsideTheAdminPrefix(t *testing.T) {
	handler := noStoreServer(t, []httpapi.Route{
		stubRoute(http.MethodGet, "/api/v1/billing/plans", http.StatusOK),
		stubRoute(http.MethodGet, "/api/v1/auth/sso/providers", http.StatusOK),
		stubRoute(http.MethodGet, "/api/v1/administrators", http.StatusOK),
	})
	for _, path := range []string{"/api/v1/billing/plans", "/api/v1/auth/sso/providers", "/api/v1/administrators", "/api/v1/admin/unmounted"} {
		if got := serve(handler, httptest.NewRequest(http.MethodGet, path, nil)).Header().Get("Cache-Control"); got != "" {
			t.Errorf("GET %s: Cache-Control %q, want none", path, got)
		}
	}
}

// A trailing-slash variant of a mounted admin path is answered by the router's
// own 307 redirect to that path; it carries no-store like the route it names.
// A non-admin redirect and a wrong-method request on an exact route do not.
func TestAdminTrailingSlashRedirectsAnswerNoStoreThroughTheServer(t *testing.T) {
	routes := mountedAdminRoutes(t)
	handler := noStoreServer(t, append(routes,
		stubRoute(http.MethodGet, "/api/v1/billing/plans", http.StatusOK)))
	for _, route := range routes {
		path := strings.NewReplacer("{", "", "}", "").Replace(route.Pattern)
		for _, variant := range []string{path + "/", path + "//"} {
			response := serve(handler, httptest.NewRequest(http.MethodGet, variant, nil))
			if response.Code != http.StatusTemporaryRedirect {
				continue
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("GET %s: 307 without no-store", variant)
			}
		}
	}
	probe := serve(handler, httptest.NewRequest(http.MethodGet, "/api/v1/admin/credentials/github/name/", nil))
	if probe.Code != http.StatusTemporaryRedirect || probe.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("credentials slash variant: status %d cache-control %q", probe.Code, probe.Header().Get("Cache-Control"))
	}
	control := serve(handler, httptest.NewRequest(http.MethodGet, "/api/v1/billing/plans/", nil))
	if control.Code != http.StatusTemporaryRedirect || control.Header().Get("Cache-Control") != "" {
		t.Fatalf("billing slash variant: status %d cache-control %q, want 307 with none", control.Code, control.Header().Get("Cache-Control"))
	}
	if got := serve(handler, httptest.NewRequest(http.MethodGet, "/api/v1/admin/unmounted/", nil)).Header().Get("Cache-Control"); got != "" {
		t.Fatalf("unmounted admin path slash variant: Cache-Control %q", got)
	}
}
