package teamsidentity

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// TestRoutesRegisterWithoutPanicking is the test that actually would have
// caught the real defect this file's stub route introduced: httpapi's
// server (internal/auth/httpapi/server.go) auto-registers an explicit HEAD
// handler for every GET route (ExplicitHead: true, the SAME option
// apiservice's real wiring passes), including the {team_id} wildcard --
// and Go's http.ServeMux PANICS at construction time when a literal
// pattern registers only GET while a wildcard at the same path also
// carries an (auto-registered) HEAD, because neither method set is a
// superset of the other at the overlap. TestTeamsDiscoverStaticRouteWins
// below did not catch this (it never registers HEAD), which is exactly
// why this test goes through the REAL construction path -- Routes() into
// httpapi.NewServer, the same call apiservice makes -- rather than a
// hand-built mux.
func TestRoutesRegisterWithoutPanicking(t *testing.T) {
	guard := policy.NewGuard(nil, nil)
	_, err := httpapi.NewServer(httpapi.ServerOptions{
		Address:        "127.0.0.1:0",
		Routes:         Routes(nil, guard, nil),
		RequestTimeout: time.Second,
		MaxBodyBytes:   1024,
		ExplicitHead:   true,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
}

// TestTeamsDiscoverCapturedByWildcardOnRealRoutesToday pins the ACTUAL,
// CURRENT behavior of Routes()' real route table (built through the same
// httpapi.NewServer call apiservice makes, not a hand-built mux): with no
// literal /teams/discover registered (see the route table's comment on
// why one cannot be, safely, in this framework today), an anonymous
// GET /api/v1/admin/teams/discover is captured by the {team_id} wildcard's
// AdminOrg-guarded getTeam handler -- proven here by its 401 (the guard
// runs and refuses; a request that fell through to the mux's own
// not-found handler would be 404 instead). This is production-SAFE only
// because ingress routes GET /teams/discover to the Python api by exact
// path; the Go binary's own route table never receives live traffic for
// it today. This test exists to FAIL the moment that stops being true --
// a new pattern registered here that changes which handler (or status)
// answers this exact path is a deliberate routing change that must
// re-justify itself against the ingress assumption above, not a silent
// drift.
func TestTeamsDiscoverCapturedByWildcardOnRealRoutesToday(t *testing.T) {
	guard := policy.NewGuard(nil, nil)
	server, err := httpapi.NewServer(httpapi.ServerOptions{
		Address:        "127.0.0.1:0",
		Routes:         Routes(nil, guard, nil),
		RequestTimeout: time.Second,
		MaxBodyBytes:   1024,
		ExplicitHead:   true,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/discover", nil)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /teams/discover on the real route table: got %d, want %d (captured by the "+
			"{team_id} wildcard's AdminOrg guard) -- routing for this path changed, re-check the "+
			"ingress assumption in this test's doc comment before updating the expectation",
			rec.Code, http.StatusUnauthorized)
	}
}

// TestTeamsDiscoverStaticRouteWins proves the precedence Routes()'
// registration comment relies on (CHAOS-6310 r1 finding #7): Go's
// http.ServeMux resolves a literal path segment ahead of a wildcard at the
// same position, regardless of which was registered first -- confirmed
// here with the SAME two pattern strings Routes() actually registers, so a
// request to /api/v1/admin/teams/discover reaches the literal handler, not
// the {team_id} wildcard treating "discover" as a team lookup. Registered
// in wildcard-then-literal order specifically, the harder direction to get
// right by accident. This test alone is NOT sufficient proof the real
// route table is safe (see TestRoutesRegisterWithoutPanicking above for
// why) -- it isolates the precedence RULE, the construction test proves
// the actual table.
func TestTeamsDiscoverStaticRouteWins(t *testing.T) {
	mux := http.NewServeMux()
	var hitWildcard, hitLiteral bool
	mux.HandleFunc("GET /api/v1/admin/teams/{team_id}", func(w http.ResponseWriter, r *http.Request) {
		hitWildcard = true
	})
	mux.HandleFunc("GET /api/v1/admin/teams/discover", func(w http.ResponseWriter, r *http.Request) {
		hitLiteral = true
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/discover", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if hitWildcard {
		t.Error("the {team_id} wildcard handled /teams/discover -- it must not")
	}
	if !hitLiteral {
		t.Error("the literal /teams/discover handler was never reached")
	}

	// A genuine team_id still reaches the wildcard.
	hitWildcard, hitLiteral = false, false
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/eng", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if !hitWildcard || hitLiteral {
		t.Errorf("a real team_id must reach the wildcard: wildcard=%v literal=%v", hitWildcard, hitLiteral)
	}
}
