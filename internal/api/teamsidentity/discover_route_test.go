package teamsidentity

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
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
		Routes:         Routes(nil, guard, nil, nil, nil),
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
		Routes:         Routes(nil, guard, nil, nil, nil),
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

// TestGetTeamInterceptsDiscoverMissingProvider is CHAOS-6310 r2 finding #4's
// direct reproduction and fix: an AUTHENTICATED (guard already satisfied --
// this calls the unwrapped handler directly, the same way an admin request
// reaches it after guard.Wrap passes) GET .../teams/discover with no
// `provider` query parameter must answer Python's own 422 "Field required"
// for that missing query parameter, not fall through to a team lookup on
// the literal string "discover" (which answered Go's 404 "Team not found"
// before this fix, live-proved as a P1 against the real venue). A request
// that DOES supply `provider` still falls through to the pre-existing
// "team not found" 404 -- discover's real behavior once a provider is
// present is CHAOS-6311's to implement; this fixes only the one case r2
// proved live.
func TestGetTeamInterceptsDiscoverMissingProvider(t *testing.T) {
	h := handlers{store: Store{}, logger: slog.Default()}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/discover", nil)
	req.SetPathValue("team_id", "discover")
	rec := httptest.NewRecorder()
	h.getTeam(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("GET .../teams/discover with no provider: got %d, want 422\nbody: %s", rec.Code, rec.Body.String())
	}
	want := `{"detail":[{"type":"missing","loc":["query","provider"],"msg":"Field required","input":null}]}`
	if rec.Body.String() != want {
		t.Errorf("body = %s, want %s", rec.Body.String(), want)
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

// TestDiscoverTeamsValidatesProviderPattern proves the FastAPI Query(...,
// pattern=...) 422 for a provider outside github|gitlab|jira|linear, and
// that a real one still passes validation (checked by observing it does
// NOT hit the pattern-mismatch branch -- credential resolution then fails
// with a nil Pool, a distinct, later error this test also pins).
func TestDiscoverTeamsValidatesProviderPattern(t *testing.T) {
	h := handlers{store: Store{}, logger: slog.Default()}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/discover?provider=bitbucket", nil)
	req.SetPathValue("team_id", "discover")
	rec := httptest.NewRecorder()
	h.getTeam(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("provider=bitbucket: got %d, want 422\nbody: %s", rec.Code, rec.Body.String())
	}
	want := `{"detail":[{"type":"string_pattern_mismatch","loc":["query","provider"],"msg":"String should match pattern '^(github|gitlab|jira|linear)$'","input":"bitbucket","ctx":{"pattern":"^(github|gitlab|jira|linear)$"}}]}`
	if rec.Body.String() != want {
		t.Errorf("body = %s, want %s", rec.Body.String(), want)
	}
}

// TestDiscoverTeamsNoCredentialConfigured proves the 404 "No credentials
// found for provider" body FastAPI's route answers when
// resolve_with_fallback finds nothing -- reachable here with a nil Pool
// (PostgresCredentialRepository.ResolveEncrypted's own "no pool" branch
// answers the same ErrCredentialNotFound a real empty-result query would).
// noopDecryptor is never actually invoked in this test: with a nil Pool,
// PostgresCredentialRepository.ResolveEncrypted returns ErrCredentialNotFound
// before CredentialResolver.Resolve ever reaches a decrypt call -- but
// Resolve's OWN nil-Decryptor guard fires first if this is left nil, which
// would mask that behavior behind a different, unrelated error.
type noopDecryptor struct{}

func (noopDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return nil, providerfoundation.ErrCredentialInvalid
}

func TestDiscoverTeamsNoCredentialConfigured(t *testing.T) {
	h := handlers{store: Store{}, credentials: discoverCredentials{Decryptor: noopDecryptor{}}, logger: slog.Default()}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/discover?provider=jira", nil)
	req.SetPathValue("team_id", "discover")
	req = req.WithContext(policy.WithUser(req.Context(), &policy.User{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	h.getTeam(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("no credential configured: got %d, want 404\nbody: %s", rec.Code, rec.Body.String())
	}
	want := `{"detail":"No credentials found for provider 'jira'"}`
	if rec.Body.String() != want {
		t.Errorf("body = %s, want %s", rec.Body.String(), want)
	}
}
