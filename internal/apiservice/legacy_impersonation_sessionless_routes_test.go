package apiservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

type sessionlessAnswer struct {
	status      int
	body        string
	stamped     bool
	sessionRead int
	forwarded   int
}

// TestSessionlessRoutesAnswerTheLegacyImpersonationCaller serves the eight
// routes that never read the session (switch-org is refused in go-api, see
// TestSwitchOrgRefusesTheImpersonatingAdminBearer), with AUTH_PROXY_IMPERSONATION on and the
// legacy caller (the admin's own bearer beside a live go-api session row),
// and names each answer.
func TestSessionlessRoutesAnswerTheLegacyImpersonationCaller(t *testing.T) {
	verifier, err := edgetoken.New(strings.Repeat("fixture-key-", 3), "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	tokens := edtokens.New(t)
	store := &legacySessionStore{admin: uuid.New(), target: uuid.New(), tOrg: uuid.New()}
	auth := tokens.AuthenticatorOver(t, store, quietLogger())
	rows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte(ownerUpstreamBody))
	})
	upstream, _ := url.Parse(stub.URL)
	proxy := AuthProxyConfig{Upstream: upstream, Impersonation: true, On: map[AuthAggregate]bool{AuthSessions: true}}
	protected, err := newProtection(apiTestConfig(), nil, false, proxy, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	scope.DeclaredRows(rows)
	scope.RefuseLegacyImpersonation(protected.scope.RefusesLegacyImpersonation())
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	routes := Routes(Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()), Verifier: verifier,
		AuthProxy: proxy, Limits: httpapi.NewMemoryCounters(time.Now)}, quietLogger())
	server, err := newServer(apiTestConfig(), quietLogger(), routes, nil, proxy.ScopeExemptRoutes(), auth.ReadOnce, scope.Principal, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	access := tokens.MustMint(t, edtokens.Claims{UserID: store.admin, OrgID: uuid.NewString(), TokenVersion: 1})

	serve := func(method, pattern, bearer string, extra map[string]string) sessionlessAnswer {
		store.mu.Lock()
		store.sessionReads = 0
		store.mu.Unlock()
		before := len(stub.requests())
		path := routeParam.ReplaceAllStringFunc(pattern, func(string) string { return uuid.NewString() })
		request := httptest.NewRequest(method, path, strings.NewReader("{}"))
		if bearer != "" {
			request.Header.Set("Authorization", "Bearer "+bearer)
		}
		request.Header.Set("Content-Type", "application/json")
		for name, value := range extra {
			request.Header.Set(name, value)
		}
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		store.mu.Lock()
		reads := store.sessionReads
		store.mu.Unlock()
		return sessionlessAnswer{status: recorder.Code, body: recorder.Body.String(), stamped: recorder.Header().Get("X-Impersonating") != "",
			sessionRead: reads, forwarded: len(stub.requests()) - before}
	}

	// The control: the row is live and the switch is on, so the scope reads
	// it, admits status in its session and refuses the legacy caller on start.
	if got := serve(http.MethodGet, "/api/v1/admin/impersonate/status", access, nil); got.status != http.StatusOK || !got.stamped || got.sessionRead != 1 {
		t.Fatalf("control status: %+v, want 200 stamped after one session read", got)
	}
	if got := serve(http.MethodPost, "/api/v1/admin/impersonate", access, nil); got.status != http.StatusForbidden || got.stamped || got.sessionRead != 1 {
		t.Fatalf("control start: %+v, want the 403 refusal after one session read", got)
	}

	githubHeaders := map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": uuid.NewString()}
	cells := []struct {
		profile string
		extra   map[string]string
		status  int
		who     string
	}{
		{"GET /api/v1/external-ingest/availability", nil, http.StatusUnauthorized, "nobody: 401 invalid ingest token"},
		{"POST /api/v1/external-ingest/validate", nil, http.StatusUnauthorized, "nobody: 401 invalid ingest token"},
		{"POST /api/v1/external-ingest/batches", nil, http.StatusUnauthorized, "nobody: 401 invalid ingest token"},
		{"GET /api/v1/external-ingest/batches", nil, http.StatusUnauthorized, "nobody: 401 invalid ingest token"},
		{"GET /api/v1/external-ingest/batches/{ingestion_id}", nil, http.StatusUnauthorized, "nobody: 401 invalid ingest token"},
		{"POST /api/v1/billing/webhooks/stripe", nil, http.StatusBadRequest, "nobody: 400 invalid Stripe signature"},
		{"POST /api/v1/webhooks/github", githubHeaders, http.StatusUnauthorized, "nobody: 401 invalid signature"},
		{"POST /api/v1/webhooks/pagerduty/{binding_id}", nil, http.StatusInternalServerError, "nobody: binding lookup (no store here)"},
	}
	for _, cell := range cells {
		t.Run(cell.profile, func(t *testing.T) {
			method, pattern, _ := strings.Cut(cell.profile, " ")
			withBearer := serve(method, pattern, access, cell.extra)
			t.Logf("%s => %+v (%s)", cell.profile, withBearer, cell.who)
			without := serve(method, pattern, "", cell.extra)
			if cell.status != 0 && withBearer.status != cell.status {
				t.Errorf("status %d, want %d", withBearer.status, cell.status)
			}
			if withBearer.stamped || withBearer.sessionRead != 0 {
				t.Errorf("the scope applied the session here: %+v", withBearer)
			}
			if withBearer.status != without.status || withBearer.body != without.body {
				t.Errorf("the admin bearer changed the answer: with %d %s, without %d %s", withBearer.status, withBearer.body, without.status, without.body)
			}
			if withBearer.forwarded != 0 {
				t.Errorf("forwarded to auth-service %d times, want 0", withBearer.forwarded)
			}
		})
	}

	if seen := stub.requests(); len(seen) != 0 {
		t.Fatalf("auth-service saw %d requests from the eight routes, want 0", len(seen))
	}
}
