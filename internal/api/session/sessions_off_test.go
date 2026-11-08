package session

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// With no session issuer (AUTH_PROXY_SESSIONS off) the routes that hand out a token pair answer 503 naming
// AUTH_PROXY_SESSIONS; go-api mints nothing itself (CHAOS-13155).
func TestSessionsOffAnswers503NamingTheFlagOnEveryMintingRoute(t *testing.T) {
	deps := completeDeps(t)
	deps.Issuer = nil
	locals := map[string]http.Handler{}
	deps.Inner = func(pattern string, local http.Handler) http.Handler {
		locals[pattern] = local
		return local
	}
	if routes := Routes(deps); len(routes) != 18 {
		t.Fatalf("%d routes mounted without an issuer, want 18", len(routes))
	}
	for _, pattern := range []string{
		"/api/v1/auth/login", "/api/v1/auth/social-login", "/api/v1/auth/refresh", "/api/v1/auth/switch-org",
		"/api/v1/auth/change-password", "/api/v1/auth/accept-invite",
	} {
		local := locals[pattern]
		if local == nil {
			t.Errorf("%s: no handler", pattern)
			continue
		}
		rec := httptest.NewRecorder()
		local.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, pattern, strings.NewReader("{}")))
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "AUTH_PROXY_SESSIONS") || !strings.Contains(rec.Body.String(), `"reason_code":"config_missing"`) {
			t.Errorf("%s: status %d body %q, want 503 naming AUTH_PROXY_SESSIONS", pattern, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "access_token") {
			t.Errorf("%s: answered a token: %q", pattern, rec.Body.String())
		}
	}
}

// With a session issuer the local handlers run: none answers the sessions-off refusal.
func TestWithASessionIssuerNoRouteAnswersTheSessionsOffRefusal(t *testing.T) {
	deps := completeDeps(t)
	locals := map[string]http.Handler{}
	deps.Inner = func(pattern string, local http.Handler) http.Handler {
		locals[pattern] = local
		return local
	}
	Routes(deps)
	for pattern := range sessionsOffRoutes {
		local := locals[pattern]
		if local == nil {
			t.Errorf("%s: no handler", pattern)
			continue
		}
		rec := httptest.NewRecorder()
		func() {
			defer func() { _ = recover() }()
			local.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, pattern, strings.NewReader("{}")))
		}()
		if strings.Contains(rec.Body.String(), "AUTH_PROXY_SESSIONS") {
			t.Errorf("%s: answered the sessions-off refusal with an issuer set: %q", pattern, rec.Body.String())
		}
	}
}

// The go-api plane of the retired HS256 refresh: with AUTH_PROXY_SESSIONS off, go-api's own /refresh answers a
// valid go-api refresh JWT (signed with go-api's key) with the sessions-off 503 through the full route (guard,
// body validation, limiter); it verifies nothing and mints nothing.
func TestSessionsOffRefreshAnswersAValidGoAPIRefreshJWTWith503(t *testing.T) {
	deps := completeDeps(t)
	deps.Issuer = nil
	signer, err := edgetoken.NewSigner("wiring-test-signing-key-0123456789abcdef", "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.Refresh(edgetoken.RefreshClaims{UserID: uuid.NewString(), OrgID: uuid.NewString(), FamilyID: uuid.NewString()},
		time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deps.Verifier.VerifyRefresh(token); err != nil {
		t.Fatalf("the planted token is not a valid go-api refresh JWT: %v", err)
	}
	var refresh http.Handler
	for _, route := range Routes(deps) {
		if route.Method == http.MethodPost && route.Pattern == "/api/v1/auth/refresh" {
			refresh = route.Handler
		}
	}
	if refresh == nil {
		t.Fatal("go-api mounts no /api/v1/auth/refresh")
	}
	rec := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", strings.NewReader(`{"refresh_token":"`+token+`"}`))
	request.Header.Set("Content-Type", "application/json")
	refresh.ServeHTTP(rec, request)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"reason_code":"config_missing"`) ||
		!strings.Contains(rec.Body.String(), "AUTH_PROXY_SESSIONS") {
		t.Fatalf("go-api /refresh with sessions off: %d %s, want 503 config_missing naming AUTH_PROXY_SESSIONS", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), "access_token") || strings.Contains(rec.Body.String(), "refresh_token") {
		t.Fatalf("go-api /refresh with sessions off answered a token: %s", rec.Body)
	}
}
