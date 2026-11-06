package session

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "AUTH_PROXY_SESSIONS") {
			t.Errorf("%s: status %d body %q, want 503 naming AUTH_PROXY_SESSIONS", pattern, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "access_token") {
			t.Errorf("%s: answered a token: %q", pattern, rec.Body.String())
		}
	}
}
