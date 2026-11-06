package sso

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// With no session issuer (AUTH_PROXY_SESSIONS off) the sign-in routes answer 503 naming AUTH_PROXY_SESSIONS;
// go-api mints no login token pair itself (CHAOS-13155).
func TestSessionsOffAnswers503NamingTheFlagOnEverySignInRoute(t *testing.T) {
	signer, err := edgetoken.NewSigner("sso-off-test-signing-key-0123456789abcdef", "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	locals := map[string]http.Handler{}
	routes := Routes(Deps{
		Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, slog.New(slog.DiscardHandler)),
		StateSecret: "sso-off-test-state-secret-0123456789abcdef", Signer: signer,
		Inner: func(pattern string, local http.Handler) http.Handler {
			locals[pattern] = local
			return local
		},
	})
	if len(routes) == 0 {
		t.Fatal("no routes mounted")
	}
	for _, pattern := range []string{
		"/api/v1/auth/oidc/{provider_id}/authorize", "/api/v1/auth/oidc/{provider_id}/callback",
		"/api/v1/auth/saml/{provider_id}/metadata", "/api/v1/auth/saml/{provider_id}/initiate", "/api/v1/auth/saml/{provider_id}/acs",
	} {
		local := locals[pattern]
		if local == nil {
			t.Errorf("%s: no handler", pattern)
			continue
		}
		rec := httptest.NewRecorder()
		local.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}")))
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "AUTH_PROXY_SESSIONS") {
			t.Errorf("%s: status %d body %q, want 503 naming AUTH_PROXY_SESSIONS", pattern, rec.Code, rec.Body.String())
		}
	}
}
