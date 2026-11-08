package sso

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// With no session issuer (AUTH_PROXY_SESSIONS off) the sign-in routes answer 503 naming AUTH_PROXY_SESSIONS;
// go-api mints no login token pair itself (CHAOS-13155).
func TestSessionsOffAnswers503NamingTheFlagOnEverySignInRoute(t *testing.T) {
	locals := map[string]http.Handler{}
	routes := Routes(Deps{
		Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, slog.New(slog.DiscardHandler)),
		StateSecret: "sso-off-test-state-secret-0123456789abcdef",
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
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "AUTH_PROXY_SESSIONS") || !strings.Contains(rec.Body.String(), `"reason_code":"config_missing"`) {
			t.Errorf("%s: status %d body %q, want 503 naming AUTH_PROXY_SESSIONS", pattern, rec.Code, rec.Body.String())
		}
	}
}

// With a session issuer the sign-in routes run: none answers the sessions-off refusal.
func TestWithASessionIssuerNoSignInRouteAnswersTheSessionsOffRefusal(t *testing.T) {
	locals := map[string]http.Handler{}
	Routes(Deps{
		Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, slog.New(slog.DiscardHandler)),
		StateSecret: "sso-off-test-state-secret-0123456789abcdef", Issuer: okIssuer{},
		Inner: func(pattern string, local http.Handler) http.Handler {
			locals[pattern] = local
			return local
		},
	})
	for pattern, local := range locals {
		rec := httptest.NewRecorder()
		func() {
			defer func() { _ = recover() }()
			local.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}")))
		}()
		if strings.Contains(rec.Body.String(), "AUTH_PROXY_SESSIONS") {
			t.Errorf("%s: answered the sessions-off refusal with an issuer set: %q", pattern, rec.Body.String())
		}
	}
}

type okIssuer struct{}

func (okIssuer) Issue(context.Context, *http.Request, sessionissue.Request) (sessionissue.Pair, error) {
	return sessionissue.Pair{}, nil
}
