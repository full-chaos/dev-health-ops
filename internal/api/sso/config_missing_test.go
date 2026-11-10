package sso

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

const configMissingProvider = "00000000-0000-4000-8000-000000000001"

func TestSignInRoutesAnswer503ConfigMissingWithoutTheSSOStateKey(t *testing.T) {
	routes := Routes(Deps{
		Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, slog.New(slog.DiscardHandler)),
		Logger: slog.New(slog.DiscardHandler), Issuer: okIssuer{},
	})
	cases := []struct {
		method, pattern, path, first, second string
	}{
		{http.MethodPost, "/api/v1/auth/oidc/{provider_id}/authorize", "/api/v1/auth/oidc/" + configMissingProvider + "/authorize", "", ""},
		{http.MethodPost, "/api/v1/auth/oidc/{provider_id}/callback", "/api/v1/auth/oidc/" + configMissingProvider + "/callback", "", ""},
		{http.MethodGet, "/api/v1/auth/saml/{provider_id}/metadata", "/api/v1/auth/saml/" + configMissingProvider + "/metadata", "", ""},
		{http.MethodPost, "/api/v1/auth/saml/{provider_id}/initiate", "/api/v1/auth/saml/" + configMissingProvider + "/initiate", "", ""},
		{http.MethodPost, "/api/v1/auth/saml/{provider_id}/acs", "/api/v1/auth/saml/" + configMissingProvider + "/acs", "", ""},
		{http.MethodPost, "/api/v1/auth/oauth/{first}/{second}", "/api/v1/auth/oauth/" + configMissingProvider + "/authorize", configMissingProvider, "authorize"},
		{http.MethodPost, "/api/v1/auth/oauth/{first}/{second}", "/api/v1/auth/oauth/" + configMissingProvider + "/callback", configMissingProvider, "callback"},
		{http.MethodGet, "/api/v1/auth/oauth/{first}/{second}", "/api/v1/auth/oauth/github/authorize", "github", "authorize"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			var handler http.Handler
			for _, route := range routes {
				if route.Method == tc.method && route.Pattern == tc.pattern {
					handler = route.Handler
				}
			}
			if handler == nil {
				t.Fatal("route not mounted")
			}
			request := httptest.NewRequest(tc.method, tc.path, strings.NewReader("{}"))
			request.Header.Set("Content-Type", "application/json")
			request.SetPathValue("provider_id", configMissingProvider)
			request.SetPathValue("first", tc.first)
			request.SetPathValue("second", tc.second)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status %d, want 503: %s", recorder.Code, recorder.Body.String())
			}
			var body struct {
				Detail struct {
					Message    string `json:"message"`
					ReasonCode string `json:"reason_code"`
				} `json:"detail"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("body %q: %v", recorder.Body.String(), err)
			}
			if body.Detail.ReasonCode != "config_missing" || !strings.Contains(body.Detail.Message, "SSO_STATE_SECRET") {
				t.Fatalf("body %q, want reason_code config_missing and a message naming SSO_STATE_SECRET", recorder.Body.String())
			}
		})
	}
}
