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
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// With AUTH_PROXY_OIDC off (no Inner), the 2 OIDC and 3 OAuth sign-in routes answer in this order (CHAOS-14131,
// acp ruling on PR #6591 r1): no SSO_STATE_SECRET -> 503 config_missing naming SSO_STATE_SECRET, before any body check;
// a state secret but no session issuer -> 503 naming AUTH_PROXY_SESSIONS; both set -> the body's 422, else 503 naming
// AUTH_PROXY_OIDC. Every cell runs on a zero pool (a database read would panic), so none reads a provider row.
func TestOIDCAndOAuthForwardOffPrecedence(t *testing.T) {
	const provider = "00000000-0000-4000-8000-000000000001"
	routes := []struct{ name, method, target, valid, invalidTarget, invalid string }{
		{"oidc authorize", http.MethodPost, "/api/v1/auth/oidc/" + provider + "/authorize", `{}`, "", `{"use_pkce":"sometimes"}`},
		{"oidc callback", http.MethodPost, "/api/v1/auth/oidc/" + provider + "/callback", `{"code":"c","state":"s"}`, "", `{"code_verifier":1}`},
		{"oauth authorize", http.MethodPost, "/api/v1/auth/oauth/" + provider + "/authorize", `{}`, "", `{"redirect_uri":[]}`},
		{"oauth callback", http.MethodPost, "/api/v1/auth/oauth/" + provider + "/callback", `{"code":"c","state":"s"}`, "", `{}`},
		{"oauth by type", http.MethodGet, "/api/v1/auth/oauth/github/authorize?org_id=" + provider, "", "/api/v1/auth/oauth/github/authorize", ""},
	}
	configs := []struct {
		name        string
		stateSecret string
		issuer      sessionissue.Issuer
		// named is the env the 503 names for a valid body; invalidStatus is the answer to an invalid one.
		named         string
		invalidStatus int
	}{
		{"no state secret", "", okIssuer{}, "SSO_STATE_SECRET", http.StatusServiceUnavailable},
		{"sessions off", "precedence-state-secret", nil, "AUTH_PROXY_SESSIONS", http.StatusServiceUnavailable},
		{"all set", "precedence-state-secret", okIssuer{}, "AUTH_PROXY_OIDC", http.StatusUnprocessableEntity},
	}
	cells := 0
	for _, config := range configs {
		logger := slog.New(slog.DiscardHandler)
		mounted := Routes(Deps{Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, logger), Logger: logger,
			StateSecret: config.stateSecret, Issuer: config.issuer, HTTPClient: &http.Client{Transport: oidcNoUpstream{t: t}}})
		mux := http.NewServeMux()
		for _, route := range mounted {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
		for _, route := range routes {
			for _, input := range []string{"valid", "invalid"} {
				target, body, wantStatus := route.target, route.valid, http.StatusServiceUnavailable
				if input == "invalid" {
					body, wantStatus = route.invalid, config.invalidStatus
					if route.invalidTarget != "" {
						target = route.invalidTarget
					}
				}
				t.Run(config.name+"/"+route.name+"/"+input, func(t *testing.T) {
					cells++
					request := httptest.NewRequest(route.method, target, strings.NewReader(body))
					if body != "" {
						request.Header.Set("Content-Type", "application/json")
					}
					recorder := httptest.NewRecorder()
					serveRecovered(mux, recorder, request)
					if recorder.Code != wantStatus {
						t.Fatalf("status %d %s, want %d", recorder.Code, recorder.Body.String(), wantStatus)
					}
					if wantStatus != http.StatusServiceUnavailable {
						return
					}
					var answer struct {
						Detail struct {
							Message    string `json:"message"`
							ReasonCode string `json:"reason_code"`
						} `json:"detail"`
					}
					if err := json.Unmarshal(recorder.Body.Bytes(), &answer); err != nil {
						t.Fatalf("body %s: %v", recorder.Body.String(), err)
					}
					if answer.Detail.ReasonCode != "config_missing" || answer.Detail.Message != config.named+" is not set" {
						t.Fatalf("503 %s, want config_missing %q", recorder.Body.String(), config.named+" is not set")
					}
				})
			}
		}
	}
	if cells != 30 {
		t.Fatalf("%d cells ran, want 30 (3 configurations x 5 routes x valid/invalid)", cells)
	}
}
