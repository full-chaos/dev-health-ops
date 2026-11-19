package sso

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// With sign-in configured and no OIDC forward, the 2 OIDC and 3 OAuth sign-in routes answer 503 config_missing naming
// AUTH_PROXY_OIDC: go-api has no local OIDC or OAuth sign-in (CHAOS-14131), so it reads and writes no provider row.
func TestOIDCAndOAuthForwardOffAnswerConfigMissing(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	routes := Routes(Deps{
		Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, logger), Logger: logger,
		StateSecret: "oidc-forward-only-state-secret", Issuer: okIssuer{},
		HTTPClient: &http.Client{Transport: oidcNoUpstream{t: t}},
	})
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	want := httptest.NewRecorder()
	policy.WriteConfigMissing(want, "AUTH_PROXY_OIDC")
	const provider = "00000000-0000-4000-8000-000000000001"
	for _, tc := range []struct{ name, method, target, body string }{
		{"oidc authorize", http.MethodPost, "/api/v1/auth/oidc/" + provider + "/authorize", `{}`},
		{"oidc callback", http.MethodPost, "/api/v1/auth/oidc/" + provider + "/callback", `{"code":"c","state":"s"}`},
		{"oauth authorize", http.MethodPost, "/api/v1/auth/oauth/" + provider + "/authorize", `{}`},
		{"oauth callback", http.MethodPost, "/api/v1/auth/oauth/" + provider + "/callback", `{"code":"c","state":"s"}`},
		{"oauth by type", http.MethodGet, "/api/v1/auth/oauth/github/authorize?org_id=" + provider, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body *strings.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			} else {
				body = strings.NewReader("")
			}
			request := httptest.NewRequest(tc.method, tc.target, body)
			if tc.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			recorder := httptest.NewRecorder()
			serveRecovered(mux, recorder, request)
			if recorder.Code != http.StatusServiceUnavailable || !bytes.Equal(recorder.Body.Bytes(), want.Body.Bytes()) {
				t.Fatalf("%s %s = %d %q, want 503 %q", tc.method, tc.target, recorder.Code, recorder.Body.Bytes(), want.Body.Bytes())
			}
		})
	}
}

// serveRecovered answers 599 with the panic text when a handler panics (a local sign-in reaching the zero pool).
func serveRecovered(handler http.Handler, w *httptest.ResponseRecorder, r *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			w.Code = 599
			w.Body.Reset()
			_, _ = fmt.Fprint(w.Body, recovered)
		}
	}()
	handler.ServeHTTP(w, r)
}

type oidcNoUpstream struct{ t *testing.T }

func (o oidcNoUpstream) RoundTrip(*http.Request) (*http.Response, error) {
	o.t.Error("OIDC forward off reached an upstream")
	return nil, fmt.Errorf("no upstream")
}
