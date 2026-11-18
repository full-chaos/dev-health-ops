package sso

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// The OAuth provider create and patch routes are retired: nothing writes an oauth_* provider row, so go-api mounts
// neither. A request for either is the service's not-found answer, before any guard, for every process license. The
// callers with a token are the integration cells (oauth_providers_retired_integration_test.go).
func retiredMux(t *testing.T) (*http.ServeMux, []httpapi.Route) {
	t.Helper()
	write := func(w http.ResponseWriter, r *http.Request, code httpapi.Code) {
		w.Header().Set("X-Code", string(code))
		httpapi.WriteError(w, r, code)
	}
	routes := Routes(Deps{Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, slog.New(slog.DiscardHandler)),
		Logger: slog.New(slog.DiscardHandler), Write: write})
	if len(routes) == 0 {
		t.Fatal("no routes mounted")
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { write(w, r, httpapi.CodeNotFound) })
	return mux, routes
}

func TestRetiredOAuthProviderCreateIsNotMounted(t *testing.T) {
	_, routes := retiredMux(t)
	for _, route := range routes {
		if route.Pattern == "/api/v1/auth/oauth/providers" {
			t.Errorf("%s %s is mounted; the OAuth provider create route is retired", route.Method, route.Pattern)
		}
	}
}

func TestRetiredOAuthProviderRoutesAnswerNotFound(t *testing.T) {
	const providerID = "22000000-0000-4000-8000-000000000003"
	for _, license := range []*licensing.ProcessLicense{nil, {Tier: "enterprise", Features: map[string]bool{ssoFeature: true}}} {
		licensing.SetProcessLicense(license)
		t.Cleanup(func() { licensing.SetProcessLicense(nil) })
		mux, _ := retiredMux(t)
		for _, step := range []struct {
			method, path, body string
			status             int
			code               httpapi.Code
			allow              string
		}{
			{http.MethodPost, "/api/v1/auth/oauth/providers", `{}`, http.StatusNotFound, httpapi.CodeNotFound, ""},
			{http.MethodGet, "/api/v1/auth/oauth/providers", "", http.StatusNotFound, httpapi.CodeNotFound, ""},
			{http.MethodPatch, "/api/v1/auth/oauth/providers/" + providerID, `{"name":"x"}`, http.StatusNotFound, httpapi.CodeNotFound, ""},
			{http.MethodPut, "/api/v1/auth/oauth/providers/x", "", http.StatusNotFound, httpapi.CodeNotFound, ""},
			{http.MethodDelete, "/api/v1/auth/oauth/providers/x", "", http.StatusNotFound, httpapi.CodeNotFound, ""},
			// The next route in Starlette order whose path matches answers 405 with its own method.
			{http.MethodPatch, "/api/v1/auth/oauth/providers/authorize", `{}`, http.StatusMethodNotAllowed, httpapi.CodeMethodNotAllowed, "POST"},
			{http.MethodGet, "/api/v1/auth/oauth/providers/callback", "", http.StatusMethodNotAllowed, httpapi.CodeMethodNotAllowed, "POST"},
		} {
			request := httptest.NewRequest(step.method, step.path, strings.NewReader(step.body))
			if step.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, request)
			if recorder.Code != step.status || recorder.Header().Get("X-Code") != string(step.code) || recorder.Header().Get("Allow") != step.allow {
				t.Errorf("license %v: %s %s = %d code %q Allow %q body %s, want %d code %q Allow %q", license, step.method, step.path,
					recorder.Code, recorder.Header().Get("X-Code"), recorder.Header().Get("Allow"), recorder.Body.String(), step.status, step.code, step.allow)
			}
		}
	}
}

// The OAuth sign-in routes stay mounted: without SSO_STATE_SECRET each answers its 503 config_missing, not the not-found.
func TestRetiringOAuthProviderRoutesKeepsTheSignInRoutes(t *testing.T) {
	mux, _ := retiredMux(t)
	for _, step := range []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/auth/oauth/22000000-0000-4000-8000-000000000003/authorize", `{}`},
		{http.MethodPost, "/api/v1/auth/oauth/22000000-0000-4000-8000-000000000003/callback", `{"code":"c","state":"s"}`},
		{http.MethodGet, "/api/v1/auth/oauth/github/authorize?org_id=o", ""},
	} {
		request := httptest.NewRequest(step.method, step.path, strings.NewReader(step.body))
		if step.body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "SSO_STATE_SECRET") {
			t.Errorf("%s %s = %d %s, want the sign-in route's 503 naming SSO_STATE_SECRET", step.method, step.path, recorder.Code, recorder.Body.String())
		}
	}
}
