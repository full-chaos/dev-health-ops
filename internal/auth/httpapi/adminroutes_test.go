package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMatchesAdminRouteUsesMethodAndPatternRules(t *testing.T) {
	cases := []struct {
		method, path string
		want         bool
	}{
		{"GET", "/api/v1/admin/llm-settings/models", true},
		{"GET", "/api/v1/admin/integrations/pagerduty/webhook-bindings/abc", true},
		{"PATCH", "/api/v1/admin/llm-settings/models", false},
		{"GET", "/api/v1/admin/credentials", false},
		{"GET", "/api/v1/admin/llm-settings/models/extra", false},
		{"GET", "/api/v1/other", false},
	}
	for _, tc := range cases {
		if got := MatchesAdminRoute(httptest.NewRequest(tc.method, tc.path, nil)); got != tc.want {
			t.Errorf("%s %s: Matches = %v, want %v", tc.method, tc.path, got, tc.want)
		}
	}
}

func TestNoStoreAdminRoutesSetsTheHeaderBeforeTheNextHandlerOnlyOnAdminRoutes(t *testing.T) {
	seen := ""
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		seen = w.Header().Get("Cache-Control")
		w.WriteHeader(http.StatusForbidden)
	})
	handler := NoStoreAdminRoutes(next)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/api/v1/admin/llm-settings/models", nil))
	if seen != "no-store" || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("admin route: next saw %q, response %q", seen, recorder.Header().Get("Cache-Control"))
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/api/v1/other", nil))
	if recorder.Header().Get("Cache-Control") != "" {
		t.Fatalf("non-admin route got Cache-Control %q", recorder.Header().Get("Cache-Control"))
	}
}
