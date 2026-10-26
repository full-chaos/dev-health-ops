package authapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var sessionsStubPaths = []string{"/api/v1/auth/refresh", "/api/v1/auth/logout"}

func TestSessionsNotEnabledRoutesAreExactlyRefreshAndLogoutUnthrottled(t *testing.T) {
	routes := SessionsNotEnabledRoutes()
	if len(routes) != len(sessionsStubPaths) {
		t.Fatalf("%d routes; want %d", len(routes), len(sessionsStubPaths))
	}
	for i, route := range routes {
		if route.Method != http.MethodPost || route.Pattern != sessionsStubPaths[i] || !route.Unthrottled {
			t.Fatalf("route %d = %s %s unthrottled=%v; want POST %s unthrottled", i, route.Method, route.Pattern, route.Unthrottled, sessionsStubPaths[i])
		}
	}
}

func TestSessionsNotEnabledRoutesAnswer503RetryAfterOneWithGoAPIBody(t *testing.T) {
	for _, route := range SessionsNotEnabledRoutes() {
		rec := httptest.NewRecorder()
		route.Handler.ServeHTTP(rec, httptest.NewRequest(route.Method, route.Pattern, strings.NewReader(`{"refresh_token":"x"}`)))
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
			t.Fatalf("%s: status %d Retry-After %q; want 503 and 1", route.Pattern, rec.Code, rec.Header().Get("Retry-After"))
		}
		if want := `{"detail":{"message":"` + textAuthUnavailable + `"}}`; rec.Body.String() != want {
			t.Fatalf("%s: body %q; want %q", route.Pattern, rec.Body.String(), want)
		}
	}
}

func TestSessionsNotEnabledRoutesReadNoBody(t *testing.T) {
	body := &countingBody{Reader: strings.NewReader(`{"refresh_token":"x"}`)}
	for _, route := range SessionsNotEnabledRoutes() {
		rec := httptest.NewRecorder()
		route.Handler.ServeHTTP(rec, httptest.NewRequest(route.Method, route.Pattern, body))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s: status %d", route.Pattern, rec.Code)
		}
	}
	if body.reads != 0 {
		t.Fatalf("the stubs read the request body %d times", body.reads)
	}
}
