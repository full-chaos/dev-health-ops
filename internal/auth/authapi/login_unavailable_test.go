package authapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoginNotEnabledAnswers503WithRetryAfter1AndGoAPIBody(t *testing.T) {
	route := LoginNotEnabledRoute()
	if route.Method != http.MethodPost || route.Pattern != "/api/v1/auth/login" || !route.Unthrottled {
		t.Fatalf("route %s %s unthrottled=%v; want POST /api/v1/auth/login unthrottled", route.Method, route.Pattern, route.Unthrottled)
	}
	mux := http.NewServeMux()
	mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, route.Pattern, strings.NewReader(`{"email":"a@example.com","password":"x"}`)))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("status %d Retry-After %q; want 503 and 1", rec.Code, rec.Header().Get("Retry-After"))
	}
	if want := `{"detail":{"message":"` + textAuthUnavailable + `"}}`; rec.Body.String() != want {
		t.Fatalf("body %q; want %q", rec.Body.String(), want)
	}
}

func TestLoginNotEnabledWritesNothing(t *testing.T) {
	body := &countingBody{Reader: strings.NewReader(`{"email":"a@example.com"}`)}
	route := LoginNotEnabledRoute()
	rec := httptest.NewRecorder()
	route.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, route.Pattern, body))
	if rec.Code != http.StatusServiceUnavailable || body.reads != 0 {
		t.Fatalf("status %d, body reads %d; want 503 and 0", rec.Code, body.reads)
	}
}
