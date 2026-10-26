package authapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func notOwnedMux(t *testing.T) *http.ServeMux {
	t.Helper()
	routes := NotOwnedCredentialRoutes()
	if len(routes) != 6 {
		t.Fatalf("%d routes; want 6", len(routes))
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	return mux
}

var notOwnedRoutes = []struct{ method, path string }{
	{http.MethodGet, "/api/v1/auth/verify"},
	{http.MethodPost, "/api/v1/auth/resend-verification"},
	{http.MethodPost, "/api/v1/auth/forgot-password"},
	{http.MethodPost, "/api/v1/auth/reset-password"},
	{http.MethodPost, "/api/v1/auth/change-password"},
	{http.MethodPost, "/api/v1/auth/register"},
}

func TestCredentialRoutesNotOwnedAnswer503WithRetryAfterOnAllSixRoutes(t *testing.T) {
	mux := notOwnedMux(t)
	for _, rt := range notOwnedRoutes {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(rt.method, rt.path, strings.NewReader(`{"email":"a@example.com"}`)))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s %s: status %d; want 503", rt.method, rt.path, rec.Code)
		}
		if got := rec.Header().Get("Retry-After"); got != "1" {
			t.Fatalf("%s %s: Retry-After %q; want 1", rt.method, rt.path, got)
		}
		want := `{"detail":{"message":"` + textAuthUnavailable + `"}}`
		if rec.Body.String() != want {
			t.Fatalf("%s %s: body %q; want %q", rt.method, rt.path, rec.Body.String(), want)
		}
	}
}

func TestCredentialRoutesNotOwnedWriteNothing(t *testing.T) {
	body := &countingBody{Reader: strings.NewReader(`{"email":"a@example.com"}`)}
	mux := notOwnedMux(t)
	for _, rt := range notOwnedRoutes {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(rt.method, rt.path, body)
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s %s: status %d", rt.method, rt.path, rec.Code)
		}
	}
	if body.reads != 0 {
		t.Fatalf("the stub read the request body %d times", body.reads)
	}
}

type countingBody struct {
	*strings.Reader
	reads int
}

func (c *countingBody) Read(p []byte) (int, error) {
	c.reads++
	return c.Reader.Read(p)
}

func TestUnavailableRetryAnswers503RetryAfterAndReadsNothing(t *testing.T) {
	body := &countingBody{Reader: strings.NewReader("payload")}
	rec := httptest.NewRecorder()
	UnavailableRetry().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/x", body))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("status %d Retry-After %q; want 503 and 1", rec.Code, rec.Header().Get("Retry-After"))
	}
	if want := `{"detail":{"message":"` + textAuthUnavailable + `"}}`; rec.Body.String() != want {
		t.Fatalf("body %q; want %q", rec.Body.String(), want)
	}
	if rec.Header().Get("Content-Type") != "application/json" || body.reads != 0 {
		t.Fatalf("content type %q reads %d", rec.Header().Get("Content-Type"), body.reads)
	}
}
