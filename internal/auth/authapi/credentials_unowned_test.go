package authapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
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

func TestNotOwnedRoutesAnswer503ThroughNewServerUnderLoad(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	routes := append(NotOwnedCredentialRoutes(), httpapi.Route{Method: http.MethodGet, Pattern: "/v1/test-hold", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
	})})
	now := time.Now()
	server, err := httpapi.NewServer(httpapi.ServerOptions{
		Address: "127.0.0.1:0", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Routes: routes,
		RequestTimeout: time.Second, MaxBodyBytes: 1024, RateLimit: 0.001, RateLimitBurst: 1, MaxInFlight: 1,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/test-hold", nil))
		close(done)
	}()
	<-entered
	for round := 0; round < 4; round++ {
		for _, rt := range notOwnedRoutes {
			req := httptest.NewRequest(rt.method, rt.path, strings.NewReader("{}"))
			req.ContentLength = 1 << 30
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
				t.Fatalf("round %d %s %s: %d Retry-After %q; want 503 and 1", round, rt.method, rt.path, rec.Code, rec.Header().Get("Retry-After"))
			}
		}
	}
	close(release)
	<-done
}
