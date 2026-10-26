package authruntime

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	refreshPattern = "/api/v1/auth/refresh"
	logoutPattern  = "/api/v1/auth/logout"
	issuePattern   = "/internal/v1/sessions"
)

func routesNamed(routes []httpapi.Route, pattern string) []httpapi.Route {
	var out []httpapi.Route
	for _, route := range routes {
		if route.Pattern == pattern {
			out = append(out, route)
		}
	}
	return out
}

func TestSessionRoutesWithoutKeysAreExactlyTheRefreshLogoutStubs(t *testing.T) {
	routes, err := sessionRoutes(authconfig.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, authschema.ValidatedIdentifier{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, route := range routes {
		if !route.Unthrottled {
			t.Fatalf("%s %s is throttled; a stub must answer before any limiter", route.Method, route.Pattern)
		}
		got = append(got, route.Method+" "+route.Pattern)
	}
	if want := "POST " + refreshPattern + ",POST " + logoutPattern; strings.Join(got, ",") != want {
		t.Fatalf("mounted %v; want exactly %s", got, want)
	}
	if credentials, _, err := credentialRoutes(authconfig.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, authschema.ValidatedIdentifier{}, nil); err != nil || len(credentials) != 0 {
		t.Fatalf("keyless credential routes %d err %v; want none, so the unthrottled set is exactly the 2 stubs", len(credentials), err)
	}
	if len(routesNamed(routes, issuePattern)) != 0 {
		t.Fatal("a stub is mounted on the session issue route, which must keep its service-credential check")
	}
	for _, route := range routes {
		rec := httptest.NewRecorder()
		route.Handler.ServeHTTP(rec, httptest.NewRequest(route.Method, route.Pattern, strings.NewReader("{}")))
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
			t.Fatalf("%s: status %d Retry-After %q; want 503 and 1", route.Pattern, rec.Code, rec.Header().Get("Retry-After"))
		}
	}
}

func TestSessionRoutesWithKeysMountTheRealRefreshLogoutAndNoStub(t *testing.T) {
	routes := loginSessionRoutes(t, loginConfig(t, true), mustLimits(t))
	for _, pattern := range []string{refreshPattern, logoutPattern} {
		mounted := routesNamed(routes, pattern)
		if len(mounted) != 1 {
			t.Fatalf("%s mounted %d times; want once (the real route, no stub)", pattern, len(mounted))
		}
		rec := httptest.NewRecorder()
		mounted[0].Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, pattern, strings.NewReader("{}")))
		if rec.Code == http.StatusServiceUnavailable && rec.Header().Get("Retry-After") == "1" && rec.Body.Len() < 100 {
			t.Fatalf("%s: the real route answered the stub", pattern)
		}
	}
	if len(routesNamed(routes, issuePattern)) != 1 {
		t.Fatalf("the session issue route is mounted %d times; want once", len(routesNamed(routes, issuePattern)))
	}
}

func TestSessionStubsAnswer503ThroughTheServerUnderLoad(t *testing.T) {
	cfg := authconfig.Config{}
	routes := loginSessionRoutes(t, cfg, nil)
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	routes = append(routes, httpapi.Route{Method: http.MethodGet, Pattern: "/v1/test-hold", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
	})})
	options := apiServerOptions(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), routes)
	options.RateLimit, options.RateLimitBurst, options.MaxInFlight = 0.001, 1, 1
	options.Address, options.RequestTimeout, options.MaxBodyBytes = "127.0.0.1:0", time.Second, 1024
	server, err := httpapi.NewServer(options)
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
		for _, pattern := range []string{refreshPattern, logoutPattern} {
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, pattern, strings.NewReader("{}")))
			if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
				t.Fatalf("round %d %s: %d Retry-After %q; want 503 and 1", round, pattern, rec.Code, rec.Header().Get("Retry-After"))
			}
		}
	}
	close(release)
	<-done
}

func TestAPIRoutesLogSaysMountedOnlyForRealRoutes(t *testing.T) {
	stubs := loginSessionRoutes(t, authconfig.Config{}, nil)
	real := loginSessionRoutes(t, loginConfig(t, true), mustLimits(t))
	wellKnown := []httpapi.Route{{Method: http.MethodGet, Pattern: "/.well-known/jwks.json"}}
	logged := func(cfg authconfig.Config, routes []httpapi.Route) string {
		var buf bytes.Buffer
		logAPIRoutes(context.Background(), slog.New(slog.NewTextHandler(&buf, nil)), cfg, routes, 0)
		return buf.String()
	}
	keyless := authconfig.Config{}
	if got := logged(keyless, stubs); !strings.Contains(got, "api_routes_mounted=false") || !strings.Contains(got, "sessions_stub=true") {
		t.Fatalf("stub-only pod logged %q; want api_routes_mounted=false sessions_stub=true", got)
	}
	if got := logged(keyless, append(append([]httpapi.Route{}, stubs...), wellKnown...)); !strings.Contains(got, "api_routes_mounted=true") || !strings.Contains(got, "sessions_stub=true") {
		t.Fatalf("stub plus discovery pod logged %q; want api_routes_mounted=true sessions_stub=true", got)
	}
	if got := logged(loginConfig(t, true), real); !strings.Contains(got, "api_routes_mounted=true") || !strings.Contains(got, "sessions_stub=false") {
		t.Fatalf("keyed pod logged %q; want api_routes_mounted=true sessions_stub=false", got)
	}
}
