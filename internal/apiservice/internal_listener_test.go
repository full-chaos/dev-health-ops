package apiservice

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// internalPaths are every request path the unauthenticated internal routes
// answer. The public listener must never serve any of them (CHAOS-7181): the
// network boundary, not a credential, is their only control.
var internalPaths = []string{
	"/api/v1/internal/acr/health",
	"/api/v1/internal/acr/entitlements/0b1b2c3d-0000-4000-8000-000000000000",
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func handlerFor(t *testing.T, address string, internal bool) http.Handler {
	t.Helper()
	cfg := config.Config{APIAddress: address, APIInternalAddress: address}
	if internal {
		server, err := NewInternalServer(cfg, quietLog(), InternalRoutes(Deps{}, quietLog()))
		if err != nil {
			t.Fatalf("NewInternalServer: %v", err)
		}
		return server.Handler()
	}
	server, err := NewServer(cfg, quietLog(), Routes(Deps{}, quietLog()))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return server.Handler()
}

// TestPublicListenerNeverServesInternalRoutes: every internal path, in the
// plain, trailing-slash, encoded, dot-segment, case and prefix-only shapes a
// bypass would try, is a 404 on the public mux with no acr body.
func TestPublicListenerNeverServesInternalRoutes(t *testing.T) {
	public := handlerFor(t, "127.0.0.1:0", false)
	var paths []string
	for _, path := range internalPaths {
		paths = append(paths, path, path+"/", strings.Replace(path, "/internal/", "/%69nternal/", 1),
			strings.Replace(path, "/api/v1/internal", "/api/v1/x/../internal", 1))
	}
	paths = append(paths, "/api/v1/internal", "/api/v1/internal/", "/api/v1/internal/acr")
	for _, path := range paths {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost} {
			rec := httptest.NewRecorder()
			public.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
			body := rec.Body.String()
			if rec.Code == http.StatusOK || strings.Contains(body, "acr_service_health") || strings.Contains(body, "acr_entitlement") {
				t.Errorf("public %s %s = %d %s, want no internal route", method, path, rec.Code, body)
			}
			if rec.Code >= 500 || (rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed && rec.Code < 300) {
				t.Errorf("public %s %s = %d, want a 404-class refusal", method, path, rec.Code)
			}
		}
	}
	// The registered public route table carries no /api/v1/internal pattern.
	for _, route := range Routes(Deps{}, quietLog()) {
		if strings.Contains(route.Pattern, "/api/v1/internal") {
			t.Errorf("public Routes mounts %s %s", route.Method, route.Pattern)
		}
	}
}

// TestInternalListenerServesInternalRoutes: the internal mux answers the two
// routes with the acr client's shapes, and serves nothing else.
func TestInternalListenerServesInternalRoutes(t *testing.T) {
	internal := handlerFor(t, "127.0.0.1:0", true)
	for _, route := range InternalRoutes(Deps{}, quietLog()) {
		if !strings.HasPrefix(route.Pattern, "/api/v1/internal/") {
			t.Errorf("internal mux mounts a non-internal route %s %s", route.Method, route.Pattern)
		}
	}
	// No store (Deps{}): health and entitlements both answer 503, not 404 -- the
	// route exists and reports it is not ready.
	for _, path := range internalPaths {
		rec := httptest.NewRecorder()
		internal.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("internal GET %s = %d %s, want 503 (route present, no store)", path, rec.Code, rec.Body.String())
		}
	}
	for _, path := range []string{"/health", "/api/v1/orgs/me", "/api/v1/internal/acr/nope"} {
		rec := httptest.NewRecorder()
		internal.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("internal GET %s = %d, want 404 (only the internal routes live here)", path, rec.Code)
		}
	}
}

// TestConfigureServesInternalRoutesOnlyOnTheInternalListener: real sockets.
// With DEV_HEALTH_API_INTERNAL_ADDR set the internal paths answer on that
// listener and 404 on the api listener; unset, no internal listener exists
// (fail closed: the paths answer nowhere).
func TestConfigureServesInternalRoutesOnlyOnTheInternalListener(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	off, err := configure(context.Background(), config.Config{APIAddress: "127.0.0.1:0"}, health.NewRegistry(time.Second), quietLogger())
	if err != nil {
		t.Fatalf("configure (off): %v", err)
	}
	if len(off) != 1 {
		t.Fatalf("%d components with the internal address unset, want only the api", len(off))
	}
	components, err := configure(context.Background(), config.Config{APIAddress: "127.0.0.1:0", APIInternalAddress: "127.0.0.1:0"}, registry, quietLogger())
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	if len(components) != 2 || registry.RequiredCount() != 2 {
		t.Fatalf("%d components, %d required checks, want the api and the internal listener", len(components), registry.RequiredCount())
	}
	servers := map[string]*httpapi.Server{}
	for _, component := range components {
		server := component.(*httpapi.Server)
		servers[server.Name()] = server
		if err := server.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", server.Name(), err)
		}
		defer func() { _ = server.Shutdown(context.Background()) }()
	}
	internal, main := servers["internal-http"], servers["api-http"]
	if internal == nil || main == nil || internal.Address() == main.Address() {
		t.Fatalf("components %v", servers)
	}
	if ready := registry.CheckRequired(context.Background()); !ready.Ready {
		t.Fatalf("not ready with both listeners bound: %+v", ready)
	}
	status := func(server *httpapi.Server, path string) int {
		response, err := http.Get("http://" + server.Address() + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode
	}
	for _, path := range internalPaths {
		if got := status(main, path); got != http.StatusNotFound {
			t.Errorf("api listener GET %s = %d, want 404", path, got)
		}
		if got := status(internal, path); got != http.StatusServiceUnavailable {
			t.Errorf("internal listener GET %s = %d, want 503 (present, no database)", path, got)
		}
	}
}

// TestACRPublicCompatBridgeServesBothListeners: with the one-roll bridge on
// (Deps.ACRPublicCompat), the public mux also serves both acr routes; off (the
// default) it serves neither (TestPublicListenerNeverServesInternalRoutes).
func TestACRPublicCompatBridgeServesBothListeners(t *testing.T) {
	cfg := config.Config{APIAddress: "127.0.0.1:0"}
	server, err := NewServer(cfg, quietLog(), Routes(Deps{ACRPublicCompat: true}, quietLog()))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	public := server.Handler()
	for _, path := range internalPaths {
		rec := httptest.NewRecorder()
		public.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("compat public GET %s = %d, want 503 (route present, no store)", path, rec.Code)
		}
	}
	for _, route := range Routes(Deps{ACRPublicCompat: true}, quietLog()) {
		if strings.Contains(route.Pattern, "/api/v1/internal") && !strings.HasPrefix(route.Pattern, "/api/v1/internal/acr/") {
			t.Errorf("compat mounts a non-acr internal route %s", route.Pattern)
		}
	}
}
