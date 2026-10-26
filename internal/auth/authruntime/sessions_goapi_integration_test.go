//go:build integration

package authruntime

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

func goAPIPost(handler http.Handler, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.RemoteAddr = "192.0.2.10:4711"
	request.Header.Set("Origin", goAPITestOrigin)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// TestGoAPIProxiedRefreshLogoutMixedWindowIs503ThenOK drives one go-api against
// two auth-service upstreams in sequence: a pod without session keys (503 +
// Retry-After: 1, never 404), then the same address answered by a keyed pod
// over live PostgreSQL (the real answers).
func TestGoAPIProxiedRefreshLogoutMixedWindowIs503ThenOK(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)

	cfg := authconfig.Config{}
	routes := loginSessionRoutes(t, cfg, nil)
	options := apiServerOptions(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), routes)
	options.Address, options.RequestTimeout, options.MaxBodyBytes = "127.0.0.1:0", time.Second, 1024
	keyless, err := httpapi.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}

	var current atomic.Pointer[http.Handler]
	oldPod, newPod := keyless.Handler(), http.Handler(e.front.Config.Handler)
	current.Store(&oldPod)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { (*current.Load()).ServeHTTP(w, r) }))
	defer upstream.Close()
	front := goAPIFront(t, upstream.URL)

	body := `{"refresh_token":"not-a-live-credential"}`
	for _, path := range []string{"/api/v1/auth/refresh", "/api/v1/auth/logout"} {
		rec := goAPIPost(front, path, body)
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
			t.Fatalf("keyless %s: status %d Retry-After %q body %q; want 503 and 1", path, rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
		}
	}
	current.Store(&newPod)
	if rec := goAPIPost(front, "/api/v1/auth/refresh", body); rec.Code != http.StatusUnauthorized {
		t.Fatalf("keyed refresh: status %d body %q; want the real 401", rec.Code, rec.Body.String())
	}
	if rec := goAPIPost(front, "/api/v1/auth/logout", body); rec.Code != http.StatusOK {
		t.Fatalf("keyed logout: status %d body %q; want the real 200", rec.Code, rec.Body.String())
	}
}
