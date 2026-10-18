package authruntime

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// productionStackMarker is printed once the test has driven the stack. The
// auth gate (ci/check_go.sh auth middleware) fails unless the test binary's
// output carries it: a green test name alone is not evidence that the stack was
// mounted and driven (CHAOS-4887, G-69; the CHAOS-3271 lesson that router-only
// tests are insufficient).
const productionStackMarker = "AUTH_PRODUCTION_STACK_EXECUTED"

// TestProductionMiddlewareStackIsMountedAndDriven builds the API server through
// configure (configureWithRoutes, from a loaded Config) with one probe route, and drives real HTTP requests through
// the resulting handler. It asserts, on the real stack, every behaviour the
// service claims: correlation id on every response (routed or not), the
// 404 and 405 envelopes, per-route rate limiting, the body
// bound, panic recovery without leaking the panic, and the access-log line.
func TestProductionMiddlewareStackIsMountedAndDriven(t *testing.T) {
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(map[string]string{
		authconfig.EnvDatabaseURI:            "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:         "/nonexistent",
		authconfig.EnvSigningKeyID:           "stack-test",
		authconfig.EnvMaxBodyBytes:           "1024",
		authconfig.EnvRateLimitPerSecond:     "0.01",
		authconfig.EnvRateLimitBurst:         "2",
		authconfig.EnvRequestTimeout:         "2s",
		authconfig.EnvAPIAddress:             "127.0.0.1:0",
		authconfig.EnvOperatorAddress:        "127.0.0.1:1",
		authconfig.EnvHealthCheckTimeout:     "1s",
		authconfig.EnvShutdownTimeout:        "5s",
		authconfig.EnvDatabaseConnectTimeout: "1s",
	})})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	logs := &safeBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	routes := []httpapi.Route{
		{Method: http.MethodPost, Pattern: "/v1/probe", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				http.Error(w, "body", http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})},
		{Method: http.MethodGet, Pattern: "/v1/panic", Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("secret-panic-detail")
		})},
	}
	components, err := configureWithRoutes(context.Background(), cfg, logger, routes)
	if err != nil {
		t.Fatalf("configure the production components: %v", err)
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	var apiHandler http.Handler
	for _, component := range components {
		if announced, ok := component.(announcedListener); ok {
			if server, ok := announced.listener.(*httpapi.Server); ok {
				apiHandler = server.Handler()
			}
		}
	}
	if apiHandler == nil {
		t.Fatal("configure built no API listener")
	}
	front := httptest.NewServer(apiHandler)
	defer front.Close()
	client := &http.Client{Timeout: 5 * time.Second}

	do := func(method, path, body string) (*http.Response, []byte) {
		t.Helper()
		request, err := http.NewRequest(method, front.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		return response, raw
	}

	// Unrouted: the service's own 404 envelope, with a correlation id.
	response, raw := do(http.MethodGet, "/v1/nothing-here", "")
	if response.StatusCode != http.StatusNotFound || response.Header.Get("X-Request-ID") == "" {
		t.Fatalf("unrouted path = %d, request id %q", response.StatusCode, response.Header.Get("X-Request-ID"))
	}
	var envelope httpapi.Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Error.RequestID != response.Header.Get("X-Request-ID") {
		t.Fatalf("404 body %q does not carry the response's request id: %v", raw, err)
	}

	// Wrong method on a routed path: 405 with Allow.
	response, _ = do(http.MethodDelete, "/v1/probe", "")
	if response.StatusCode != http.StatusMethodNotAllowed || response.Header.Get("Allow") == "" {
		t.Fatalf("wrong method = %d, Allow %q", response.StatusCode, response.Header.Get("Allow"))
	}

	// Body bound: over MaxBodyBytes is refused before the handler runs.
	response, _ = do(http.MethodPost, "/v1/probe", strings.Repeat("x", 4096))
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body = %d, want 413", response.StatusCode)
	}

	// Panic recovery: a 500 envelope that does not carry the panic value.
	response, raw = do(http.MethodGet, "/v1/panic", "")
	if response.StatusCode != http.StatusInternalServerError || strings.Contains(string(raw), "secret-panic-detail") {
		t.Fatalf("panic = %d %q, want a 500 envelope without the panic value", response.StatusCode, raw)
	}

	// Rate limit: the route's bucket (burst 2, then 0.01/s) refuses with a
	// no-store 429 envelope. Earlier requests on /v1/probe already spent two tokens
	// (405 and 413 are decided inside the bucket), so the next one is refused.
	refused := false
	for i := 0; i < 5 && !refused; i++ {
		response, _ = do(http.MethodPost, "/v1/probe", "{}")
		refused = response.StatusCode == http.StatusTooManyRequests
	}
	if !refused || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("the rate limit never refused with a no-store envelope (last %d)", response.StatusCode)
	}

	if !strings.Contains(logs.String(), `"msg"`) {
		t.Fatal("the access observer wrote nothing to the log")
	}
	t.Logf("%s statuses=404,405,413,500,429 request_id=present no_store=present", productionStackMarker)
}
