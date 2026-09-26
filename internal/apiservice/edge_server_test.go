package apiservice

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// TestEdgeUnhandledErrorShape: the billing edge is a bare Starlette app, so
// an unhandled error is the plain text "Internal Server Error" carrying only
// its content headers (the main app's JSON body comes from an exception
// handler the edge does not have); every other answer passes through.
func TestEdgeUnhandledErrorShape(t *testing.T) {
	unhandled := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(policy.UnhandledErrorHeader, "1")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Extra", "leak")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"detail":"Internal server error"}`))
	})
	rec := httptest.NewRecorder()
	EdgeUnhandledErrorShape(unhandled).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/x", nil))
	if rec.Code != http.StatusInternalServerError || rec.Body.String() != "Internal Server Error" {
		t.Fatalf("unhandled: %d %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Fatalf("content type %q", got)
	}
	if rec.Header().Get("X-Extra") != "" || rec.Header().Get(policy.UnhandledErrorHeader) != "" {
		t.Fatalf("headers leaked: %v", rec.Header())
	}
	if rec.Header().Get("Content-Length") != "21" {
		t.Fatalf("content length %q", rec.Header().Get("Content-Length"))
	}

	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"a":1}`))
	})
	rec = httptest.NewRecorder()
	EdgeUnhandledErrorShape(ok).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != `{"a":1}` || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("passthrough: %d %q %v", rec.Code, rec.Body.String(), rec.Header())
	}
}

// TestConfigureServesTheBillingEdgeOnItsOwnListenerOnlyWhenSet: the edge is
// off unless DEV_HEALTH_API_BILLING_EDGE_ADDR is set (the default wiring is
// one listener); when set it is a second component with its own readiness
// check, serving the edge's /health (503 with no secrets configured) and the
// catch-all 404 for a route only the main listener has, with none of the main
// app's security headers (CHAOS-6520).
func TestConfigureServesTheBillingEdgeOnItsOwnListenerOnlyWhenSet(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	components, err := configure(context.Background(), config.Config{APIAddress: "127.0.0.1:0", APIBillingEdgeAddress: "127.0.0.1:0"}, registry, quietLogger())
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	if len(components) != 2 {
		t.Fatalf("%d components, want the api and the billing edge", len(components))
	}
	if registry.RequiredCount() != 2 {
		t.Fatalf("%d required checks, want the api listener and the edge listener", registry.RequiredCount())
	}
	servers := map[string]*httpapi.Server{}
	for _, component := range components {
		server := component.(*httpapi.Server)
		servers[server.Name()] = server
	}
	edge, main := servers["billing-edge-http"], servers["api-http"]
	if edge == nil || main == nil {
		t.Fatalf("components %v", servers)
	}
	if ready := registry.CheckRequired(context.Background()); ready.Ready {
		t.Fatal("ready before either listener is bound")
	}
	if err := main.Start(context.Background()); err != nil {
		t.Fatalf("start api: %v", err)
	}
	defer func() { _ = main.Shutdown(context.Background()) }()
	if ready := registry.CheckRequired(context.Background()); ready.Ready {
		t.Fatal("ready with the edge listener unbound")
	}
	if err := edge.Start(context.Background()); err != nil {
		t.Fatalf("start edge: %v", err)
	}
	defer func() { _ = edge.Shutdown(context.Background()) }()
	if ready := registry.CheckRequired(context.Background()); !ready.Ready {
		t.Fatalf("not ready with both listeners bound: %+v", ready)
	}
	if edge.Address() == main.Address() {
		t.Fatalf("both listeners on %s", edge.Address())
	}

	get := func(server *httpapi.Server, path string) (*http.Response, string) {
		response, err := http.Get("http://" + server.Address() + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response, string(body)
	}
	response, body := get(edge, "/health")
	wantBody := `{"status":"down","services":{"stripe_secret_key":"not_configured","stripe_webhook_secret":"not_configured","license_private_key":"not_configured","stripe_client":"down","postgres":"not_configured"}}`
	if response.StatusCode != http.StatusServiceUnavailable || body != wantBody {
		t.Fatalf("edge /health: %d %s", response.StatusCode, body)
	}
	response, body = get(edge, "/api/v1/billing/plans")
	if response.StatusCode != http.StatusNotFound || body != `{"detail":"Not Found"}` {
		t.Fatalf("edge plans: %d %s", response.StatusCode, body)
	}
	if response.Header.Get("X-Frame-Options") != "" {
		t.Fatalf("the edge carries the main app's security headers: %v", response.Header)
	}
}
