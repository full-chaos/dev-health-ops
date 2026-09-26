package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func compatServe(t *testing.T, checks map[string]error, ready bool, path string) (int, string, string) {
	t.Helper()
	registry := health.NewRegistry(2 * time.Second)
	for name, failure := range checks {
		failure := failure
		if err := registry.RegisterRequired(name, func(context.Context) error { return failure }); err != nil {
			t.Fatal(err)
		}
	}
	registry.SetReady(ready)
	operator, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test"})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	OperatorCompat(registry, operator, "dev-health-query-api").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	return recorder.Code, recorder.Body.String(), recorder.Header().Get("Content-Type")
}

// The old query listener's /readyz bodies, byte for byte (the go-api runbook asserts them),
// from the same required checks the operator /readyz runs; a failure says its class only.
func TestCompatReadyzKeepsTheOldBodies(t *testing.T) {
	boom := errors.New("dial tcp 10.0.0.9:5432: secret-host refused")
	cases := []struct {
		name   string
		checks map[string]error
		ready  bool
		code   int
		body   string
	}{
		{"all dependencies pass", map[string]error{"query_clickhouse": nil, "query_postgres": nil, "query_jwks": nil, "query_role_posture": nil, "query_listener": nil}, true, 200, "ready"},
		{"nothing configured", map[string]error{notConfiguredCheckName: nil, "query_listener": nil}, true, 200, "ready: /query not configured"},
		{"clickhouse", map[string]error{"query_clickhouse": boom, "query_postgres": nil}, true, 503, "not ready: clickhouse"},
		{"postgres", map[string]error{"query_clickhouse": nil, "query_postgres": boom}, true, 503, "not ready: postgres"},
		{"jwks", map[string]error{"query_jwks": boom}, true, 503, "not ready: jwks"},
		{"posture", map[string]error{"query_role_posture": boom}, true, 503, "not ready: postgres_posture"},
		{"first class wins", map[string]error{"query_jwks": boom, "query_postgres": boom}, true, 503, "not ready: postgres"},
		{"listener not bound", map[string]error{"query_listener": boom}, true, 503, "not ready: dependency"},
		{"gate closed", map[string]error{"query_clickhouse": nil}, false, 503, "not ready: dependency"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, body, contentType := compatServe(t, c.checks, c.ready, "/readyz")
			if code != c.code || body != c.body {
				t.Fatalf("/readyz = %d %q, want %d %q", code, body, c.code, c.body)
			}
			if contentType != "text/plain; charset=utf-8" {
				t.Fatalf("Content-Type = %q", contentType)
			}
			if strings.Contains(body, "10.0.0.9") || strings.Contains(body, "secret-host") {
				t.Fatalf("/readyz leaks the dependency error: %q", body)
			}
		})
	}
}

func TestCompatHealthzAndMetrics(t *testing.T) {
	if code, body, _ := compatServe(t, nil, true, "/healthz"); code != 200 || body != "ok" {
		t.Fatalf("/healthz = %d %q, want 200 ok", code, body)
	}
	code, body, _ := compatServe(t, map[string]error{"query_postgres": nil}, true, "/metrics")
	if code != 200 || !strings.Contains(body, `dev_health_runtime_check_failed{check="query_postgres"} 0`) {
		t.Fatalf("/metrics = %d, not the process metrics:\n%s", code, body)
	}
	// The old listener's target_info, one sample of 1 naming the service and the SDK.
	if !strings.Contains(body, `target_info{service_name="dev-health-query-api",telemetry_sdk_language="go",telemetry_sdk_name="opentelemetry",telemetry_sdk_version="`+sdk.Version()+`"} 1`) ||
		!strings.Contains(body, "\n# HELP target_info Target metadata\n# TYPE target_info gauge\n") || !strings.HasSuffix(body, "} 1\n") {
		t.Fatalf("/metrics lacks the old target_info series:\n%s", body)
	}
}

// A refused scrape (the operator handler answers only reads) keeps its status and gets no
// target_info appended.
func TestCompatMetricsKeepsTheOperatorsRefusal(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	operator, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test"})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	OperatorCompat(registry, operator, "dev-health-query-api").ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if recorder.Code == http.StatusOK || strings.Contains(recorder.Body.String(), "target_info") {
		t.Fatalf("POST /metrics = %d %q, want the operator's refusal untouched", recorder.Code, recorder.Body.String())
	}
}

// A listener with the compat routes answers the three operator paths and still serves
// the plane's routes; without them the operator paths belong to the plane (404).
func TestListenersCompatRoutesSitBesideThePlane(t *testing.T) {
	plane := &Plane{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}), Close: func() {}}
	registry := health.NewRegistry(time.Second)
	if err := registry.RegisterRequired("query_routes", NotConfiguredCheck()); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)
	operator, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test"})
	if err != nil {
		t.Fatal(err)
	}
	with, _ := Listeners("127.0.0.1:0", "", plane, OperatorCompat(registry, operator, "dev-health-query-api"))
	without, _ := Listeners("127.0.0.1:0", "", plane, nil)
	status := func(l *Listener, path string) int {
		rec := httptest.NewRecorder()
		l.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		if got := status(with, path); got != http.StatusOK {
			t.Errorf("with compat: %s = %d, want 200", path, got)
		}
		if got := status(without, path); got != http.StatusTeapot {
			t.Errorf("without compat: %s = %d, want the plane's answer", path, got)
		}
	}
	if got := status(with, "/query"); got != http.StatusTeapot {
		t.Errorf("with compat: /query = %d, want the plane's answer", got)
	}
}
