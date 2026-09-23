package health

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func serve(t *testing.T, deps Deps, method, path string) (int, string) {
	t.Helper()
	deps.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	for _, route := range Routes(deps) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	return recorder.Code, recorder.Body.String()
}

func TestProbesWithoutDependencies(t *testing.T) {
	groups := func(names ...string) *[]string { return &names }
	cases := []struct {
		name   string
		deps   Deps
		path   string
		status int
		body   string
	}{
		{"ready", Deps{}, "/ready", 200, `{"status":"ready"}`},
		{"health, nothing configured", Deps{}, "/health", 503,
			`{"status":"down","services":{"postgres":"not_configured","clickhouse":"down","redis":"not_configured","rate_limiter":"noop"}}`},
		{"health, unreachable valkey", Deps{ValkeyURI: "redis://127.0.0.1:1/1"}, "/health", 503,
			`{"status":"down","services":{"postgres":"not_configured","clickhouse":"down","redis":"down","rate_limiter":"noop"}}`},
		{"workers, no fleet declared", Deps{}, "/health/workers", 503, `{"status":"down","services":{"celery":"down"}}`},
		{"workers, declared but empty", Deps{ExpectedWorkerGroups: groups()}, "/health/workers", 503,
			`{"status":"down","services":{"expected_worker_groups":"misconfigured"}}`},
		{"workers, no database", Deps{ExpectedWorkerGroups: groups("ops", "sync", "ops")}, "/health/workers", 503,
			`{"status":"down","services":{"go_worker:ops":"unknown","go_worker:sync":"unknown","celery":"retired"}}`},
	}
	for _, test := range cases {
		status, body := serve(t, test.deps, http.MethodGet, test.path)
		if status != test.status || body != test.body {
			t.Errorf("%s: %d %s, want %d %s", test.name, status, body, test.status, test.body)
		}
	}
	if status, body := serve(t, Deps{}, http.MethodHead, "/ready"); status != 200 || body != `{"status":"ready"}` {
		t.Errorf("HEAD /ready through the handler: %d %q", status, body)
	}
}
