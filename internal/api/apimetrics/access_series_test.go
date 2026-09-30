package apimetrics

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// TestRequestSeriesReachTheOperatorMetricsText drives requests through a real
// httpapi server, registers the source on a health registry the way every
// process does, and reads the body the operator /metrics HTTP handler serves: the per-route counter and the duration histogram appear
// under the names a query counts by, with the four bounded labels only.
func TestRequestSeriesReachTheOperatorMetricsText(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	if err := Register(registry); err != nil {
		t.Fatal(err)
	}
	operator, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test", Version: "test"})
	if err != nil {
		t.Fatal(err)
	}
	server, err := httpapi.NewServer(httpapi.ServerOptions{
		Address: "127.0.0.1:0", Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)), Listener: "internal",
		RequestTimeout: time.Second, MaxBodyBytes: 1024, RateLimit: 1000, RateLimitBurst: 1000,
		Routes: []httpapi.Route{{Method: http.MethodGet, Pattern: "/api/v1/scrape/{id}", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"/api/v1/scrape/abc", "/api/v1/scrape/def", "/api/v1/gone/abc"} {
		server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, target, nil))
	}
	scrape := httptest.NewRecorder()
	operator.Handler().ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if scrape.Code != http.StatusOK {
		t.Fatalf("operator /metrics = %d, want 200", scrape.Code)
	}
	text := scrape.Body
	for _, want := range []string{
		`dev_health_api_http_requests_total{listener="internal",method="GET",route="/api/v1/scrape/{id}",status_class="2xx"} 2`,
		`dev_health_api_http_requests_total{listener="internal",method="GET",route="unmatched",status_class="4xx"} 1`,
		`dev_health_api_http_request_duration_seconds_count{listener="internal",method="GET",route="/api/v1/scrape/{id}",status_class="2xx"} 2`,
		`dev_health_api_http_request_duration_seconds_bucket{listener="internal",method="GET",route="/api/v1/scrape/{id}",status_class="2xx",le="0.005"}`,
	} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("scrape text lacks %s\n%s", want, text.String())
		}
	}
	if strings.Contains(text.String(), "abc") || strings.Contains(text.String(), "gone") {
		t.Errorf("scrape text carries a raw path:\n%s", text.String())
	}
}
