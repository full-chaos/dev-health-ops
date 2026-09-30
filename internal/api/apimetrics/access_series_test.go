package apimetrics

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// TestRequestSeriesReachTheOperatorMetricsText drives requests through a real
// httpapi server after Install and reads the scrape text the operator
// /metrics serves: the per-route counter and the duration histogram appear
// under the names a query counts by, with the four bounded labels only.
func TestRequestSeriesReachTheOperatorMetricsText(t *testing.T) {
	source, err := Install()
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
	var text bytes.Buffer
	if err := source.WritePrometheus(&text); err != nil {
		t.Fatal(err)
	}
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
