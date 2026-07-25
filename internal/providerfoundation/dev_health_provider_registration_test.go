package providerfoundation

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// TestSharedMetricsAccumulateAcrossRealRequestsAndScrape is CHAOS-3118
// evidence for the dev_health_provider_* family: it proves both halves of the
// gap the report identified.
//
//  1. A single, shared *Metrics accumulates across multiple real
//     HTTPClient.Do calls, instead of the pre-fix pattern
//     (cmd/dev-health-worker/provider_sync.go constructing a fresh
//     providerfoundation.NewMetrics() inside BuildExecutor, once per unit
//     dispatch, and discarding it — every counter reset to zero on the very
//     next unit and was never scraped in between).
//  2. Registering that one instance with a health.Registry — exactly the
//     production /metrics path (health.Server.handleMetrics ->
//     Registry.WriteMetricsPartial) — surfaces its real, accumulated,
//     HELP/TYPE-complete series, which is what "registered" means for this
//     family (see cmd/dev-health-worker/dependencies.go's
//     workerFamily.metricsSource wiring).
func TestSharedMetricsAccumulateAcrossRealRequestsAndScrape(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()

	successClient := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusOK, nil, `{}`), nil
	}), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	successClient.Metrics = metrics
	successClient.Provider = "github"

	rateLimitedClient := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusTooManyRequests, nil, `{}`), nil
	}), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	rateLimitedClient.Metrics = metrics
	rateLimitedClient.Provider = "github"

	// Two real requests through the SAME *Metrics instance, as two different
	// units dispatched by one long-lived worker process would.
	if _, err := successClient.Do(context.Background(), http.MethodGet, "/items", nil); err != nil {
		t.Fatalf("first request: %v", err)
	}
	if _, err := rateLimitedClient.Do(context.Background(), http.MethodGet, "/items", nil); err == nil {
		t.Fatal("expected the second request to report a rate-limited classification")
	}

	registry := health.NewRegistry(time.Second)
	if err := registry.RegisterMetrics("provider_foundation", metrics); err != nil {
		t.Fatalf("RegisterMetrics: %v", err)
	}

	var scrape bytes.Buffer
	outcomes, err := registry.WriteMetricsPartial(&scrape)
	if err != nil {
		t.Fatalf("WriteMetricsPartial: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Source != "provider_foundation" || outcomes[0].Err != nil {
		t.Fatalf("unexpected scrape outcomes: %+v", outcomes)
	}
	text := scrape.String()
	if !strings.Contains(text, "# HELP dev_health_provider_requests_total") ||
		!strings.Contains(text, "# TYPE dev_health_provider_requests_total counter") {
		t.Fatalf("missing HELP/TYPE metadata in scraped output:\n%s", text)
	}
	if !strings.Contains(text, `dev_health_provider_requests_total{provider="github",class=""} 1`) {
		t.Fatalf("missing accumulated success sample in scraped output:\n%s", text)
	}
	if !strings.Contains(text, `dev_health_provider_requests_total{provider="github",class="rate_limited"} 1`) {
		t.Fatalf("missing accumulated rate-limited sample in scraped output:\n%s", text)
	}
}
