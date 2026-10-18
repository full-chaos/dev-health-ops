package acr

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var (
	metricReaderOnce sync.Once
	metricReader     *sdkmetric.ManualReader
)

// The global meter provider delegates to its first SDK only, so every test in
// the package reads counters through this one reader and compares deltas.
func acrMetricReader() *sdkmetric.ManualReader {
	metricReaderOnce.Do(func() {
		metricReader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader)))
	})
	return metricReader
}

func counterByAttribute(t *testing.T, name string, filter func(metricdata.DataPoint[int64]) (string, bool)) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := acrMetricReader().Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				if key, ok := filter(point); ok {
					counts[key] += point.Value
				}
			}
		}
	}
	return counts
}

func internalRequestCounts(t *testing.T) map[string]int64 {
	return counterByAttribute(t, "devhealth_acr_internal_requests_total", func(p metricdata.DataPoint[int64]) (string, bool) {
		route, _ := p.Attributes.Value("route")
		outcome, _ := p.Attributes.Value("outcome")
		return route.AsString() + "/" + outcome.AsString(), true
	})
}

func TestRoutesDeclareTheAuthLimiterZeroSeries(t *testing.T) {
	limiter := httpapi.NewKeyedLimiter(httpapi.NewMemoryCounters(nil), AuthAttemptLimit)
	Routes(Deps{Limiter: limiter})
	var scrape strings.Builder
	if err := httpapi.RateLimitStoreErrors.WritePrometheus(&scrape); err != nil {
		t.Fatal(err)
	}
	want := `dev_health_api_rate_limit_store_errors_total{limit="internal_acr_auth_attempt"} 0`
	if !strings.Contains(scrape.String(), want) {
		t.Fatalf("scrape has no zero series for the auth limiter, want %q in:\n%s", want, scrape.String())
	}
}

func TestHealthStoreFailureIsCounted(t *testing.T) {
	acrMetricReader()
	for name, store := range map[string]EntitlementStore{
		"unconfigured":      nil,
		"store_unavailable": &fakeStore{readyErr: ErrUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			before := internalRequestCounts(t)["health/"+name]
			server := newTestServer(store)
			defer server.Close()
			response, err := getAuthed(server.URL + healthPath)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 503 {
				t.Fatalf("status = %d, want 503", response.StatusCode)
			}
			if got := internalRequestCounts(t)["health/"+name] - before; got != 1 {
				t.Fatalf("health 503 counted %d times under outcome %q, want 1", got, name)
			}
		})
	}
}
