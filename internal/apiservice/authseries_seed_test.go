package apiservice

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// A cumulative OTel counter creates a series on its first Add, so the first export of a low-traffic series already holds its count and
// a rate/increase query has no earlier sample to subtract. Both families below must exist at 0 before the first request.

func counterPoints(t *testing.T, reader *sdkmetric.ManualReader, name string) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				out[point.Attributes.Encoded(attribute.DefaultEncoder())] = point.Value
			}
		}
	}
	return out
}

func TestTheAuthProxyCounterIsSeededAtZeroBeforeAnyRequest(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	counter, err := provider.Meter("seed-test").Int64Counter("devhealth_api_auth_proxy_requests_total")
	if err != nil {
		t.Fatal(err)
	}
	previous := authProxyRequests
	authProxyRequests = counter
	t.Cleanup(func() { authProxyRequests = previous })

	proxy := newAuthProxy(mustProxyConfig(t, allOn("http://127.0.0.1:1")), quietLogger())
	proxy.seedSeries(proxiedRoutes(func(AuthAggregate) bool { return true }))

	points := counterPoints(t, reader, "devhealth_api_auth_proxy_requests_total")
	if len(points) == 0 {
		t.Fatal("no series exists before the first request: the first export would already hold the count")
	}
	for series, value := range points {
		if value != 0 {
			t.Errorf("series %s starts at %d, want 0", series, value)
		}
	}
}

func TestThePublicListenersAuthRouteRequestCounterIsSeededAtZeroBeforeAnyRequest(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(previous); _ = provider.Shutdown(context.Background()) })

	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewServer(cfg, quietLogger(), proxiedRoutes(func(AuthAggregate) bool { return true })); err != nil {
		t.Fatal(err)
	}

	points := counterPoints(t, reader, "dev_health_api_http_requests_total")
	if len(points) == 0 {
		t.Fatal("no per-route series exists before the first request")
	}
	for series, value := range points {
		if value != 0 {
			t.Errorf("series %s starts at %d, want 0", series, value)
		}
	}
}
