package httpapi

import (
	"context"
	"log/slog"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The SLO alerts read dev_health_api_http_request_duration_seconds. The p99 target of the PDP is 75 ms and the
// issue and refresh p95 target is 200 ms (docs/reference/auth/readiness-and-slo.md), so both values must be
// bucket edges: a quantile that falls between two edges is only known to the upper edge.
var wantDurationBucketEdges = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.2, 0.25, 0.5, 1, 2.5, 5, 10, 30}

func TestDurationHistogramEdgesArePinned(t *testing.T) {
	if !slices.Equal(durationBuckets, wantDurationBucketEdges) {
		t.Fatalf("durationBuckets = %v, want %v", durationBuckets, wantDurationBucketEdges)
	}
	h := newAccessHarness(t, slog.LevelInfo, nil, itemsRoute())
	h.do("GET", "/v1/items/item-1", nil)
	var collected metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			data, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || m.Name != durationMetricName {
				continue
			}
			for _, point := range data.DataPoints {
				seen++
				if !slices.Equal(point.Bounds, wantDurationBucketEdges) {
					t.Fatalf("exported bounds = %v, want %v", point.Bounds, wantDurationBucketEdges)
				}
			}
		}
	}
	if seen == 0 {
		t.Fatal("no duration histogram point was exported")
	}
}
