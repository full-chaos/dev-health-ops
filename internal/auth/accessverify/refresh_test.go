package accessverify

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func refreshPoints(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != RefreshAcceptedMetric || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				service, _ := point.Attributes.Value(attribute.Key("service"))
				class, _ := point.Attributes.Value(attribute.Key("class"))
				if point.Attributes.Len() != 2 {
					t.Fatalf("label set has %d labels, want service and class only", point.Attributes.Len())
				}
				out[service.AsString()+"|"+class.AsString()] += point.Value
			}
		}
	}
	return out
}

func TestRefreshCounterEmitsServiceAndClassAndStartsAtZero(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	counter, err := NewRefreshCounter(meter, "svc-a")
	if err != nil {
		t.Fatal(err)
	}
	key := "svc-a|" + ClassLegacyRefresh
	points := refreshPoints(t, reader)
	if value, present := points[key]; !present || value != 0 {
		t.Fatalf("series before any accept = %v (present %t), want 0", value, present)
	}
	counter.Accepted(context.Background())
	counter.Accepted(context.Background())
	if got := refreshPoints(t, reader)[key]; got != 2 {
		t.Fatalf("after two accepts = %d, want 2", got)
	}
}

func TestRefreshCounterIsNilSafe(t *testing.T) {
	var counter *RefreshCounter
	counter.Accepted(context.Background())
}
