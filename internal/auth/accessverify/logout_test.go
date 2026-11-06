package accessverify

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func logoutPoints(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != LegacyLogoutAcceptedMetric || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				if point.Attributes.Len() != 2 {
					t.Fatalf("label set has %d labels, want service and class only", point.Attributes.Len())
				}
				service, _ := point.Attributes.Value(attribute.Key("service"))
				class, _ := point.Attributes.Value(attribute.Key("class"))
				out[service.AsString()+"|"+class.AsString()] += point.Value
			}
		}
	}
	return out
}

func TestLogoutCounterEmitsServiceAndClassAndStartsAtZero(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := NewLogoutCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"), "svc-a")
	if err != nil {
		t.Fatal(err)
	}
	key := "svc-a|" + ClassLegacyRefresh
	if value, present := logoutPoints(t, reader)[key]; !present || value != 0 {
		t.Fatalf("series before any logout = %v (present %t), want 0", value, present)
	}
	counter.Accepted(context.Background())
	if got := logoutPoints(t, reader); got[key] != 1 || len(got) != 1 {
		t.Fatalf("after one logout = %v, want only %s = 1", got, key)
	}
}

func TestLogoutCounterIsNilSafe(t *testing.T) {
	var counter *LogoutCounter
	counter.Accepted(context.Background())
}
