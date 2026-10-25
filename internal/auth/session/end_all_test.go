package session

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecordEndedAdvancesTheRevocationCounterByTheRevokedCount(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	counter, err := meter.Int64Counter("auth.session.revocations")
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{revocations: counter}
	total := func() int64 {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatal(err)
		}
		var sum int64
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if data, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == "auth.session.revocations" {
					for _, p := range data.DataPoints {
						sum += p.Value
					}
				}
			}
		}
		return sum
	}
	s.RecordEnded(context.Background(), Ended{Sessions: 0, RefreshTokens: 5})
	if got := total(); got != 0 {
		t.Fatalf("counter = %d after an end that revoked no session, want 0", got)
	}
	s.RecordEnded(context.Background(), Ended{Sessions: 3})
	s.RecordEnded(context.Background(), Ended{Sessions: 2})
	if got := total(); got != 5 {
		t.Fatalf("counter = %d, want 5", got)
	}
}
