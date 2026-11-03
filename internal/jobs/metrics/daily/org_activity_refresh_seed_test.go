package daily

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A counter series exists only after its first Add, so its first export already holds the first failure and an increase() query has
// no earlier sample. The refresh-failed counter must exist at 0 as soon as a refresher is attached, before any failure.

func refreshFailedPoints(t *testing.T, reader *sdkmetric.ManualReader) []int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var values []int64
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != OrgActivityRefreshFailedCounter {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				values = append(values, point.Value)
			}
		}
	}
	return values
}

func manualMeterProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(previous); _ = provider.Shutdown(context.Background()) })
	return reader
}

func TestTheRefreshFailedCounterIsSeededAtZeroWhenARefresherIsAttached(t *testing.T) {
	reader := manualMeterProvider(t)
	handler, err := NewFinalizeHandler(finalizeStoreWithClaim())
	if err != nil {
		t.Fatal(err)
	}
	if got := refreshFailedPoints(t, reader); len(got) != 0 {
		t.Fatalf("a handler with no refresher already holds series %v", got)
	}
	handler.SetOrgActivityRefresher(&recordingOrgActivity{})
	got := refreshFailedPoints(t, reader)
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("series after attaching a refresher = %v, want exactly one at 0", got)
	}
}

func TestAFailedRefreshReadsOneOnTheSeededSeries(t *testing.T) {
	reader := manualMeterProvider(t)
	handler, err := NewFinalizeHandler(finalizeStoreWithClaim())
	if err != nil {
		t.Fatal(err)
	}
	handler.SetOrgActivityRefresher(&recordingOrgActivity{refreshErr: errors.New("boom")})
	handler.refreshOrgActivity(context.Background(), Run{ID: testRunID, OrganizationID: testOrgID})
	got := refreshFailedPoints(t, reader)
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("series after one failed refresh = %v, want exactly one at 1", got)
	}
}
