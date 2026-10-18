package daily

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecordUserRowsSpanningTeamsCountsForEachProvider(t *testing.T) {
	reader := useManualMeterProvider(t)

	recordUserRowsSpanningTeams(context.Background(), map[string]int{"jira": 2, "github": 1})
	recordUserRowsSpanningTeams(context.Background(), nil)

	got := spanningCounterByProvider(t, reader)
	if got["jira"] != 2 || got["github"] != 1 || len(got) != 2 {
		t.Fatalf("counter = %v, want jira 2 and github 1", got)
	}
}

// useManualMeterProvider installs a meter provider that a test reads back, and
// restores the previous one when the test ends.
func useManualMeterProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	return reader
}

// spanningCounterByProvider reads the sum of the spanning-teams counter for each provider.
func spanningCounterByProvider(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != WorkItemUserRowsSpanningTeamsCounter {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want a sum", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				provider, _ := point.Attributes.Value("provider")
				got[provider.AsString()] += point.Value
			}
		}
	}
	return got
}
