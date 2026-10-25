package otlpmetrics

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/rivercontrib/otelriver"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const syntheticRiverTag = "synthetic-tag"

func TestAJobTagNeverReachesTheReaderOnRiverWorkInstruments(t *testing.T) {
	policy, err := DefaultLabelPolicy(nil)
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(policy.View()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	if err := policy.Register(provider.Meter("test")); err != nil {
		t.Fatal(err)
	}
	middleware := otelriver.NewMiddleware(&otelriver.MiddlewareConfig{MeterProvider: provider})
	job := &rivertype.JobRow{Kind: "synthetic_kind", Queue: "synthetic_queue", Attempt: 1, Priority: 1, Tags: []string{syntheticRiverTag}}
	if err := middleware.Work(context.Background(), job, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if !strings.HasPrefix(m.Name, "river.work_") {
				continue
			}
			seen[m.Name] = true
			var sets []string
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					sets = append(sets, fmt.Sprintf("%v", p.Attributes.ToSlice()))
				}
			case metricdata.Histogram[float64]:
				for _, p := range d.DataPoints {
					sets = append(sets, fmt.Sprintf("%v", p.Attributes.ToSlice()))
				}
			case metricdata.Gauge[float64]:
				for _, p := range d.DataPoints {
					sets = append(sets, fmt.Sprintf("%v", p.Attributes.ToSlice()))
				}
			default:
				t.Fatalf("%s: unexpected data type %T", m.Name, m.Data)
			}
			if len(sets) == 0 {
				t.Fatalf("%s: no data points", m.Name)
			}
			for _, s := range sets {
				if strings.Contains(s, syntheticRiverTag) || strings.Contains(s, "tag") {
					t.Errorf("%s: tag reached the reader: %s", m.Name, s)
				}
				if !strings.Contains(s, "synthetic_kind") || !strings.Contains(s, "synthetic_queue") {
					t.Errorf("%s: kind or queue lost: %s", m.Name, s)
				}
			}
		}
	}
	for _, name := range []string{"river.work_count", "river.work_duration", "river.work_duration_histogram"} {
		if !seen[name] {
			t.Errorf("%s did not reach the reader", name)
		}
	}
	if containsAny(fmt.Sprintf("%+v", data), []string{syntheticRiverTag}) {
		t.Error("the planted tag value is in the exported data")
	}
	if policy.Dropped()[[2]string{"river.work_count", "tag"}] == 0 {
		t.Errorf("drop of tag not counted: %v", policy.Dropped())
	}
}
