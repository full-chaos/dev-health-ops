package syncreconciler

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	prometheusbridge "go.opentelemetry.io/contrib/bridges/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/platform/otlpmetrics"
)

type fragmentText string

func (f fragmentText) EachMetricsFragment(_ map[string]bool, fn func(string, []byte, error)) {
	fn("reconciler", []byte(f), nil)
}

// What the OTLP exporter is handed for the pipeline's own /metrics text: the real writer, the real embedded labels.tsv, the
// real Gatherer and bridge. A family whose row lacks "stage" folds its eight stages into one point without the attribute.
func TestStageHistogramsReachOTLPWithOneSeriesPerStage(t *testing.T) {
	pipeline := phasePipeline(t, func(context.Context) error { return nil })
	if _, err := pipeline.Step(context.Background(), time.Now().UTC(), 10); err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	if err := pipeline.WritePrometheus(&text); err != nil {
		t.Fatal(err)
	}
	policy, err := otlpmetrics.DefaultLabelPolicy(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	gatherer := &otlpmetrics.Gatherer{Source: fragmentText(text.String()), Policy: policy}
	reader := sdkmetric.NewManualReader(sdkmetric.WithProducer(prometheusbridge.NewMetricProducer(prometheusbridge.WithGatherer(gatherer))))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	exported := map[string]metricdata.Metrics{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			exported[m.Name] = m
		}
	}
	want := make([]string, 0, len(orderedStages))
	for _, stage := range orderedStages {
		want = append(want, string(stage))
	}
	sort.Strings(want)
	for _, family := range []string{"dev_health_reconciler_stage_duration_seconds", "dev_health_reconciler_stage_acquire_wait_seconds"} {
		histogram, ok := exported[family].Data.(metricdata.Histogram[float64])
		if !ok {
			t.Fatalf("%s is not exported as a float histogram: %+v", family, exported[family])
		}
		got := make([]string, 0, len(histogram.DataPoints))
		for _, point := range histogram.DataPoints {
			value, _ := point.Attributes.Value("stage")
			got = append(got, value.AsString())
		}
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s exports %d points with stage values %q, want one point per stage %q", family, len(got), got, want)
		}
	}
}
