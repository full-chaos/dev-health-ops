package otlpmetrics

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	prometheusbridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func fragmentPolicy(t *testing.T, table string) *LabelPolicy {
	t.Helper()
	policy, err := NewLabelPolicy([]byte("instrument\tlabels\treason\n"+table), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

// collectExported runs the fragments through the real Gatherer, the real prometheusbridge producer and an in-memory reader:
// what the OTLP exporter would be handed.
func collectExported(t *testing.T, fragments string, policy *LabelPolicy) map[string]metricdata.Metrics {
	t.Helper()
	gatherer := &Gatherer{Source: textSource{"fragment": fragments}, Policy: policy, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	reader := sdkmetric.NewManualReader(sdkmetric.WithProducer(prometheusbridge.NewMetricProducer(prometheusbridge.WithGatherer(gatherer))))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]metricdata.Metrics{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func sumPoints(t *testing.T, m metricdata.Metrics) []metricdata.DataPoint[float64] {
	t.Helper()
	sum, ok := m.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("%s is %T, want a float sum", m.Name, m.Data)
	}
	return sum.DataPoints
}

func gaugePoints(t *testing.T, m metricdata.Metrics) []metricdata.DataPoint[float64] {
	t.Helper()
	gauge, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("%s is %T, want a float gauge", m.Name, m.Data)
	}
	return gauge.DataPoints
}

func hasKey(set attribute.Set, key string) bool {
	_, ok := set.Value(attribute.Key(key))
	return ok
}

const plantedFragment = "# TYPE planted_total counter\nplanted_total{kind=\"a\",user_id=\"u-1\"} 3\nplanted_total{kind=\"b\",user_id=\"u-2\"} 4\n"

// The planted unbounded label reaches the exporter without a policy (the red control) and does not with one.
func TestAPlantedUnboundedLabelDoesNotReachTheExporter(t *testing.T) {
	open := sumPoints(t, collectExported(t, plantedFragment, nil)["planted_total"])
	if len(open) != 2 || !hasKey(open[0].Attributes, "user_id") {
		t.Fatalf("control: without a policy the unbounded label must reach the exporter, got %v", open)
	}
	policy := fragmentPolicy(t, "planted_total\tkind\tcode: test\n")
	points := sumPoints(t, collectExported(t, plantedFragment, policy)["planted_total"])
	if len(points) != 2 {
		t.Fatalf("points = %d, want 2 (kind a and b stay distinct)", len(points))
	}
	for _, p := range points {
		if hasKey(p.Attributes, "user_id") || !hasKey(p.Attributes, "kind") {
			t.Errorf("point attributes %v, want kind only", p.Attributes)
		}
	}
	if got := policy.Dropped()[[2]string{"planted_total", "user_id"}]; got != 2 {
		t.Errorf("dropped user_id = %d, want 2 (never the value: keys %v)", got, policy.Dropped())
	}
}

func TestAFamilyWithNoRowKeepsItsSamplesWithoutLabels(t *testing.T) {
	policy := fragmentPolicy(t, "")
	points := sumPoints(t, collectExported(t, "# TYPE lonely_total counter\nlonely_total{kind=\"a\"} 5\n", policy)["lonely_total"])
	if len(points) != 1 || points[0].Value != 5 || points[0].Attributes.Len() != 0 {
		t.Fatalf("points = %+v, want one sample of 5 with no labels", points)
	}
	if got := policy.Dropped()[[2]string{"lonely_total", "*"}]; got != 1 {
		t.Errorf("dropped (lonely_total,*) = %d, want 1: %v", got, policy.Dropped())
	}
}

func TestADroppedLabelNameThatIsNotEchoableCountsAsOther(t *testing.T) {
	policy := fragmentPolicy(t, "shouty_total\tkind\tcode: test\n")
	collectExported(t, "# TYPE shouty_total counter\nshouty_total{kind=\"a\",UserID=\"u\"} 1\n", policy)
	if got := policy.Dropped()[[2]string{"shouty_total", "other"}]; got != 1 {
		t.Errorf("dropped (shouty_total,other) = %d, want 1: %v", got, policy.Dropped())
	}
	for key := range policy.Dropped() {
		if strings.Contains(key[1], "UserID") {
			t.Errorf("the counter echoed a label name that is not [a-z0-9_.]{1,64}: %v", key)
		}
	}
}

func TestACounterCollisionSums(t *testing.T) {
	policy := fragmentPolicy(t, "merge_total\t\tcode: test\n")
	points := sumPoints(t, collectExported(t, "# TYPE merge_total counter\nmerge_total{id=\"a\"} 3\nmerge_total{id=\"b\"} 4\n", policy)["merge_total"])
	if len(points) != 1 || points[0].Value != 7 {
		t.Fatalf("points = %+v, want one sample of 7", points)
	}
	if got := policy.Dropped()[[2]string{"merge_total", "*"}]; got != 1 {
		t.Errorf("collisions counted = %d, want 1: %v", got, policy.Dropped())
	}
}

func TestAGaugeCollisionTakesTheMaximumAndSaysSo(t *testing.T) {
	policy := fragmentPolicy(t, "merge_ratio\t\tcode: test\n")
	exported := collectExported(t, "# HELP merge_ratio A ratio.\n# TYPE merge_ratio gauge\nmerge_ratio{id=\"a\"} 3\nmerge_ratio{id=\"b\"} 9\nmerge_ratio{id=\"c\"} 4\n", policy)["merge_ratio"]
	points := gaugePoints(t, exported)
	if len(points) != 1 || points[0].Value != 9 {
		t.Fatalf("points = %+v, want one sample of 9 (the maximum)", points)
	}
	if !strings.Contains(exported.Description, "report the maximum") {
		t.Errorf("description %q does not state the gauge rule", exported.Description)
	}
	if got := policy.Dropped()[[2]string{"merge_ratio", "*"}]; got != 2 {
		t.Errorf("collisions counted = %d, want 2 (two samples merged into the first): %v", got, policy.Dropped())
	}
}

func TestAHistogramCollisionSumsCountSumAndBuckets(t *testing.T) {
	policy := fragmentPolicy(t, "merge_seconds\t\tcode: test\n")
	text := "# TYPE merge_seconds histogram\n" +
		"merge_seconds_bucket{id=\"a\",le=\"1\"} 1\nmerge_seconds_bucket{id=\"a\",le=\"+Inf\"} 2\nmerge_seconds_sum{id=\"a\"} 3\nmerge_seconds_count{id=\"a\"} 2\n" +
		"merge_seconds_bucket{id=\"b\",le=\"1\"} 2\nmerge_seconds_bucket{id=\"b\",le=\"+Inf\"} 5\nmerge_seconds_sum{id=\"b\"} 8\nmerge_seconds_count{id=\"b\"} 5\n"
	histogram, ok := collectExported(t, text, policy)["merge_seconds"].Data.(metricdata.Histogram[float64])
	if !ok || len(histogram.DataPoints) != 1 {
		t.Fatalf("histogram = %+v", histogram)
	}
	point := histogram.DataPoints[0]
	if point.Count != 7 || point.Sum != 11 {
		t.Errorf("count %d sum %v, want 7 and 11", point.Count, point.Sum)
	}
	if len(point.BucketCounts) != 2 || point.BucketCounts[0] != 3 || point.BucketCounts[1] != 4 {
		t.Errorf("bucket counts %v, want [3 4] (cumulative 1+2 and 2+5 as 3 and 7)", point.BucketCounts)
	}
	if got := policy.Dropped()[[2]string{"merge_seconds", "*"}]; got != 1 {
		t.Errorf("collisions counted = %d, want 1: %v", got, policy.Dropped())
	}
}

func TestASummaryCollisionSumsCountAndSumAndDropsQuantiles(t *testing.T) {
	policy := fragmentPolicy(t, "merge_latency\t\tcode: test\n")
	text := "# TYPE merge_latency summary\n" +
		"merge_latency{id=\"a\",quantile=\"0.5\"} 1\nmerge_latency_sum{id=\"a\"} 3\nmerge_latency_count{id=\"a\"} 2\n" +
		"merge_latency{id=\"b\",quantile=\"0.5\"} 2\nmerge_latency_sum{id=\"b\"} 8\nmerge_latency_count{id=\"b\"} 5\n"
	exported := collectExported(t, text, policy)["merge_latency"]
	histogramLike, ok := exported.Data.(metricdata.Summary)
	if !ok || len(histogramLike.DataPoints) != 1 {
		t.Fatalf("data = %T %+v", exported.Data, exported.Data)
	}
	point := histogramLike.DataPoints[0]
	if point.Count != 7 || point.Sum != 11 || len(point.QuantileValues) != 0 {
		t.Errorf("count %d sum %v quantiles %v, want 7, 11 and none", point.Count, point.Sum, point.QuantileValues)
	}
	if got := policy.Dropped()[[2]string{"merge_latency", "*"}]; got != 1 {
		t.Errorf("collisions counted = %d, want 1: %v", got, policy.Dropped())
	}
}

func TestAListedFamilyWithItsLabelsOnlyIsUntouched(t *testing.T) {
	policy := fragmentPolicy(t, "planted_total\tkind,user_id\tcode: test\n")
	points := sumPoints(t, collectExported(t, plantedFragment, policy)["planted_total"])
	if len(points) != 2 || !hasKey(points[0].Attributes, "user_id") {
		t.Fatalf("points = %v, want both samples with their labels", points)
	}
	if len(policy.Dropped()) != 0 {
		t.Errorf("drops on an exact row: %v", policy.Dropped())
	}
}
