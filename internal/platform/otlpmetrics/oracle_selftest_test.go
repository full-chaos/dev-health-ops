package otlpmetrics

import (
	"fmt"
	"math"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// The differential oracle (compare, collect, floatsEqual) is test code that
// decides whether a push is faithful. These tests are its own: each defect it
// used to be blind to (CHAOS-7661) must now make it FAIL.

type recorder struct{ errors []string }

func (*recorder) Helper() {}
func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func labelsOf(kv ...string) []*commonpb.KeyValue {
	var out []*commonpb.KeyValue
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, &commonpb.KeyValue{Key: kv[i], Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: kv[i+1]}}})
	}
	return out
}

func counterPoint(value float64, kv ...string) *metricpb.NumberDataPoint {
	return &metricpb.NumberDataPoint{Attributes: labelsOf(kv...), Value: &metricpb.NumberDataPoint_AsDouble{AsDouble: value}}
}

func counterMetric(name string, points ...*metricpb.NumberDataPoint) *metricpb.Metric {
	return &metricpb.Metric{Name: name, Data: &metricpb.Metric_Sum{Sum: &metricpb.Sum{
		IsMonotonic: true, AggregationTemporality: metricpb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, DataPoints: points,
	}}}
}

func exportOf(metrics ...*metricpb.Metric) *metricpb.ResourceMetrics {
	return &metricpb.ResourceMetrics{ScopeMetrics: []*metricpb.ScopeMetrics{{Metrics: metrics}}}
}

const oneCounterScrape = "# TYPE jobs_total counter\njobs_total{outcome=\"ok\"} 5\n"

func TestCompareAcceptsOnePointPerSeriesPerExportAcrossTwoExports(t *testing.T) {
	// A push is flushed and then shut down: the same series arrives once in each
	// export. That is faithful, and the oracle must not call it a duplicate.
	var r recorder
	families, series := compare(&r, "self", parseExposition(t, oneCounterScrape), []*metricpb.ResourceMetrics{
		exportOf(counterMetric("jobs_total", counterPoint(5, "outcome", "ok"))),
		exportOf(counterMetric("jobs_total", counterPoint(5, "outcome", "ok"))),
	})
	if len(r.errors) != 0 || families != 1 || series != 1 {
		t.Fatalf("a faithful two-export push was rejected or miscounted: errors=%v families=%d series=%d", r.errors, families, series)
	}
}

func TestCompareFailsOnADuplicatePointInOneExport(t *testing.T) {
	// Two identical points for one scrape series in ONE export used to collapse
	// in collect()'s per-label map and pass.
	var r recorder
	compare(&r, "self", parseExposition(t, oneCounterScrape), []*metricpb.ResourceMetrics{
		exportOf(counterMetric("jobs_total", counterPoint(5, "outcome", "ok"), counterPoint(5, "outcome", "ok"))),
	})
	if len(r.errors) == 0 || !strings.Contains(strings.Join(r.errors, "\n"), "delivered 2 times") {
		t.Fatalf("a duplicate delivery of one series passed the oracle: %v", r.errors)
	}
}

func TestCompareFailsOnTheSameFamilyEmittedTwiceInOneExport(t *testing.T) {
	var r recorder
	compare(&r, "self", parseExposition(t, oneCounterScrape), []*metricpb.ResourceMetrics{
		exportOf(
			counterMetric("jobs_total", counterPoint(5, "outcome", "ok")),
			counterMetric("jobs_total", counterPoint(5, "outcome", "ok")),
		),
	})
	if len(r.errors) == 0 {
		t.Fatalf("one family emitted twice in one export passed the oracle")
	}
}

func TestFloatsEqualInfinities(t *testing.T) {
	inf, finite := math.Inf(1), 3.5
	for name, tc := range map[string]struct {
		a, b float64
		want bool
	}{
		"+Inf vs finite":        {inf, finite, false},
		"finite vs +Inf":        {finite, inf, false},
		"-Inf vs finite":        {math.Inf(-1), finite, false},
		"+Inf vs +Inf":          {inf, inf, true},
		"+Inf vs -Inf":          {inf, math.Inf(-1), false},
		"+Inf vs a huge finite": {inf, math.MaxFloat64, false},
		"NaN vs NaN":            {math.NaN(), math.NaN(), true},
		"NaN vs finite":         {math.NaN(), finite, false},
		"equal finite":          {finite, finite, true},
		"within tolerance":      {1e9, 1e9 + 0.5, true},
		"outside tolerance":     {1, 1.1, false},
	} {
		if got := floatsEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: floatsEqual(%v, %v) = %v, want %v", name, tc.a, tc.b, got, tc.want)
		}
	}
}

func histogramPoint(kv ...string) *metricpb.HistogramDataPoint {
	return &metricpb.HistogramDataPoint{
		Attributes: labelsOf(kv...), Count: 10, Sum: func() *float64 { v := 3.5; return &v }(),
		ExplicitBounds: []float64{0.1, 1}, BucketCounts: []uint64{4, 5, 1},
	}
}

func histogramMetric(name string, points ...*metricpb.HistogramDataPoint) *metricpb.Metric {
	return &metricpb.Metric{Name: name, Data: &metricpb.Metric_Histogram{Histogram: &metricpb.Histogram{
		AggregationTemporality: metricpb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, DataPoints: points,
	}}}
}

const oneHistogramScrape = "# TYPE wait_seconds histogram\nwait_seconds_bucket{le=\"0.1\"} 4\nwait_seconds_bucket{le=\"1\"} 9\nwait_seconds_bucket{le=\"+Inf\"} 10\nwait_seconds_sum 3.5\nwait_seconds_count 10\n"

func TestCompareFailsOnADuplicateHistogramPointInOneExport(t *testing.T) {
	var r recorder
	compare(&r, "self", parseExposition(t, oneHistogramScrape), []*metricpb.ResourceMetrics{
		exportOf(histogramMetric("wait_seconds", histogramPoint(), histogramPoint())),
	})
	if len(r.errors) == 0 || !strings.Contains(strings.Join(r.errors, "\n"), "delivered 2 times") {
		t.Fatalf("a duplicate histogram point passed the oracle: %v", r.errors)
	}
	// and a clean histogram, flushed twice, is accepted
	var clean recorder
	compare(&clean, "self", parseExposition(t, oneHistogramScrape), []*metricpb.ResourceMetrics{
		exportOf(histogramMetric("wait_seconds", histogramPoint())),
		exportOf(histogramMetric("wait_seconds", histogramPoint())),
	})
	if len(clean.errors) != 0 {
		t.Fatalf("a faithful two-export histogram was rejected: %v", clean.errors)
	}
}

func TestADuplicateInTheFirstExportIsNotForgottenByACleanSecondExport(t *testing.T) {
	var r recorder
	compare(&r, "self", parseExposition(t, oneCounterScrape), []*metricpb.ResourceMetrics{
		exportOf(counterMetric("jobs_total", counterPoint(5, "outcome", "ok"), counterPoint(5, "outcome", "ok"))),
		exportOf(counterMetric("jobs_total", counterPoint(5, "outcome", "ok"))),
	})
	if len(r.errors) == 0 {
		t.Fatalf("the clean second export hid the first export's duplicate")
	}
}

func gaugeMetric(name string, points ...*metricpb.NumberDataPoint) *metricpb.Metric {
	return &metricpb.Metric{Name: name, Data: &metricpb.Metric_Gauge{Gauge: &metricpb.Gauge{DataPoints: points}}}
}

const oneGaugeScrape = "# TYPE pool_in_use gauge\npool_in_use{pool=\"queue\"} 3\n"

func TestCompareFailsOnADuplicateGaugePointInOneExport(t *testing.T) {
	var r recorder
	compare(&r, "self", parseExposition(t, oneGaugeScrape), []*metricpb.ResourceMetrics{
		exportOf(gaugeMetric("pool_in_use", counterPoint(3, "pool", "queue"), counterPoint(3, "pool", "queue"))),
	})
	if len(r.errors) == 0 || !strings.Contains(strings.Join(r.errors, "\n"), "delivered 2 times") {
		t.Fatalf("a duplicate gauge point passed the oracle: %v", r.errors)
	}
	var clean recorder
	compare(&clean, "self", parseExposition(t, oneGaugeScrape), []*metricpb.ResourceMetrics{
		exportOf(gaugeMetric("pool_in_use", counterPoint(3, "pool", "queue"))),
		exportOf(gaugeMetric("pool_in_use", counterPoint(3, "pool", "queue"))),
	})
	if len(clean.errors) != 0 {
		t.Fatalf("a faithful two-export gauge was rejected: %v", clean.errors)
	}
}
