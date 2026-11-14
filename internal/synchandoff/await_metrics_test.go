package synchandoff

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	prometheusbridge "go.opentelemetry.io/contrib/bridges/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/platform/otlpmetrics"
)

func scrapeAwait(t *testing.T, m *awaitMetrics) string {
	t.Helper()
	var b strings.Builder
	if err := m.WritePrometheus(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func newAwaitMetrics() *awaitMetrics {
	return &awaitMetrics{count: map[string]uint64{}, sum: map[string]float64{}, buckets: map[string][]uint64{}}
}

// Every Wait state maps to the Python outcome label: a disabled PagerDuty sync
// (StateTerminal) is a completed occurrence there.
func TestAwaitOutcomeLabelMatchesPython(t *testing.T) {
	for state, want := range map[State]string{
		StateMaterialized: "materialized", StateTerminal: "materialized",
		StateQuarantined: "quarantined", StatePending: "pending",
	} {
		if got := awaitOutcomeLabel(state); got != want {
			t.Errorf("state %d -> %q, want %q", state, got, want)
		}
	}
}

// The exposition is prometheus_client's: cumulative buckets with the Python le
// text, _sum and _count per outcome, the counter under its _total name, and no
// series for an outcome that never happened.
func TestAwaitMetricsExposition(t *testing.T) {
	m := newAwaitMetrics()
	if got := scrapeAwait(t, m); strings.Contains(got, "{outcome=") {
		t.Fatalf("a series before any observation:\n%s", got)
	}
	m.observe(StateMaterialized, 300*time.Millisecond)
	m.observe(StateTerminal, 30*time.Millisecond)
	m.observe(StateQuarantined, 3*time.Second)
	m.observe(StatePending, 10*time.Second)
	got := scrapeAwait(t, m)
	for _, want := range []string{
		"# TYPE sync_manual_trigger_await_outcome_total counter\n",
		"# TYPE sync_manual_trigger_await_latency_seconds histogram\n",
		`sync_manual_trigger_await_outcome_total{outcome="materialized"} 2` + "\n",
		`sync_manual_trigger_await_outcome_total{outcome="quarantined"} 1` + "\n",
		`sync_manual_trigger_await_outcome_total{outcome="pending"} 1` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="0.05"} 1` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="0.25"} 1` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="0.5"} 2` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="+Inf"} 2` + "\n",
		`sync_manual_trigger_await_latency_seconds_count{outcome="materialized"} 2` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="quarantined",le="2.0"} 0` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="quarantined",le="5.0"} 1` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="pending",le="10.0"} 1` + "\n",
		`sync_manual_trigger_await_latency_seconds_bucket{outcome="pending",le="5.0"} 0` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Count(got, "_sum{") != 3 {
		t.Errorf("want one _sum per observed outcome:\n%s", got)
	}
}

// A nil receiver observes nothing (a caller built without metrics).
func TestAwaitMetricsNilReceiverIsSafe(t *testing.T) {
	var m *awaitMetrics
	m.observe(StatePending, time.Second)
}

// Every Python bucket bound and its le text is pinned: one outcome observed AT
// each bound and 1ms ABOVE each bound, then the ten bucket lines read back.
// Cumulative counts are 2i+1 for bound i (the AT observations of bounds 0..i and
// the ABOVE observations of bounds 0..i-1) and 18 for +Inf.
func TestAwaitBucketBoundsAndLeTextArePythons(t *testing.T) {
	m := newAwaitMetrics()
	bounds := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
		time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 20 * time.Second}
	for _, bound := range bounds {
		m.observe(StateMaterialized, bound)
		m.observe(StateMaterialized, bound+time.Millisecond)
	}
	got := scrapeAwait(t, m)
	labels := []string{"0.05", "0.1", "0.25", "0.5", "1.0", "2.0", "5.0", "10.0", "20.0"}
	for i, label := range labels {
		want := fmt.Sprintf(`sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="%s"} %d`+"\n", label, 2*i+1)
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if want := `sync_manual_trigger_await_latency_seconds_bucket{outcome="materialized",le="+Inf"} 18` + "\n"; !strings.Contains(got, want) {
		t.Errorf("missing %q in:\n%s", want, got)
	}
	if n := strings.Count(got, "_bucket{"); n != 10 {
		t.Errorf("want ten bucket lines, got %d", n)
	}
}

// observe and a scrape race-free: run under the race leg.
func TestAwaitMetricsObserveAndScrapeAreRaceFree(t *testing.T) {
	m := newAwaitMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				m.observe(StatePending, time.Millisecond)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				var b strings.Builder
				_ = m.WritePrometheus(&b)
			}
		}()
	}
	wg.Wait()
	if got := scrapeAwait(t, m); !strings.Contains(got, `sync_manual_trigger_await_outcome_total{outcome="pending"} 1600`+"\n") {
		t.Errorf("lost observations:\n%s", got)
	}
}

// The histogram sum adds every observation: it is not the last one. The durations
// are exact in float64 (0.25 + 0.5, 1 + 2), so the printed sums are exact.
func TestAwaitSumAddsEveryObservation(t *testing.T) {
	m := newAwaitMetrics()
	m.observe(StateMaterialized, 250*time.Millisecond)
	m.observe(StateMaterialized, 500*time.Millisecond)
	m.observe(StatePending, time.Second)
	m.observe(StatePending, 2*time.Second)
	m.observe(StateQuarantined, 4*time.Second)
	got := scrapeAwait(t, m)
	for _, want := range []string{
		`sync_manual_trigger_await_latency_seconds_sum{outcome="materialized"} 0.75` + "\n",
		`sync_manual_trigger_await_latency_seconds_sum{outcome="pending"} 3` + "\n",
		`sync_manual_trigger_await_latency_seconds_sum{outcome="quarantined"} 4` + "\n",
		`sync_manual_trigger_await_latency_seconds_count{outcome="materialized"} 2` + "\n",
		`sync_manual_trigger_await_latency_seconds_count{outcome="pending"} 2` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

// What the OTLP exporter is handed for the await latency histogram: the real writer, the real embedded labels.tsv, the real
// Gatherer and bridge. A row without "outcome" folds the three outcomes into one point without the attribute.
func TestAwaitLatencyReachesOTLPWithOneSeriesPerOutcome(t *testing.T) {
	m := newAwaitMetrics()
	for _, state := range []State{StateQuarantined, StatePending, StateTerminal} {
		m.observe(state, 120*time.Millisecond)
	}
	policy, err := otlpmetrics.DefaultLabelPolicy(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	gatherer := &otlpmetrics.Gatherer{Source: awaitFragment(scrapeAwait(t, m)), Policy: policy}
	reader := sdkmetric.NewManualReader(sdkmetric.WithProducer(prometheusbridge.NewMetricProducer(prometheusbridge.WithGatherer(gatherer))))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != awaitLatencyMetric {
				continue
			}
			histogram, ok := metric.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s is not exported as a float histogram: %+v", metric.Name, metric)
			}
			for _, point := range histogram.DataPoints {
				value, _ := point.Attributes.Value("outcome")
				got = append(got, value.AsString())
			}
		}
	}
	sort.Strings(got)
	if want := "materialized,pending,quarantined"; strings.Join(got, ",") != want {
		t.Errorf("%s exports %d points with outcome values %q, want %s", awaitLatencyMetric, len(got), got, want)
	}
}

type awaitFragment string

func (f awaitFragment) EachMetricsFragment(_ map[string]bool, fn func(string, []byte, error)) {
	fn("await", []byte(f), nil)
}
