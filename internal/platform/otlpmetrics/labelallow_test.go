package otlpmetrics

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const testTable = "instrument\tlabels\treason\n" +
	"test_requests_total\toutcome,reason\tfixture\n" +
	DroppedMetricName + "\tinstrument,label\town counter\n"

func newPolicyProvider(t *testing.T, table string, logs *bytes.Buffer, readers ...sdkmetric.Reader) (*LabelPolicy, *sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	policy, err := NewLabelPolicy([]byte(table), slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	manual := sdkmetric.NewManualReader()
	options := []sdkmetric.Option{sdkmetric.WithReader(manual), sdkmetric.WithView(policy.View())}
	for _, reader := range readers {
		options = append(options, sdkmetric.WithReader(reader))
	}
	provider := sdkmetric.NewMeterProvider(options...)
	if err := policy.Register(provider.Meter("test")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return policy, provider, manual
}

func collectLabels(t *testing.T, reader sdkmetric.Reader) metricdata.ResourceMetrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	return data
}

func sums(t *testing.T, data metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				return d.DataPoints
			}
		}
	}
	return nil
}

func keysOf(point metricdata.DataPoint[int64]) []string {
	var keys []string
	for _, kv := range point.Attributes.ToSlice() {
		keys = append(keys, string(kv.Key))
	}
	return keys
}

// Synthetic values only: none of these strings may appear anywhere in what the reader hands to an exporter.
var plantedValues = []string{"9f1c2d3e-0000-4000-8000-000000000001", "synthetic.person@example.invalid", "/api/v1/orgs/synthetic-org/secret-path", "synthetic failure text: connection reset"}

func TestAnUnboundedLabelNeverReachesTheReader(t *testing.T) {
	var logs bytes.Buffer
	policy, provider, manual := newPolicyProvider(t, testTable, &logs)
	counter, _ := provider.Meter("test").Int64Counter("test_requests_total")
	ctx := context.Background()
	counter.Add(ctx, 2, metric.WithAttributes(attribute.String("outcome", "ok"), attribute.String("reason", "none"),
		attribute.String("request_id", plantedValues[0]), attribute.String("email", plantedValues[1]),
		attribute.String("path", plantedValues[2]), attribute.String("error", plantedValues[3])))
	counter.Add(ctx, 3, metric.WithAttributes(attribute.String("outcome", "ok"), attribute.String("reason", "none"),
		attribute.String("request_id", "9f1c2d3e-0000-4000-8000-000000000002")))
	data := collectLabels(t, manual)
	points := sums(t, data, "test_requests_total")
	if len(points) != 1 || points[0].Value != 5 {
		t.Fatalf("data points %+v, want one point of value 5: the dropped dimension must aggregate", points)
	}
	if got := strings.Join(keysOf(points[0]), ","); got != "outcome,reason" {
		t.Fatalf("exported keys %q, want outcome,reason", got)
	}
	if rendered := fmt.Sprintf("%+v", data); containsAny(rendered, append(plantedValues, "9f1c2d3e-0000-4000-8000-000000000002")) {
		t.Fatal("a planted value reached the reader")
	}
	dropped := policy.Dropped()
	for _, label := range []string{"request_id", "email", "path", "error"} {
		if dropped[[2]string{"test_requests_total", label}] == 0 {
			t.Fatalf("drop of %s not counted: %v", label, dropped)
		}
	}
	// The counter itself is exported through the same provider and echoes names only.
	counted := sums(t, collectLabels(t, manual), DroppedMetricName)
	if len(counted) == 0 {
		t.Fatal("the drop counter reached no reader")
	}
	if rendered := fmt.Sprintf("%+v", counted); containsAny(rendered, plantedValues) {
		t.Fatal("a value reached the drop counter")
	}
}

func containsAny(text string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func TestAnUnlistedInstrumentKeepsItsDataAndLosesEveryLabel(t *testing.T) {
	var logs bytes.Buffer
	policy, provider, manual := newPolicyProvider(t, testTable, &logs)
	counter, _ := provider.Meter("test").Int64Counter("test_unlisted_total")
	for i := 0; i < 3; i++ {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", "ok"), attribute.String("request_id", plantedValues[0])))
	}
	points := sums(t, collectLabels(t, manual), "test_unlisted_total")
	if len(points) != 1 || points[0].Value != 3 || len(keysOf(points[0])) != 0 {
		t.Fatalf("data points %+v, want one label-free point of value 3", points)
	}
	if policy.Dropped()[[2]string{"test_unlisted_total", "*"}] == 0 {
		t.Fatalf("unlisted drop not counted under *: %v", policy.Dropped())
	}
	if n := strings.Count(logs.String(), "test_unlisted_total"); n != 1 {
		t.Fatalf("the unlisted instrument warned %d times, want once:\n%s", n, logs.String())
	}
	if strings.Contains(logs.String(), plantedValues[0]) {
		t.Fatal("a value reached the log")
	}
}

func TestTheDropCounterEchoesOnlyNameShapedKeys(t *testing.T) {
	var logs bytes.Buffer
	policy, provider, manual := newPolicyProvider(t, testTable, &logs)
	counter, _ := provider.Meter("test").Int64Counter("test_requests_total")
	planted := []string{
		"Bearer_synthetic0000000000000000000000",
		"token synthetic with spaces",
		"UPPER_CASE_KEY",
		strings.Repeat("a", 65),
		"synthetic@example.invalid",
		"",
	}
	for _, key := range planted {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", "ok"), attribute.String(key, "v")))
	}
	dropped := policy.Dropped()
	if dropped[[2]string{"test_requests_total", "other"}] == 0 {
		t.Fatalf("shaped-wrong keys not counted as other: %v", dropped)
	}
	for key := range dropped {
		if !instrumentPattern.MatchString(key[0]) || (key[1] != "*" && !echoablePattern.MatchString(key[1])) {
			t.Fatalf("drop series %v echoes a name outside [a-z0-9_.]{1,64}", key)
		}
	}
	rendered := fmt.Sprintf("%+v", collectLabels(t, manual))
	for _, key := range planted[:5] {
		if strings.Contains(rendered, key) {
			t.Fatalf("key %q reached a series", key)
		}
	}
}

func TestTheDropCounterIsBounded(t *testing.T) {
	var logs bytes.Buffer
	policy, provider, _ := newPolicyProvider(t, testTable, &logs)
	counter, _ := provider.Meter("test").Int64Counter("test_requests_total")
	for i := 0; i < maxDropSeries+50; i++ {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", "ok"), attribute.String(fmt.Sprintf("dynamic_key_%d", i), "v")))
	}
	dropped := policy.Dropped()
	if len(dropped) > maxDropSeries+1 {
		t.Fatalf("%d drop series, want at most %d plus the overflow bucket", len(dropped), maxDropSeries)
	}
	if dropped[[2]string{otherName, otherName}] == 0 {
		t.Fatal("overflow not counted under other")
	}
}

type captureExporter struct {
	mu   sync.Mutex
	data []metricdata.ResourceMetrics
}

func (e *captureExporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}
func (e *captureExporter) Aggregation(sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.AggregationDefault{}
}
func (e *captureExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data = append(e.data, *rm)
	return nil
}
func (e *captureExporter) ForceFlush(context.Context) error { return nil }
func (e *captureExporter) Shutdown(context.Context) error   { return nil }

func TestAnUnboundedLabelNeverReachesThePeriodicExporter(t *testing.T) {
	var logs bytes.Buffer
	exporter := &captureExporter{}
	periodic := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Hour))
	_, provider, _ := newPolicyProvider(t, testTable, &logs, periodic)
	counter, _ := provider.Meter("test").Int64Counter("test_requests_total")
	counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", "ok"), attribute.String("email", plantedValues[1])))
	if err := periodic.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if len(exporter.data) == 0 {
		t.Fatal("the exporter received nothing")
	}
	if containsAny(fmt.Sprintf("%+v", exporter.data), plantedValues) || strings.Contains(fmt.Sprintf("%+v", exporter.data), `"email"`) {
		t.Fatal("a planted label reached the exporter")
	}
}

func TestLabelTableRefusals(t *testing.T) {
	for name, table := range map[string]string{
		"duplicate row":      "a_total\tx\tr\na_total\ty\tr\n",
		"unsorted keys":      "a_total\ty,x\tr\n",
		"duplicate key":      "a_total\tx,x\tr\n",
		"key shape":          "a_total\tBad-Key\tr\n",
		"instrument shape":   "A Total\tx\tr\n",
		"seven keys":         "a_total\ta,b,c,d,e,f,g\tfirst-party\n",
		"missing reason col": "a_total\tx\n",
	} {
		if _, err := NewLabelPolicy([]byte(table), nil); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := NewLabelPolicy([]byte("a_total\ta,b,c,d,e,f,g\tthird-party: library\n"), nil); err != nil {
		t.Errorf("a third-party row over the budget was refused: %v", err)
	}
}

func TestTheEmbeddedTableParses(t *testing.T) {
	policy, err := DefaultLabelPolicy(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !policy.Listed(DroppedMetricName) {
		t.Fatal("the drop counter has no row")
	}
}
