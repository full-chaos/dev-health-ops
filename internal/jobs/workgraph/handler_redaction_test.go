package workgraph

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const buildPermanentCancelMetric = "dev_health_workgraph_build_permanent_cancels_total"

var (
	counterReaderOnce sync.Once
	counterReader     *sdkmetric.ManualReader
)

// The global meter provider binds package-level instruments once, so the
// reader is installed once per process and the test reads a delta.
func sharedCounterReader() *sdkmetric.ManualReader {
	counterReaderOnce.Do(func() {
		counterReader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(counterReader)))
	})
	return counterReader
}

func buildCancelCount(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != buildPermanentCancelMetric {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric data %T, want Sum[int64]", m.Data)
			}
			for _, point := range sum.DataPoints {
				class, _ := point.Attributes.Value("error_class")
				if class.AsString() != "unclassified" {
					t.Fatalf("error_class label = %q", class.AsString())
				}
				total += point.Value
			}
		}
	}
	return total
}

func TestBuildPermanentCancelLogsNoErrorTextAndCounts(t *testing.T) {
	reader := sharedCounterReader()
	before := buildCancelCount(t, reader)

	const marker = "SECRET-MARKER-https://tenant.example/token=abc123"
	store := &fakeStore{claim: testClaim(time.Second)}
	step := failingPreStep{err: fmt.Errorf("bind failed: %s: %w", marker, errors.New(marker))}
	var logs bytes.Buffer
	handler, err := NewBuildHandler(store, []NativePreStep{step}, nil, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), buildExecution()); err == nil {
		t.Fatal("want a permanent failure")
	}

	out := logs.String()
	if !strings.Contains(out, "permanent cancel after step failure") {
		t.Fatalf("log line missing: %s", out)
	}
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("want a Warn line: %s", out)
	}
	if strings.Contains(out, "SECRET-MARKER") || strings.Contains(out, "abc123") {
		t.Fatalf("raw step error text reached the log: %s", out)
	}
	for _, want := range []string{"error_class=unclassified", "error_type="} {
		if !strings.Contains(out, want) {
			t.Fatalf("log missing %q: %s", want, out)
		}
	}

	if got := buildCancelCount(t, reader) - before; got != 1 {
		t.Fatalf("%s delta = %d, want 1", buildPermanentCancelMetric, got)
	}
}
