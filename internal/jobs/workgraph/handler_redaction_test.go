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

	"github.com/jackc/pgx/v5/pgconn"
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

func TestStepFailureReasonStaysOutOfTheBuildLog(t *testing.T) {
	const marker = "SECRET-MARKER-https://tenant.example/token=abc123"
	var logs bytes.Buffer
	store := &fakeStore{claim: testClaim(time.Second)}
	step := failingPreStep{err: &StepFailure{Reason: marker, Evidence: map[string]any{"links_read": 4}}}
	handler, err := NewBuildHandler(store, []NativePreStep{step}, nil, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), buildExecution()); err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if !strings.Contains(out, "steps reported FAILED") || !strings.Contains(out, "links_read") {
		t.Fatalf("failed-step line or its counts missing: %s", out)
	}
	if strings.Contains(out, "SECRET-MARKER") || strings.Contains(out, "abc123") {
		t.Fatalf("step failure reason reached the log: %s", out)
	}
	if !strings.Contains(string(store.lastEvidence), "SECRET-MARKER") {
		t.Fatalf("the ledger evidence lost failure_reason: %s", store.lastEvidence)
	}
}

func TestErrorAttrNamesTheRootTypeThroughAJoin(t *testing.T) {
	joined := errors.Join(&pgconn.PgError{Code: "40001", Message: "SECRET-MARKER"})
	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Error("probe", ErrorAttr(joined))
	out := logs.String()
	if !strings.Contains(out, "error_type=*pgconn.PgError") || !strings.Contains(out, "sqlstate=40001") {
		t.Fatalf("join root not resolved: %s", out)
	}
	if strings.Contains(out, "SECRET-MARKER") {
		t.Fatalf("error text reached the log: %s", out)
	}
}
