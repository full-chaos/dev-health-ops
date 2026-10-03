package analytics

import (
	"context"
	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestDefaultRecordDegradation_RecordsDriverCause exercises
// defaultRecordDegradation ITSELF.
//
// This test exists because of a removal check that came back green when
// it should have gone red. TestResolve_FlowMatrixDegradation_IsReported
// swaps recordDegradation for a capturing stub -- which is the right way
// to observe that the swallow reports -- but that stub REPLACES the very
// function whose body records error.cause. Deleting the error.cause
// attribute therefore left that test passing: it asserts on the error
// handed TO the recorder, never on what the recorder does with it.
//
// That is the same layer-masking shape found twice already in this port,
// this time hiding a telemetry fix rather than a query guard: the
// injection seam that makes one behaviour testable makes the behaviour
// BEHIND it untestable, so it needs a test at its own level.
func TestDefaultRecordDegradation_RecordsIdentityNotText(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	ctx, span := provider.Tracer("test").Start(context.Background(), "resolve")

	// Real client shape: fixed Error(), cause only via Unwrap(). The driver message quotes a table and a value: none of that
	// text may reach the span (CHAOS-7936); the exception's code and name do (they tell a missing table from a timeout).
	driverErr := &clickhousedriver.Exception{Code: 60, Name: "UNKNOWN_TABLE", Message: "Table default.planted_table_7936 does not exist for the planted detail of ticket 7936"}
	wrapped := &fakeOperationError{operation: "query", cause: driverErr}

	defaultRecordDegradation(ctx, "flowMatrix", wrapped)
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 recorded span, got %d", len(ended))
	}
	events := ended[0].Events()
	if len(events) != 1 || events[0].Name != "analytics.degraded" {
		t.Fatalf("expected one analytics.degraded event, got %+v", events)
	}

	attrs := map[string]string{}
	for _, a := range events[0].Attributes {
		attrs[string(a.Key)] = a.Value.Emit()
	}
	want := map[string]string{
		"phase":                "flowMatrix",
		"error.class":          "other",
		"error.type":           "*proto.Exception",
		"clickhouse_code":      "60",
		"clickhouse_exception": "UNKNOWN_TABLE",
	}
	for key, value := range want {
		if attrs[key] != value {
			t.Fatalf("attribute %s = %q, want %q (attributes: %v)", key, attrs[key], value, attrs)
		}
	}
	for key, value := range attrs {
		if key == "error" || key == "error.cause" || strings.Contains(value, "planted") || strings.Contains(value, "ClickHouse query failed") {
			t.Fatalf("attribute %s = %q carries error text", key, value)
		}
	}
}
