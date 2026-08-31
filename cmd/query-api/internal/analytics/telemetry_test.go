package analytics

import (
	"context"
	"errors"
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
func TestDefaultRecordDegradation_RecordsDriverCause(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	ctx, span := provider.Tracer("test").Start(context.Background(), "resolve")

	// Real client shape: fixed Error(), cause only via Unwrap().
	driverErr := errors.New("code: 60, message: Table default.work_item_cycle_times does not exist")
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
		attrs[string(a.Key)] = a.Value.AsString()
	}

	if attrs["phase"] != "flowMatrix" {
		t.Fatalf("phase = %q, want flowMatrix", attrs["phase"])
	}
	// The defect this guards: against the real client, "error" alone is
	// the fixed "ClickHouse query failed" for EVERY failure.
	if !strings.Contains(attrs["error"], "ClickHouse query failed") {
		t.Fatalf("error attr = %q, want the wrapper text", attrs["error"])
	}
	cause, ok := attrs["error.cause"]
	if !ok {
		t.Fatal("no error.cause attribute -- telemetry records only the fixed wrapper string and cannot distinguish a missing table from a timeout")
	}
	if !strings.Contains(cause, "code: 60") || !strings.Contains(cause, "work_item_cycle_times") {
		t.Fatalf("error.cause = %q, want the driver message with code and table", cause)
	}
}
