package jobruntime

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestStartJobSpanExtractsEnvelopeTraceParent installs the global
// TracerProvider exactly once for both cases it checks. otel's global
// package permanently upgrades a Tracer handle to the first real provider it
// sees and does not re-delegate on a later SetTracerProvider call, so a
// second, separate installation in a sibling test would silently route
// startJobSpan's package-level jobTracer's spans to the FIRST test's
// provider instead of its own -- sharing one installation avoids that
// entirely. It intentionally does not call t.Parallel(): it swaps
// process-global OTel state for its duration, and Go's testing runner
// completes every non-parallel test in a package before letting any
// t.Parallel() test resume past its own call, so this does not race with the
// rest of the package's (parallel) suite.
func TestStartJobSpanExtractsEnvelopeTraceParent(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	descriptor := Descriptor{Kind: "sync.provider_unit", Queue: "sync"}

	t.Run("with trace_parent, the span gets a remote parent from it", func(t *testing.T) {
		exporter.Reset()
		envelope := jobcontract.Envelope{
			CorrelationID: "corr-1",
			Domain:        jobcontract.DomainLink{Type: "sync_run_unit", ID: "00000000-0000-4000-8000-000000000001"},
			// CHAOS-3993: captured by the Python producer at outbox-enqueue time.
			TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		}

		_, span := startJobSpan(context.Background(), descriptor, 42, 1, envelope)
		span.End()

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 exported span, got %d", len(spans))
		}
		got := spans[0]
		if traceID := got.SpanContext.TraceID().String(); traceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Fatalf("trace id = %s, want the trace id carried by envelope.TraceParent", traceID)
		}
		if !got.Parent.IsRemote() {
			t.Fatal("expected the span's parent to be remote, extracted from envelope.TraceParent")
		}
		if parentSpanID := got.Parent.SpanID().String(); parentSpanID != "00f067aa0ba902b7" {
			t.Fatalf("parent span id = %s, want the span id carried by envelope.TraceParent", parentSpanID)
		}
	})

	t.Run("without trace_parent, the span is a root span", func(t *testing.T) {
		exporter.Reset()
		envelope := jobcontract.Envelope{
			CorrelationID: "corr-2",
			Domain:        jobcontract.DomainLink{Type: "sync_run_unit", ID: "00000000-0000-4000-8000-000000000002"},
			// No TraceParent: a producer that predates the field, or tracing off.
		}

		_, span := startJobSpan(context.Background(), descriptor, 43, 1, envelope)
		span.End()

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 exported span, got %d", len(spans))
		}
		if spans[0].Parent.IsValid() {
			t.Fatal("expected a root span (no parent) when envelope carries no trace_parent")
		}
	})
}
