package syncdispatchruntime

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type panickingBridge struct{ recordingBridge }

// TeamAutoImport is the panic seam exercised below: teamAutoimportWorker is
// the last coordinator worker still holding a CoordinatorBridge field
// (CHAOS-4175: dispatch_sync_run, finalize_sync_run, and
// run_sync_reference_discovery are all native now, so their own workers no
// longer have a bridge call to panic through).
func (*panickingBridge) TeamAutoImport(context.Context, DomainReference) error {
	panic("bridge exploded")
}

// TestCoordinatorSpanEndsEvenWhenTheBridgePanics does not call t.Parallel():
// it swaps the process-global TracerProvider for its duration, and Go's
// testing runner completes every non-parallel test in a package before
// letting any t.Parallel() test resume past its own call (see
// internal/jobruntime.TestStartJobSpanExtractsEnvelopeTraceParent for the
// same reasoning). Without the deferred finish in dispatchWorker.Work, a
// panic here would skip finishCoordinatorSpan entirely: the span would never
// be ended, so it would never export, silently losing the very failure a
// span exists to make visible.
func TestCoordinatorSpanEndsEvenWhenTheBridgePanics(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	worker := &teamAutoimportWorker{bridge: &panickingBridge{}}
	teamArgs := TeamAutoimportJobArgs{
		Version:       ContractVersionV1,
		OrgID:         testOrg,
		CorrelationID: "post-sync-" + testRun,
		Idempotency:   "post-sync:" + testRun + ":team_autoimport",
		Domain:        jobcontract.DomainLink{Type: "sync_run", ID: testRun},
		Payload:       jobcontract.TeamAutoimportPayload{SyncRunID: testRun},
	}

	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected the panic to propagate out of Work, not be swallowed")
			}
		}()
		_ = worker.Work(context.Background(), &river.Job[TeamAutoimportJobArgs]{Args: teamArgs})
	}()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected the span to be ended and exported despite the panic, got %d spans", len(spans))
	}
}

// TestSpanForCoordinatorJobExtractsTraceParent does not call t.Parallel() for
// the same global-state reason as TestCoordinatorSpanEndsEvenWhenTheBridgePanics.
func TestSpanForCoordinatorJobExtractsTraceParent(t *testing.T) {
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

	t.Run("with trace_parent, the span gets a remote parent from it", func(t *testing.T) {
		exporter.Reset()
		_, span := spanForCoordinatorJob(context.Background(), "dispatch_sync_run", testRun,
			"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
		span.End()

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 exported span, got %d", len(spans))
		}
		got := spans[0]
		if traceID := got.SpanContext.TraceID().String(); traceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Fatalf("trace id = %s, want the trace id carried by traceParent", traceID)
		}
		if !got.Parent.IsRemote() {
			t.Fatal("expected the span's parent to be remote, extracted from traceParent")
		}
	})

	t.Run("without trace_parent, the span is a root span", func(t *testing.T) {
		exporter.Reset()
		_, span := spanForCoordinatorJob(context.Background(), "dispatch_sync_run", testRun, "")
		span.End()

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 exported span, got %d", len(spans))
		}
		if spans[0].Parent.IsValid() {
			t.Fatal("expected a root span (no parent) when traceParent is empty")
		}
	})
}
