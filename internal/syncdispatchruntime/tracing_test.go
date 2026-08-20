package syncdispatchruntime

import (
	"context"
	"testing"

	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type panickingBridge struct{ recordingBridge }

func (*panickingBridge) Dispatch(context.Context, DispatchSyncRunArgs) error {
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

	base := TransportArgs{Version: ContractVersionV1, OrgID: testOrg, RunID: testRun, DispatchOutbox: testOutbox, RouteGeneration: 1}
	worker := &dispatchWorker{bridge: &panickingBridge{}}

	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected the panic to propagate out of Work, not be swallowed")
			}
		}()
		_ = worker.Work(context.Background(), &river.Job[DispatchSyncRunArgs]{Args: DispatchSyncRunArgs{TransportArgs: base}})
	}()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected the span to be ended and exported despite the panic, got %d spans", len(spans))
	}
}
