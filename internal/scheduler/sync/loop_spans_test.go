package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// installLoopSpanRecorder swaps the global tracer provider for an in-memory
// one. Not parallel: it changes process-global OTel state.
func installLoopSpanRecorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	return exporter
}

func spanIntAttr(t *testing.T, attrs []attribute.KeyValue, key string) int64 {
	t.Helper()
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value.AsInt64()
		}
	}
	t.Fatalf("attribute %s missing in %v", key, attrs)
	return 0
}

// CHAOS-7879: each window of the sync scheduler is one span carrying the
// window's counts; before this the scheduler binary emitted no span at all.
func TestLoopStepEmitsOneWindowSpanWithCounts(t *testing.T) {
	exporter := installLoopSpanRecorder(t)
	stepper := loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
		return HandoffResult{Candidates: 4, TimingEligible: 3, HandedOff: []Occurrence{{}, {}}, SkippedOrgMissing: 1}, nil
	})
	loop, _ := newTestLoop(t, stepper, &testLoopClock{now: time.Unix(1_700_000_000, 0)})
	if err := loop.step(context.Background(), time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "dev_health.scheduler.sync_window" {
		t.Fatalf("spans = %v, want exactly one dev_health.scheduler.sync_window", spans)
	}
	got := spans[0]
	for key, want := range map[string]int64{
		"dev_health.scheduler.candidates":          4,
		"dev_health.scheduler.timing_eligible":     3,
		"dev_health.scheduler.minted":              2,
		"dev_health.scheduler.handed_off":          2,
		"dev_health.scheduler.skipped":             1,
		"dev_health.scheduler.occurrences_retried": 0,
	} {
		if have := spanIntAttr(t, got.Attributes, key); have != want {
			t.Errorf("%s = %d, want %d", key, have, want)
		}
	}
	if got.Status.Code == codes.Error {
		t.Errorf("a clean window carries status Error")
	}
}

func TestLoopStepFailureIsAnErrorSpanAndShutdownCancelIsNot(t *testing.T) {
	exporter := installLoopSpanRecorder(t)
	failing := loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
		return HandoffResult{}, errors.New("planner down")
	})
	loop, _ := newTestLoop(t, failing, &testLoopClock{now: time.Unix(1_700_000_000, 0)})
	if err := loop.step(context.Background(), time.Unix(1_700_000_000, 0)); err == nil {
		t.Fatal("step returned nil for a failing stepper")
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error || len(spans[0].Events) == 0 {
		t.Fatalf("a failed window must be an Error span with an exception event: %v", spans)
	}

	exporter.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = loop.step(ctx, time.Unix(1_700_000_001, 0))
	spans = exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code == codes.Error {
		t.Fatalf("a shutdown cancel must not be an Error span: %v", spans)
	}
}
