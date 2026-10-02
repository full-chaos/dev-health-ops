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

func spanAttrInts(attrs []attribute.KeyValue) map[string]int64 {
	out := map[string]int64{}
	for _, kv := range attrs {
		if kv.Value.Type() == attribute.INT64 {
			out[string(kv.Key)] = kv.Value.AsInt64()
		}
	}
	return out
}

// CHAOS-7879: each window of the sync scheduler is one span carrying the
// window's counts. Every count is a DISTINCT non-zero value so a dropped or
// swapped attribute cannot pass.
func TestLoopStepEmitsOneWindowSpanWithEveryCount(t *testing.T) {
	exporter := installLoopSpanRecorder(t)
	stepper := loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
		return HandoffResult{
			Candidates: 11, TimingEligible: 10,
			HandedOff: []Occurrence{{}, {}, {}}, Repeated: []Occurrence{{}},
			SkippedOrgMissing: 1, SkippedFeatureDisabled: 2, SkippedNotPlannerManaged: 4,
		}, nil
	})
	loop, _ := newTestLoop(t, stepper, &testLoopClock{now: time.Unix(1_700_000_000, 0)})
	loop.config.Occurrences = &stubOccurrences{result: OccurrenceReconcileResult{Completed: 5, Retried: 6, Quarantined: 7}}
	if err := loop.step(context.Background(), time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "dev_health.scheduler.sync_window" {
		t.Fatalf("spans = %v, want exactly one dev_health.scheduler.sync_window", spans)
	}
	got := spans[0]
	attrs := spanAttrInts(got.Attributes)
	for key, want := range map[string]int64{
		"dev_health.scheduler.candidates":              11,
		"dev_health.scheduler.timing_eligible":         10,
		"dev_health.scheduler.minted":                  2,
		"dev_health.scheduler.handed_off":              3,
		"dev_health.scheduler.repeated":                1,
		"dev_health.scheduler.skipped":                 7,
		"dev_health.scheduler.unsupported_cron":        0,
		"dev_health.scheduler.invalid_cron":            0,
		"dev_health.scheduler.occurrences_completed":   5,
		"dev_health.scheduler.occurrences_retried":     6,
		"dev_health.scheduler.occurrences_quarantined": 7,
	} {
		have, ok := attrs[key]
		if !ok || have != want {
			t.Errorf("%s = %d (present %v), want %d", key, have, ok, want)
		}
	}
	if got.Status.Code == codes.Error {
		t.Errorf("a clean window carries status Error")
	}
}

// A window that finds unsupported or invalid cron fails closed
// (ErrSchedulerFallbackRequired): its span is an Error span that still carries
// the two cron counts.
func TestLoopStepFallbackWindowIsAnErrorSpanWithCronCounts(t *testing.T) {
	exporter := installLoopSpanRecorder(t)
	stepper := loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
		return HandoffResult{UnsupportedCron: 8, InvalidCron: 9}, nil
	})
	loop, _ := newTestLoop(t, stepper, &testLoopClock{now: time.Unix(1_700_000_000, 0)})
	if err := loop.step(context.Background(), time.Unix(1_700_000_000, 0)); !errors.Is(err, ErrSchedulerFallbackRequired) {
		t.Fatalf("step = %v, want ErrSchedulerFallbackRequired", err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error {
		t.Fatalf("spans = %v, want one Error span", spans)
	}
	attrs := spanAttrInts(spans[0].Attributes)
	if attrs["dev_health.scheduler.unsupported_cron"] != 8 || attrs["dev_health.scheduler.invalid_cron"] != 9 {
		t.Errorf("cron counts = %v, want 8 and 9", attrs)
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
	if len(spans) != 1 || spans[0].Status.Code != codes.Error || len(spans[0].Events) != 1 {
		t.Fatalf("a failed window must be an Error span with one exception event: %v", spans)
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
