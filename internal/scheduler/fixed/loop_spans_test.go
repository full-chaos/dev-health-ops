package fixed

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

func installFixedSpanRecorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	return exporter
}

type resultStepper struct {
	schedules []Schedule
	results   []ScheduleResult
}

func (stepper resultStepper) Step(_ context.Context, observedAt time.Time) (WindowResult, error) {
	return WindowResult{ObservedAt: observedAt, Schedules: stepper.results}, nil
}
func (stepper resultStepper) Schedules() []Schedule { return stepper.schedules }

func fixedAttr(attrs []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// CHAOS-7879: one window span, plus one child per schedule that decided or did
// something. The idle schedule (all zero) gets no child; a failed one does and
// is an Error span.
func TestFixedLoopStepEmitsWindowAndDecisionSpans(t *testing.T) {
	exporter := installFixedSpanRecorder(t)
	schedule := heartbeatSchedule(t)
	stepper := resultStepper{schedules: []Schedule{schedule}, results: []ScheduleResult{
		{ScheduleID: schedule.ID, Due: 2, Claimed: 1, Duplicate: 1, Handoffs: 1},
		{ScheduleID: "idle-schedule"},
		{ScheduleID: "broken-schedule", Err: errors.New("handoff failed")},
	}}
	loop, _ := newFixedTestLoop(t, stepper)
	_ = loop.step(context.Background(), mustTime(t, "2026-07-24T00:00:00Z"))

	var window, decided, failed int
	for _, span := range exporter.GetSpans() {
		switch span.Name {
		case "dev_health.scheduler.fixed_window":
			window++
			if v, ok := fixedAttr(span.Attributes, "dev_health.scheduler.handoffs"); !ok || v.AsInt64() != 1 {
				t.Errorf("window handoffs = %v (present %v), want 1", v, ok)
			}
			if v, ok := fixedAttr(span.Attributes, "dev_health.scheduler.failed"); !ok || v.AsInt64() != 1 {
				t.Errorf("window failed = %v (present %v), want 1", v, ok)
			}
		case "dev_health.scheduler.fixed_schedule":
			decided++
			name, _ := fixedAttr(span.Attributes, "dev_health.scheduler.schedule")
			if name.AsString() == "idle-schedule" {
				t.Errorf("an idle schedule got a span")
			}
			if name.AsString() == "broken-schedule" {
				failed++
				if span.Status.Code != codes.Error {
					t.Errorf("failed schedule span status = %v, want Error", span.Status.Code)
				}
			}
		}
	}
	if window != 1 || decided != 2 || failed != 1 {
		t.Fatalf("window=%d decided=%d failed=%d, want 1, 2, 1", window, decided, failed)
	}
}
