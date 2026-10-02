package fixed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"log/slog"
	"strings"
	"sync"
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

func fixedInts(attrs []attribute.KeyValue) map[string]int64 {
	out := map[string]int64{}
	for _, kv := range attrs {
		if kv.Value.Type() == attribute.INT64 {
			out[string(kv.Key)] = kv.Value.AsInt64()
		}
	}
	return out
}

// CHAOS-7879: one window span, plus one child per schedule that decided or did
// something, parented on the window. Every count is distinct and non-zero so a
// dropped or swapped attribute cannot pass; the idle schedule gets no child; a
// failed one gets an Error child and the window an Error status.
func TestFixedLoopStepEmitsWindowAndDecisionSpans(t *testing.T) {
	exporter := installFixedSpanRecorder(t)
	schedule := heartbeatSchedule(t)
	stepper := resultStepper{schedules: []Schedule{schedule}, results: []ScheduleResult{
		{ScheduleID: schedule.ID, Due: 2, Claimed: 3, Duplicate: 4, Handoffs: 5, Skipped: 6, ColdStart: true, StaleSkipped: true},
		{ScheduleID: "idle-schedule"},
		{ScheduleID: "broken-schedule", Err: errors.New("handoff failed {payload-in-error}")},
	}}
	loop, _ := newFixedTestLoop(t, stepper)
	if err := loop.step(context.Background(), mustTime(t, "2026-07-24T00:00:00Z")); err == nil {
		t.Fatal("a window with a failed schedule returned nil")
	}

	var window tracetest.SpanStub
	var children []tracetest.SpanStub
	for _, span := range exporter.GetSpans() {
		switch span.Name {
		case "dev_health.scheduler.fixed_window":
			window = span
		case "dev_health.scheduler.fixed_schedule":
			children = append(children, span)
		}
	}
	if window.Name == "" || len(children) != 2 {
		t.Fatalf("window %q, %d children, want one window and 2 children (idle schedule has none)", window.Name, len(children))
	}
	if window.Status.Code != codes.Error {
		t.Errorf("a window with a failed schedule must be an Error span")
	}
	if stage, _ := fixedAttr(window.Attributes, "dev_health.work.stage"); stage.AsString() != "schedule" {
		t.Errorf("window stage = %q, want schedule", stage.AsString())
	}
	wantWindow := map[string]int64{
		"dev_health.scheduler.schedules": 3, "dev_health.scheduler.due": 2, "dev_health.scheduler.claimed": 3,
		"dev_health.scheduler.duplicate": 4, "dev_health.scheduler.handoffs": 5, "dev_health.scheduler.skipped": 6,
		"dev_health.scheduler.failed": 1,
	}
	have := fixedInts(window.Attributes)
	for key, want := range wantWindow {
		if have[key] != want {
			t.Errorf("window %s = %d, want %d", key, have[key], want)
		}
	}
	var sawBroken, sawDecided bool
	for _, child := range children {
		if child.Parent.SpanID() != window.SpanContext.SpanID() {
			t.Errorf("child %s is not parented on the window span", child.Name)
		}
		name, _ := fixedAttr(child.Attributes, "dev_health.scheduler.schedule")
		switch name.AsString() {
		case "broken-schedule":
			sawBroken = true
			if child.Status.Code != codes.Error {
				t.Errorf("failed schedule span status = %v, want Error", child.Status.Code)
			}
			if strings.Contains(fmt.Sprint(child), "payload-in-error") || child.Status.Description != "other" {
				t.Errorf("error text reached the span or description %q is not the class", child.Status.Description)
			}
		case schedule.ID:
			sawDecided = true
			if child.Status.Code == codes.Error {
				t.Errorf("a clean schedule span carries status Error")
			}
			got := fixedInts(child.Attributes)
			for key, want := range map[string]int64{
				"dev_health.scheduler.due": 2, "dev_health.scheduler.claimed": 3, "dev_health.scheduler.duplicate": 4,
				"dev_health.scheduler.handoffs": 5, "dev_health.scheduler.skipped": 6,
			} {
				if got[key] != want {
					t.Errorf("child %s = %d, want %d", key, got[key], want)
				}
			}
			if v, ok := fixedAttr(child.Attributes, "dev_health.scheduler.cold_start"); !ok || !v.AsBool() {
				t.Errorf("cold_start = %v (present %v), want true", v, ok)
			}
			if v, ok := fixedAttr(child.Attributes, "dev_health.scheduler.stale_skipped"); !ok || !v.AsBool() {
				t.Errorf("stale_skipped = %v (present %v), want true", v, ok)
			}
		default:
			t.Errorf("unexpected child for schedule %q", name.AsString())
		}
	}
	if !sawBroken || !sawDecided {
		t.Fatalf("broken=%v decided=%v, want both", sawBroken, sawDecided)
	}
}

func TestFixedLoopCleanWindowIsNotAnErrorSpan(t *testing.T) {
	exporter := installFixedSpanRecorder(t)
	schedule := heartbeatSchedule(t)
	stepper := resultStepper{schedules: []Schedule{schedule}, results: []ScheduleResult{{ScheduleID: schedule.ID, Due: 1, Claimed: 1, Handoffs: 1}}}
	loop, _ := newFixedTestLoop(t, stepper)
	if err := loop.step(context.Background(), mustTime(t, "2026-07-24T00:00:00Z")); err != nil {
		t.Fatal(err)
	}
	for _, span := range exporter.GetSpans() {
		if span.Status.Code == codes.Error {
			t.Errorf("span %s of a clean window is an Error span", span.Name)
		}
		if _, ok := fixedAttr(span.Attributes, "dev_health.work.stage"); ok {
			t.Errorf("span %s of a clean window carries a stage", span.Name)
		}
	}
}

// scheduleDecided is true when ANY one signal is present, and only then.
func TestScheduleDecidedEachSignalAlone(t *testing.T) {
	if scheduleDecided(ScheduleResult{ScheduleID: "x"}) {
		t.Fatal("an all-zero schedule counted as decided")
	}
	for name, result := range map[string]ScheduleResult{
		"Err":          {Err: errors.New("x")},
		"Due":          {Due: 1},
		"Claimed":      {Claimed: 1},
		"Duplicate":    {Duplicate: 1},
		"Handoffs":     {Handoffs: 1},
		"Skipped":      {Skipped: 1},
		"ColdStart":    {ColdStart: true},
		"StaleSkipped": {StaleSkipped: true},
		"Evaluated":    {Evaluated: true},
	} {
		if !scheduleDecided(result) {
			t.Errorf("%s alone did not count as decided", name)
		}
	}
}

type erroringStepper struct{ schedules []Schedule }

func (stepper erroringStepper) Step(context.Context, time.Time) (WindowResult, error) {
	return WindowResult{}, errors.New("engine down {marker-in-error}")
}
func (stepper erroringStepper) Schedules() []Schedule { return stepper.schedules }

// The span carries only an error class, so the full text of a window failure
// that is not a schedule's (it was returned to run and dropped before
// CHAOS-7879) must reach the log, and not the span.
func TestFixedLoopLogsAWindowFailureThatSpansOnlyClassify(t *testing.T) {
	exporter := installFixedSpanRecorder(t)
	var logs syncBuffer
	clock := &fixedTestClock{now: mustTime(t, "2026-07-24T00:00:00Z")}
	loop, err := newLoop(erroringStepper{schedules: []Schedule{heartbeatSchedule(t)}}, LoopConfig{
		PollInterval: minLoopPollInterval,
		StepTimeout:  time.Second,
		MaxBackoff:   2 * minLoopPollInterval,
		Registry:     health.NewRegistry(time.Second),
		Logger:       slog.New(slog.NewJSONHandler(&logs, nil)),
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan struct{}, 4)
	loop.stepObserved = func() { observed <- struct{}{} }
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForStepObserved(t, observed, "the first window to finish")
	_ = loop.Shutdown(context.Background())
	if !strings.Contains(logs.String(), "fixed schedule window failed") || !strings.Contains(logs.String(), "marker-in-error") {
		t.Errorf("window failure text not logged: %q", logs.String())
	}
	var sawWindow bool
	for _, span := range exporter.GetSpans() {
		if strings.Contains(fmt.Sprint(span), "marker-in-error") {
			t.Errorf("error text reached span %s", span.Name)
		}
		if span.Name == "dev_health.scheduler.fixed_window" {
			sawWindow = true
			if span.Status.Code != codes.Error {
				t.Errorf("an engine error window must be an Error span")
			}
			if stage, _ := fixedAttr(span.Attributes, "dev_health.work.stage"); stage.AsString() != "engine" {
				t.Errorf("window stage = %q, want engine", stage.AsString())
			}
		}
	}
	if !sawWindow {
		t.Error("no fixed_window span")
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
