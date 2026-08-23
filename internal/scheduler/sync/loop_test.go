package sync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type loopStepFunc func(context.Context, time.Time, int, Coordinator) (HandoffResult, error)

func (function loopStepFunc) HandoffDueResult(
	ctx context.Context,
	now time.Time,
	limit int,
	coordinator Coordinator,
) (HandoffResult, error) {
	return function(ctx, now, limit, coordinator)
}

type testLoopClock struct {
	mu     sync.Mutex
	now    time.Time
	ticker *testLoopTicker
}

func (clock *testLoopClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *testLoopClock) NewTicker(time.Duration) loopTicker {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.ticker = &testLoopTicker{ticks: make(chan time.Time, 8)}
	return clock.ticker
}

type testLoopTicker struct {
	ticks   chan time.Time
	stopped chan struct{}
	once    sync.Once
}

func (ticker *testLoopTicker) Chan() <-chan time.Time { return ticker.ticks }
func (ticker *testLoopTicker) Stop() {
	ticker.once.Do(func() {
		ticker.stopped = make(chan struct{})
		close(ticker.stopped)
	})
}

func newTestLoop(t *testing.T, stepper HandoffStepper, clock *testLoopClock) (*Loop, *health.Registry) {
	t.Helper()
	registry := health.NewRegistry(time.Second)
	loop, err := newLoop(stepper, CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
		return OccurrenceMinted, nil
	}), LoopConfig{
		PollInterval: minLoopPollInterval,
		StepTimeout:  time.Second,
		MaxBackoff:   80 * time.Millisecond,
		Limit:        3,
		Registry:     registry,
		Occurrences:  &stubOccurrences{},
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	return loop, registry
}

func openLoopReadiness(t *testing.T, registry *health.Registry) {
	t.Helper()
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func waitForLoopReadiness(t *testing.T, loop *Loop, registry *health.Registry, wantReady bool) health.Readiness {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var status health.Readiness
	for time.Now().Before(deadline) {
		status = registry.Readiness(context.Background())
		loop.mu.Lock()
		up := loop.up
		loop.mu.Unlock()
		if status.Ready == wantReady && up == wantReady {
			return status
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for scheduler readiness %t; last status = %#v", wantReady, status)
	return status
}

// stubOccurrences is a no-work occurrence consumer. The loop requires one, so
// every loop test supplies it; the reconciler's own behavior is covered by its
// integration tests.
type stubOccurrences struct {
	calls  atomic.Int64
	result OccurrenceReconcileResult
	err    error
}

func (stepper *stubOccurrences) Reconcile(
	context.Context, time.Time, int,
) (OccurrenceReconcileResult, error) {
	stepper.calls.Add(1)
	return stepper.result, stepper.err
}

func TestLoopImmediateWindowOpensReadinessAndExportsMetrics(t *testing.T) {
	clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
	calls := make(chan struct{}, 1)
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
		calls <- struct{}{}
		return HandoffResult{
			HandedOff: []Occurrence{{ID: "first"}},
		}, nil
	}), clock)
	openLoopReadiness(t, registry)
	if status := registry.Readiness(context.Background()); status.Ready || !strings.Contains(strings.Join(status.Failed, ","), "scheduler_loop") {
		t.Fatalf("pre-start readiness = %#v", status)
	}
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-calls:
	default:
		t.Fatal("immediate scheduler window did not run")
	}
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("post-start readiness = %#v", status)
	}

	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sync_scheduler_windows_total 1",
		"sync_scheduler_handoffs_total 1",
		"sync_scheduler_unsupported_cron_fallback_total 0",
		"sync_scheduler_invalid_cron_total 0",
		"sync_scheduler_up 1",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
	if strings.Contains(metrics.String(), "{") {
		t.Fatalf("metrics must not expose dynamic labels:\n%s", metrics.String())
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLoopFallbackWindowFailsClosedAndExportsCounts(t *testing.T) {
	clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
		return HandoffResult{Candidates: 3, UnsupportedCron: 2, InvalidCron: 1}, ErrSchedulerFallbackRequired
	}), clock)
	openLoopReadiness(t, registry)
	if err := loop.Start(context.Background()); !errors.Is(err, ErrSchedulerFallbackRequired) {
		t.Fatalf("Start() error = %v", err)
	}
	if status := registry.Readiness(context.Background()); status.Ready {
		t.Fatalf("fallback readiness = %#v", status)
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sync_scheduler_unsupported_cron_fallback_total 2",
		"sync_scheduler_invalid_cron_total 1",
		"sync_scheduler_up 0",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
}

func TestLoopFailureBacksOffClosesReadinessAndRecovers(t *testing.T) {
	clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
	failure := errors.New("database unavailable")
	calls := 0
	var failureCompleted time.Time
	failed := make(chan struct{}, 1)
	recovered := make(chan struct{}, 1)
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
		calls++
		switch calls {
		case 1:
			return HandoffResult{}, nil
		case 2:
			clock.mu.Lock()
			clock.now = clock.now.Add(3 * minLoopPollInterval)
			failureCompleted = clock.now
			clock.mu.Unlock()
			failed <- struct{}{}
			return HandoffResult{}, failure
		default:
			recovered <- struct{}{}
			return HandoffResult{}, nil
		}
	}), clock)
	openLoopReadiness(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	ticker := clock.ticker
	clock.now = clock.now.Add(minLoopPollInterval)
	firstTick := clock.now
	clock.mu.Unlock()
	ticker.ticks <- firstTick
	<-failed
	status := waitForLoopReadiness(t, loop, registry, false)
	if status.Ready || !strings.Contains(strings.Join(status.Failed, ","), "scheduler_loop") {
		t.Fatalf("failed readiness = %#v", status)
	}
	var failedMetrics bytes.Buffer
	if err := loop.WritePrometheus(&failedMetrics); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(failedMetrics.String(), "sync_scheduler_last_success_age_seconds 0\n") {
		t.Fatalf("failed metrics hid prior success age:\n%s", failedMetrics.String())
	}
	// The first retry delay is exactly PollInterval from failure completion,
	// not from the stale tick that began the slow operation.
	ticker.ticks <- failureCompleted.Add(minLoopPollInterval - time.Nanosecond)
	time.Sleep(5 * time.Millisecond)
	if calls != 2 {
		t.Fatalf("backoff ignored; calls = %d", calls)
	}
	clock.mu.Lock()
	clock.now = failureCompleted.Add(minLoopPollInterval)
	retryTick := clock.now
	clock.mu.Unlock()
	ticker.ticks <- retryTick
	<-recovered
	waitForLoopReadiness(t, loop, registry, true)
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLoopTimeoutIsFailureNotEmptySuccessAndShutdownCancelsStep(t *testing.T) {
	clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
	entered := make(chan struct{}, 1)
	released := make(chan struct{})
	loop, registry := newTestLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int, _ Coordinator) (HandoffResult, error) {
		entered <- struct{}{}
		<-ctx.Done()
		close(released)
		return HandoffResult{}, ctx.Err()
	}), clock)
	openLoopReadiness(t, registry)
	if err := loop.Start(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v", err)
	}
	<-entered
	<-released
	if status := registry.Readiness(context.Background()); status.Ready || !strings.Contains(strings.Join(status.Failed, ","), "scheduler_loop") {
		t.Fatalf("timeout readiness = %#v", status)
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLoopRejectsDoubleStartAndStopsTicker(t *testing.T) {
	clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
	loop, _ := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
		return HandoffResult{}, nil
	}), clock)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := loop.Start(context.Background()); !errors.Is(err, ErrLoopAlreadyStarted) {
		t.Fatalf("second Start() error = %v", err)
	}
	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ticker.stopped:
	default:
		t.Fatal("shutdown did not stop ticker")
	}
}

// CUT-06's consumer is only useful if something drives it. The marker advances
// the moment an occurrence is handed off, so a loop that produces occurrences
// without consuming them strands work permanently once the optional Python
// consumer is off.
func TestLoopDrivesTheOccurrenceConsumerEveryWindow(t *testing.T) {
	clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
	occurrences := &stubOccurrences{
		result: OccurrenceReconcileResult{Scanned: 2, Completed: 1, Retried: 1},
	}
	registry := health.NewRegistry(time.Second)
	loop, err := newLoop(
		schedulerHandoffStepper(func() (HandoffResult, error) {
			return HandoffResult{}, nil
		}),
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
			return OccurrenceMinted, nil
		}),
		LoopConfig{
			PollInterval: minLoopPollInterval,
			StepTimeout:  time.Second,
			MaxBackoff:   80 * time.Millisecond,
			Limit:        3,
			Registry:     registry,
			Occurrences:  occurrences,
		},
		clock,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loop.Shutdown(context.Background()) }()
	if occurrences.calls.Load() != 1 {
		t.Fatalf("the initial window drove the consumer %d times", occurrences.calls.Load())
	}
	var metrics strings.Builder
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sync_scheduler_occurrences_completed_total 1",
		"sync_scheduler_occurrences_retried_total 1",
		"sync_scheduler_occurrences_quarantined_total 0",
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Errorf("metrics omit %q", want)
		}
	}
}

// A consumer failure has to fail the window. Reporting a healthy scheduler
// while pending occurrences pile up is precisely the silent-stranding failure
// this wiring exists to prevent.
func TestLoopFailsTheWindowWhenOccurrencesCannotBeConsumed(t *testing.T) {
	clock := &testLoopClock{now: at("2026-07-23T12:00:00Z")}
	occurrences := &stubOccurrences{err: ErrMaterializerUnavailable}
	registry := health.NewRegistry(time.Second)
	loop, err := newLoop(
		schedulerHandoffStepper(func() (HandoffResult, error) {
			return HandoffResult{}, nil
		}),
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
			return OccurrenceMinted, nil
		}),
		LoopConfig{
			PollInterval: minLoopPollInterval,
			StepTimeout:  time.Second,
			MaxBackoff:   80 * time.Millisecond,
			Limit:        3,
			Registry:     registry,
			Occurrences:  occurrences,
		},
		clock,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.Start(context.Background()); err == nil {
		t.Fatal("a scheduler that cannot consume its own occurrences started successfully")
	}
	if err := loop.readiness(context.Background()); err == nil {
		t.Fatal("readiness stayed open with an unconsumable occurrence backlog")
	}
}

// A loop cannot be constructed without a consumer at all.
func TestLoopRequiresAnOccurrenceConsumer(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	_, err := newLoop(
		schedulerHandoffStepper(func() (HandoffResult, error) { return HandoffResult{}, nil }),
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
			return OccurrenceMinted, nil
		}),
		LoopConfig{
			PollInterval: minLoopPollInterval,
			StepTimeout:  time.Second,
			MaxBackoff:   80 * time.Millisecond,
			Limit:        3,
			Registry:     registry,
		},
		&testLoopClock{now: at("2026-07-23T12:00:00Z")},
	)
	if err == nil {
		t.Fatal("newLoop() accepted a config with no occurrence consumer")
	}
}

type schedulerHandoffStepper func() (HandoffResult, error)

func (stepper schedulerHandoffStepper) HandoffDueResult(
	context.Context, time.Time, int, Coordinator,
) (HandoffResult, error) {
	return stepper()
}

// TestLoopWithoutALoggerDoesNotFallBackToSlogDefault proves the nil-logger
// path is inert: a loop given no Logger must not panic on a failed handoff
// window, and it must not fall back to slog.Default() -- that would send
// output to a sink other than the process's configured JSON logger, so a
// log-capturing test could pass while production ships nothing (CHAOS-3907).
// Mirrors scheduler/fixed's own nil-logger test for its literal sibling loop.
func TestLoopWithoutALoggerDoesNotFallBackToSlogDefault(t *testing.T) {
	var buf bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(original)

	failure := errors.New("initial handoff probe failure")
	stepper := loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
		return HandoffResult{}, failure
	})
	loop, _ := newTestLoop(t, stepper, &testLoopClock{})
	if loop.config.Logger != nil {
		t.Fatal("test loop unexpectedly has a logger")
	}
	if err := loop.Start(context.Background()); err == nil {
		t.Fatal("Start() = nil, want the scripted initial handoff failure")
	}
	if buf.Len() != 0 {
		t.Fatalf("nil logger fell back to slog.Default(): %s", buf.String())
	}
}

// TestLoopExportsPreMintSkipCounters proves the two pre-mint eligibility
// refusals reach Prometheus. Without this the gates would be invisible in
// production: a fleet whose schedules are being refused for missing
// organizations looks exactly like a fleet with nothing due, and those need
// opposite responses.
func TestLoopExportsPreMintSkipCounters(t *testing.T) {
	clock := &testLoopClock{now: at("2026-08-23T12:00:00Z")}
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
		return HandoffResult{
			Candidates:             5,
			TimingEligible:         5,
			SkippedOrgMissing:      3,
			SkippedFeatureDisabled: 2,
		}, nil
	}), clock)
	openLoopReadiness(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sync_scheduler_skipped_org_missing_total 3",
		"sync_scheduler_skipped_feature_disabled_total 2",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
	if strings.Contains(metrics.String(), "{") {
		t.Fatalf("metrics must not expose dynamic labels:\n%s", metrics.String())
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
