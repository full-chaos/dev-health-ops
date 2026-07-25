package sync

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
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
	loop, err := newLoop(stepper, CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) error {
		return nil
	}), LoopConfig{
		PollInterval: minLoopPollInterval,
		StepTimeout:  time.Second,
		MaxBackoff:   80 * time.Millisecond,
		Limit:        3,
		Registry:     registry,
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
	if status := registry.Readiness(context.Background()); status.Ready || !strings.Contains(strings.Join(status.Failed, ","), "scheduler_loop") {
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
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("recovered readiness = %#v", status)
	}
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
