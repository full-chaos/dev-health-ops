package joboutbox

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

type loopStepFunc func(context.Context, time.Time, int) (StepResult, error)

func (fn loopStepFunc) Step(ctx context.Context, now time.Time, limit int) (StepResult, error) {
	return fn(ctx, now, limit)
}

type testReconcilerClock struct {
	mu     sync.Mutex
	now    time.Time
	ticker *testReconcilerTicker
}

func (clock *testReconcilerClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *testReconcilerClock) NewTicker(time.Duration) reconcilerTicker {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.ticker = &testReconcilerTicker{ticks: make(chan time.Time, 4)}
	return clock.ticker
}

type testReconcilerTicker struct {
	ticks   chan time.Time
	stopped chan struct{}
	once    sync.Once
}

func (ticker *testReconcilerTicker) Chan() <-chan time.Time { return ticker.ticks }

func (ticker *testReconcilerTicker) Stop() {
	ticker.once.Do(func() {
		if ticker.stopped == nil {
			ticker.stopped = make(chan struct{})
		}
		close(ticker.stopped)
	})
}

func newTestReconcilerLoop(
	t *testing.T,
	stepper RelayStepper,
	clock *testReconcilerClock,
) (*ReconcilerLoop, *health.Registry) {
	t.Helper()
	registry := health.NewRegistry(time.Second)
	loop, err := newReconcilerLoop(stepper, ReconcilerLoopConfig{
		PollInterval: minReconcilerPollInterval,
		Limit:        7,
		Registry:     registry,
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	return loop, registry
}

func openReconcilerReadiness(t *testing.T, registry *health.Registry) {
	t.Helper()
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilerLoopImmediateNoopStepOpensReadiness(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	calls := make(chan struct{}, 1)
	loop, registry := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		calls <- struct{}{}
		return StepResult{}, nil
	}), clock)
	openReconcilerReadiness(t, registry)
	if status := registry.Readiness(context.Background()); status.Ready || !strings.Contains(strings.Join(status.Failed, ","), "reconciler_loop") {
		t.Fatalf("pre-start readiness = %#v", status)
	}
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-calls:
	default:
		t.Fatal("immediate reconciliation step did not run")
	}
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("post-start readiness = %#v", status)
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilerLoopAccumulatesResultsAndExportsLowCardinalityMetrics(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	results := []StepResult{{Claimed: 2, Delivered: 1, Retried: 1}, {Claimed: 3, Dead: 1, LeaseLost: 2}}
	steps := make(chan struct{}, 2)
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		result := results[0]
		results = results[1:]
		steps <- struct{}{}
		return result, nil
	}), clock)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-steps
	clock.mu.Lock()
	clock.now = clock.now.Add(3 * time.Second)
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now()
	<-steps

	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"worker_outbox_reconciler_claimed_total 5",
		"worker_outbox_reconciler_delivered_total 1",
		"worker_outbox_reconciler_retried_total 1",
		"worker_outbox_reconciler_dead_total 1",
		"worker_outbox_reconciler_lease_lost_total 2",
		"worker_outbox_reconciler_up 1",
		"worker_outbox_reconciler_last_success_age_seconds 0",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
	if strings.Contains(metrics.String(), "{") {
		t.Fatalf("reconciler metrics must not expose labels:\n%s", metrics.String())
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilerLoopPropagatesPeriodicFatalErrorAndClosesReadiness(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	fatal := errors.New("database unavailable")
	steps := 0
	failedStep := make(chan struct{}, 1)
	loop, registry := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		steps++
		if steps == 1 {
			return StepResult{}, nil
		}
		failedStep <- struct{}{}
		return StepResult{}, fatal
	}), clock)
	openReconcilerReadiness(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	<-failedStep
	if err := <-loop.Errors(); !errors.Is(err, fatal) {
		t.Fatalf("Errors() = %v, want %v", err, fatal)
	}
	if status := registry.Readiness(context.Background()); status.Ready || !strings.Contains(strings.Join(status.Failed, ","), "reconciler_loop") {
		t.Fatalf("failed-loop readiness = %#v", status)
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilerLoopRejectsDoubleStartAndCancelsOnShutdown(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	calls := 0
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		calls++
		return StepResult{}, nil
	}), clock)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := loop.Start(context.Background()); !errors.Is(err, ErrReconcilerLoopAlreadyStarted) {
		t.Fatalf("second Start() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("immediate step calls = %d, want one", calls)
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

func TestReconcilerLoopShutdownHonorsLifecycleDeadlineDuringStep(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	calls := 0
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		calls++
		if calls == 1 {
			return StepResult{}, nil
		}
		entered <- struct{}{}
		<-release
		return StepResult{}, nil
	}), clock)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	<-entered

	shutdownCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := loop.Shutdown(shutdownCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown() error = %v, want context cancellation", err)
	}
	close(release)
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilerLoopRejectsUnboundedConfiguration(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	stepper := loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) { return StepResult{}, nil })
	for _, config := range []ReconcilerLoopConfig{
		{PollInterval: minReconcilerPollInterval, Limit: 1},
		{PollInterval: minReconcilerPollInterval - time.Nanosecond, Limit: 1, Registry: registry},
		{PollInterval: maxReconcilerPollInterval + time.Nanosecond, Limit: 1, Registry: registry},
		{PollInterval: minReconcilerPollInterval, Limit: 0, Registry: registry},
		{PollInterval: minReconcilerPollInterval, Limit: maxReconcilerLimit + 1, Registry: registry},
	} {
		if _, err := NewReconcilerLoop(stepper, config); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("NewReconcilerLoop(%#v) error = %v", config, err)
		}
	}
}
