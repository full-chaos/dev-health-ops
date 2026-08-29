package joboutbox

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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
	results := []StepResult{{Recovered: 1, PostRepairContractRejectionsRecovered: 1, Claimed: 2, Delivered: 1, Retried: 1}, {Recovered: 2, Claimed: 3, Dead: 1, LeaseLost: 2}}
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		result := results[0]
		results = results[1:]
		return result, nil
	}), clock)
	ctx := context.Background()
	if err := loop.step(ctx, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	clock.now = clock.now.Add(3 * time.Second)
	clock.mu.Unlock()
	if err := loop.step(ctx, clock.Now()); err != nil {
		t.Fatal(err)
	}

	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"worker_outbox_reconciler_terminal_deliveries_recovered_total 3",
		"worker_outbox_reconciler_post_repair_contract_rejections_recovered_total 1",
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
}

// TestReconcilerLoopAbsorbsTransientStepFailuresWithoutFatalError is the
// CHAOS-4429 regression test. Before this fix, the very first periodic
// Relay.Step failure sent on loop.Errors() AND returned from run(),
// permanently killing the polling goroutine -- which lifecycle.Runtime then
// treated as fatal to the ENTIRE reconciler process, taking the mutation
// pipeline's materializer/kernel/lease-repair stages down with it even
// though CHAOS-4239 had already fixed exactly this defect shape for them.
// This test proves the corrected contract end to end: two consecutive
// failures (below consecutiveStepFailureDegradeThreshold) leave readiness
// open and Errors() silent; a third crosses the threshold and closes
// readiness WITHOUT firing Errors(); a subsequent success both self-heals
// readiness and proves the polling goroutine is still alive -- on the
// pre-fix code this last tick is never consumed because run() already
// returned, so the test hangs instead of completing.
func TestReconcilerLoopAbsorbsTransientStepFailuresWithoutFatalError(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	failing := errors.New("database unavailable")
	// index 0 (Start's initial step, run synchronously by Start itself) is
	// consumed and complete by the time Start returns, so it needs no
	// separate synchronization; 1-3 fail, crossing the 3-consecutive-failure
	// threshold on the third; 4 succeeds again.
	results := []error{nil, failing, failing, failing, nil}
	loop, registry := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		err := results[0]
		results = results[1:]
		return StepResult{}, err
	}), clock)
	// stepObserved (codex review finding) fires only once run has finished
	// ALL bookkeeping for a tick -- recordStepFailure or the success path --
	// so assertions right after it can never race that bookkeeping the way
	// a signal sent from inside the stepper closure could.
	observed := make(chan struct{}, 1)
	loop.stepObserved = func() { observed <- struct{}{} }
	openReconcilerReadiness(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()

	// Two consecutive failures: below the degrade threshold. Readiness must
	// stay open (chris's CHAOS-4239 ruling: a lone blip or two must not flap
	// readyz) and nothing may reach Errors().
	for i := 0; i < 2; i++ {
		ticker.ticks <- clock.Now().Add(time.Duration(i+1) * time.Second)
		<-observed
	}
	select {
	case err := <-loop.Errors():
		t.Fatalf("Errors() fired on a transient failure below the degrade threshold: %v", err)
	default:
	}
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("readiness closed after only 2 consecutive failures (below threshold %d): %#v",
			consecutiveStepFailureDegradeThreshold, status)
	}

	// Third consecutive failure crosses the threshold: readiness closes, but
	// the process itself is never torn down.
	ticker.ticks <- clock.Now().Add(3 * time.Second)
	<-observed
	select {
	case err := <-loop.Errors():
		t.Fatalf("Errors() fired on a periodic step failure -- CHAOS-4429: this must never be fatal: %v", err)
	default:
	}
	if status := registry.Readiness(context.Background()); status.Ready || !strings.Contains(strings.Join(status.Failed, ","), "reconciler_loop") {
		t.Fatalf("readiness did not close after %d consecutive failures: %#v", consecutiveStepFailureDegradeThreshold, status)
	}

	// A subsequent success self-heals readiness immediately AND proves the
	// polling goroutine survived every failure above.
	ticker.ticks <- clock.Now().Add(4 * time.Second)
	<-observed
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("readiness did not self-heal after a successful step: %#v", status)
	}

	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestReconcilerLoopStepPreservesCommittedResultOnFailure is a codex review
// finding: Relay.Step returns a non-zero StepResult ALONGSIDE a non-nil
// error on most of its own mid-pass failure paths (a claim already
// dispatched before a later claim in the same pass faults, say) -- those
// actions already committed to Postgres. step must count them even though
// the pass as a whole failed; discarding them here would now lose that
// evidence on every retry instead of just once before a pre-CHAOS-4429
// crash-loop restart reset the counters anyway.
func TestReconcilerLoopStepPreservesCommittedResultOnFailure(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	failing := errors.New("dispatch failed mid-pass")
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		return StepResult{Claimed: 3, Delivered: 2, Dead: 1}, failing
	}), clock)
	if err := loop.step(context.Background(), clock.Now()); !errors.Is(err, failing) {
		t.Fatalf("step() error = %v, want %v", err, failing)
	}

	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"worker_outbox_reconciler_claimed_total 3",
		"worker_outbox_reconciler_delivered_total 2",
		"worker_outbox_reconciler_dead_total 1",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Fatalf("metrics missing %q -- committed StepResult was discarded on a failed step:\n%s", want, metrics.String())
		}
	}
}

// TestReconcilerLoopShutdownCancellationDuringStepIsNotCountedAsFailure is a
// codex review finding (round 2 tightened round 1's fix): Shutdown sets
// stopping THEN cancels the loop's context, so a step in flight at that
// moment observes its own shutdown as a Relay.Step error. That is the
// process stopping on purpose, not an operational failure, and must never
// increment step_failures_total or the degrade streak.
//
// The stepper here returns ErrUnavailable, NOT ctx.Err() -- matching what
// the REAL Repository does (claimDueExcept/Dispatch/recordFailure all
// collapse a cancellation into ErrUnavailable before Relay.Step ever
// returns, internal/joboutbox/repository.go). Round 1's fix checked
// errors.Is(err, context.Canceled), which this error deliberately does NOT
// satisfy -- a synthetic ctx.Err() stepper here would have let that gap pass
// silently, the same way it did the first time.
func TestReconcilerLoopShutdownCancellationDuringStepIsNotCountedAsFailure(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	entered := make(chan struct{}, 1)
	calls := 0
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (StepResult, error) {
		calls++
		if calls == 1 {
			return StepResult{}, nil
		}
		entered <- struct{}{}
		<-ctx.Done()
		return StepResult{}, ErrUnavailable
	}), clock)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	<-entered

	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-loop.Errors():
		t.Fatalf("Errors() fired on shutdown cancellation, not an operational failure: %v", err)
	default:
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics.String(), "worker_outbox_reconciler_step_failures_total 0\n") {
		t.Fatalf("shutdown cancellation was counted as a step failure:\n%s", metrics.String())
	}
}

// TestReconcilerLoopExportsCHAOS4429Telemetry pins the new Prometheus
// fragment: a lifetime failure counter that counts even absorbed blips below
// the degrade threshold, and a degraded gauge that flips only once the
// threshold is crossed.
func TestReconcilerLoopExportsCHAOS4429Telemetry(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	failing := errors.New("database unavailable")
	results := []error{nil, failing, failing, failing}
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		err := results[0]
		results = results[1:]
		return StepResult{}, err
	}), clock)
	ctx := context.Background()
	for range 4 {
		// run() is what actually calls recordStepFailure on a step error;
		// exercise it directly rather than through the ticker so this test
		// stays a fast, synchronous unit test of the telemetry alone.
		if err := loop.step(ctx, clock.Now()); err != nil {
			loop.recordStepFailure(ctx, err)
		}
		clock.mu.Lock()
		clock.now = clock.now.Add(time.Second)
		clock.mu.Unlock()
	}

	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"worker_outbox_reconciler_step_failures_total 3",
		"worker_outbox_reconciler_degraded 1",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
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

// TestReconcilerLoopWithoutALoggerDoesNotFallBackToSlogDefault proves the
// mirror image of the logging tests above: a loop given no Logger must not
// panic on a failed step, and it must not fall back to slog.Default() --
// that would send output to a sink other than the process's configured JSON
// logger, so a log-capturing test could pass while production ships nothing
// (CHAOS-3907).
func TestReconcilerLoopWithoutALoggerDoesNotFallBackToSlogDefault(t *testing.T) {
	var buf bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(original)

	failure := errors.New("initial step probe failure")
	stepper := loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		return StepResult{}, failure
	})
	loop, _ := newTestReconcilerLoop(t, stepper, &testReconcilerClock{})
	if loop.config.Logger != nil {
		t.Fatal("test loop unexpectedly has a logger")
	}
	if err := loop.Start(context.Background()); err == nil {
		t.Fatal("Start() = nil, want the scripted initial step failure")
	}
	if buf.Len() != 0 {
		t.Fatalf("nil logger fell back to slog.Default(): %s", buf.String())
	}
}
