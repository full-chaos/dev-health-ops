package syncreconciler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type loopStepFunc func(context.Context, time.Time, int) (Observation, error)

func (fn loopStepFunc) Step(ctx context.Context, now time.Time, limit int) (Observation, error) {
	return fn(ctx, now, limit)
}

type recorderFunc func(Observation) bool

func (fn recorderFunc) TryRecord(observation Observation) bool { return fn(observation) }

type testClock struct {
	mu     sync.Mutex
	now    time.Time
	ticker *testTicker
}

func (clock *testClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *testClock) NewTicker(time.Duration) loopTicker {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.ticker = &testTicker{ticks: make(chan time.Time, 2)}
	return clock.ticker
}

type testTicker struct {
	ticks   chan time.Time
	stopped chan struct{}
	once    sync.Once
}

func (ticker *testTicker) Chan() <-chan time.Time { return ticker.ticks }
func (ticker *testTicker) Stop() {
	ticker.once.Do(func() {
		ticker.stopped = make(chan struct{})
		close(ticker.stopped)
	})
}

func newTestLoop(t *testing.T, stepper Stepper, clock *testClock) (*Loop, *health.Registry) {
	t.Helper()
	return newTestLoopWithTimeout(t, stepper, clock, defaultObservationTimeout)
}

func newTestLoopWithTimeout(
	t *testing.T,
	stepper Stepper,
	clock *testClock,
	timeout time.Duration,
) (*Loop, *health.Registry) {
	t.Helper()
	return newTestLoopConfigured(t, stepper, clock, timeout, nil)
}

func newTestLoopConfigured(
	t *testing.T,
	stepper Stepper,
	clock *testClock,
	timeout time.Duration,
	recorder ObservationRecorder,
) (*Loop, *health.Registry) {
	t.Helper()
	registry := health.NewRegistry(time.Second)
	loop, err := newLoop(stepper, LoopConfig{
		PollInterval:       minPollInterval,
		ObservationTimeout: timeout,
		Limit:              7,
		Registry:           registry,
		Recorder:           recorder,
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	return loop, registry
}

func openReadinessGate(t *testing.T, registry *health.Registry) {
	t.Helper()
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func testObservation() Observation {
	return Observation{
		Kinds: []KindObservation{
			{Kind: frozenKinds[0], DuePending: 2, ExpiredClaims: 1},
			{Kind: frozenKinds[1], DuePending: 1},
			{Kind: frozenKinds[2], DuePending: 3, ExpiredClaims: 2, Route: "river"},
			{Kind: frozenKinds[3]},
		},
		CeleryDuePending:  3,
		RiverDuePending:   3,
		SampledCandidates: 6,
		ObservedAt:        time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC),
		Limit:             7,
		PredicateVersion:  PredicateVersion,
		DigestVersion:     DigestVersion,
		CandidateDigest:   "sha256:must-not-be-a-metric-label",
	}
}

func TestLoopImmediateObservationGatesReadinessAndExportsGauges(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	calls := make(chan struct{}, 1)
	loop, registry := newTestLoop(t, loopStepFunc(func(_ context.Context, _ time.Time, limit int) (Observation, error) {
		if limit != 7 {
			t.Fatalf("limit = %d", limit)
		}
		calls <- struct{}{}
		return testObservation(), nil
	}), clock)
	openReadinessGate(t, registry)
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("pre-start readiness = %#v", readiness)
	}
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-calls
	if readiness := registry.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("post-start readiness = %#v", readiness)
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sync_dispatch_observer_due_pending{kind=\"dispatch_sync_run\"} 2",
		"sync_dispatch_observer_expired_claims{kind=\"post_sync\"} 2",
		"sync_dispatch_observer_celery_due_pending 3",
		"sync_dispatch_observer_river_due_pending 3",
		"sync_dispatch_observer_sampled_candidates 6",
		"sync_dispatch_observer_truncated 0",
		"sync_dispatch_observer_up 1",
		"bounded Python claim-order window",
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
	if strings.Contains(metrics.String(), " counter\n") ||
		strings.Contains(metrics.String(), PredicateVersion) ||
		strings.Contains(metrics.String(), DigestVersion) ||
		strings.Contains(metrics.String(), canonicalObservedAt(testObservation().ObservedAt)) ||
		strings.Contains(metrics.String(), "must-not-be-a-metric-label") {
		t.Fatalf("metrics must be numeric bounded gauges:\n%s", metrics.String())
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLoopRecorderDropAndPanicNeverAffectSuccessfulReadiness(t *testing.T) {
	for name, recorder := range map[string]ObservationRecorder{
		"drop": recorderFunc(func(Observation) bool { return false }),
		"panic": recorderFunc(func(Observation) bool {
			panic("recorder panic")
		}),
	} {
		t.Run(name, func(t *testing.T) {
			clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
			loop, registry := newTestLoopConfigured(
				t,
				loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
					return testObservation(), nil
				}),
				clock,
				defaultObservationTimeout,
				recorder,
			)
			openReadinessGate(t, registry)
			if err := loop.Start(context.Background()); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			if readiness := registry.Readiness(context.Background()); !readiness.Ready {
				t.Fatalf("recorder changed readiness = %#v", readiness)
			}
			if err := loop.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLoopBlockingRecorderCannotStallStepsOrSpawnUnboundedCalls(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	recorderEntered := make(chan struct{})
	recorderRelease := make(chan struct{})
	var recorderCalls atomic.Int64
	recorder := recorderFunc(func(Observation) bool {
		recorderCalls.Add(1)
		close(recorderEntered)
		<-recorderRelease
		return true
	})
	steps := make(chan struct{}, 2)
	loop, registry := newTestLoopConfigured(
		t,
		loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
			steps <- struct{}{}
			return testObservation(), nil
		}),
		clock,
		defaultObservationTimeout,
		recorder,
	)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-steps
	<-recorderEntered
	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	<-steps
	if recorderCalls.Load() != 1 {
		t.Fatalf("blocking recorder calls = %d, want one bounded in-flight call", recorderCalls.Load())
	}
	if readiness := registry.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("blocking recorder changed readiness = %#v", readiness)
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatalf("blocking recorder stalled shutdown: %v", err)
	}
	close(recorderRelease)
}

func TestLoopOffersBoundedUnknownKindObservation(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	recorded := make(chan Observation, 1)
	recorder := recorderFunc(func(observation Observation) bool {
		recorded <- observation
		return true
	})
	unknown := validRecorderObservation()
	unknown.UnknownKindCount = 1
	unknown.SampledCandidates = 3
	loop, registry := newTestLoopConfigured(
		t,
		loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
			return unknown, ErrUnknownKind
		}),
		clock,
		defaultObservationTimeout,
		recorder,
	)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case observation := <-recorded:
		if observation.UnknownKindCount != 1 || observation.SampledCandidates != 3 {
			t.Fatalf("recorded observation = %#v", observation)
		}
	case <-time.After(time.Second):
		t.Fatal("unknown-kind observation was not offered")
	}
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("unknown-kind readiness = %#v", readiness)
	}
}

func TestLoopPeriodicErrorClosesReadinessAndSurfacesError(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	fatal := errors.New("database unavailable")
	calls := 0
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		calls++
		if calls == 1 {
			return testObservation(), nil
		}
		return Observation{}, fatal
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	clock.now = clock.now.Add(5 * time.Second)
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now()
	if err := <-loop.Errors(); !errors.Is(err, fatal) {
		t.Fatalf("Errors() = %v", err)
	}
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("failed readiness = %#v", readiness)
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sync_dispatch_observer_up 0\n",
		"sync_dispatch_observer_last_success_age_seconds 5\n",
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Fatalf("post-failure metrics missing %q:\n%s", want, metrics.String())
		}
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLoopInitialObservationDeadlineIsBoundedAndSanitized(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	exited := make(chan struct{})
	loop, registry := newTestLoopWithTimeout(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
		<-ctx.Done()
		close(exited)
		return Observation{}, fmt.Errorf("postgres://operator:secret@db/app: %w", ctx.Err())
	}), clock, minObservationTimeout)
	openReadinessGate(t, registry)

	err := loop.Start(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want deadline", err)
	}
	if strings.Contains(err.Error(), "postgres://") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("Start() leaked step error detail: %v", err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("initial step goroutine did not exit after deadline")
	}
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("deadline readiness = %#v", readiness)
	}
	select {
	case fatal := <-loop.Errors():
		t.Fatalf("initial deadline unexpectedly surfaced on Errors(): %v", fatal)
	default:
	}
}

func TestLoopPeriodicObservationDeadlineIsFatalAndSanitized(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	pollExited := make(chan struct{})
	calls := 0
	loop, registry := newTestLoopWithTimeout(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
		calls++
		if calls == 1 {
			return testObservation(), nil
		}
		<-ctx.Done()
		close(pollExited)
		return Observation{}, fmt.Errorf("postgres://operator:secret@db/app: %w", ctx.Err())
	}), clock, minObservationTimeout)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	fatal := <-loop.Errors()
	if !errors.Is(fatal, context.DeadlineExceeded) {
		t.Fatalf("Errors() = %v, want deadline", fatal)
	}
	if strings.Contains(fatal.Error(), "postgres://") || strings.Contains(fatal.Error(), "secret") {
		t.Fatalf("Errors() leaked step error detail: %v", fatal)
	}
	select {
	case <-pollExited:
	default:
		t.Fatal("periodic step goroutine did not exit after deadline")
	}
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("deadline readiness = %#v", readiness)
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics.String(), "sync_dispatch_observer_up 0\n") {
		t.Fatalf("deadline metrics =\n%s", metrics.String())
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLoopReportsUnknownKindGaugeWhileFailingClosed(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	loop, _ := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		return Observation{UnknownKindCount: 4, SampledCandidates: 4}, ErrUnknownKind
	}), clock)
	if err := loop.Start(context.Background()); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("Start() error = %v", err)
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics.String(), "sync_dispatch_observer_unknown_kinds 4\n") ||
		!strings.Contains(metrics.String(), "sync_dispatch_observer_up 0\n") {
		t.Fatalf("unknown-kind failure metrics =\n%s", metrics.String())
	}
}

func TestLoopParentCancellationClosesReadiness(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		return testObservation(), nil
	}), clock)
	openReadinessGate(t, registry)
	ctx, cancel := context.WithCancel(context.Background())
	if err := loop.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if readiness := registry.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("started readiness = %#v", readiness)
	}
	cancel()
	loop.mu.Lock()
	done := loop.done
	loop.mu.Unlock()
	<-done
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("canceled readiness = %#v", readiness)
	}
	select {
	case err := <-loop.Errors():
		t.Fatalf("parent cancellation surfaced fatal error: %v", err)
	default:
	}
}

func TestLoopShutdownCancelsAndWaitsForInitialStep(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	loop, registry := newTestLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return Observation{}, ctx.Err()
	}), clock)
	openReadinessGate(t, registry)

	startResult := make(chan error, 1)
	go func() { startResult <- loop.Start(context.Background()) }()
	<-entered
	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- loop.Shutdown(context.Background()) }()
	<-canceled
	select {
	case err := <-shutdownResult:
		t.Fatalf("Shutdown returned before initial step exited: %v", err)
	default:
	}
	close(release)
	if err := <-shutdownResult; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := <-startResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() error = %v, want cancellation", err)
	}
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("post-shutdown readiness = %#v", readiness)
	}
	select {
	case err := <-loop.Errors():
		t.Fatalf("shutdown surfaced fatal error: %v", err)
	default:
	}
}

func TestLoopParentCancellationDuringInitialStepNeverPublishesReadiness(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	entered := make(chan struct{})
	release := make(chan struct{})
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		close(entered)
		<-release
		return testObservation(), nil
	}), clock)
	openReadinessGate(t, registry)

	ctx, cancel := context.WithCancel(context.Background())
	startResult := make(chan error, 1)
	go func() { startResult <- loop.Start(ctx) }()
	<-entered
	cancel()
	close(release)
	if err := <-startResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() error = %v, want cancellation", err)
	}
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("canceled initial-step readiness = %#v", readiness)
	}
	select {
	case err := <-loop.Errors():
		t.Fatalf("initial cancellation surfaced fatal error: %v", err)
	default:
	}
}

func TestLoopCancellationDuringPollingReadIsNormalAndNonfatal(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	pollEntered := make(chan struct{})
	calls := 0
	loop, registry := newTestLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
		calls++
		if calls == 1 {
			return testObservation(), nil
		}
		close(pollEntered)
		<-ctx.Done()
		return Observation{}, ctx.Err()
	}), clock)
	openReadinessGate(t, registry)
	ctx, cancel := context.WithCancel(context.Background())
	if err := loop.Start(ctx); err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	ticker := clock.ticker
	done := loop.done
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	<-pollEntered
	cancel()
	<-done
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("poll-canceled readiness = %#v", readiness)
	}
	select {
	case err := <-loop.Errors():
		t.Fatalf("poll cancellation surfaced fatal error: %v", err)
	default:
	}
}

func TestLoopUnexpectedTickerCloseIsFatalAndClosesReadiness(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		return testObservation(), nil
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	close(ticker.ticks)
	if err := <-loop.Errors(); !errors.Is(err, ErrTickerClosed) {
		t.Fatalf("Errors() = %v, want %v", err, ErrTickerClosed)
	}
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("ticker-close readiness = %#v", readiness)
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLoopRejectsInvalidConfigAndShutdownStopsTicker(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	stepper := loopStepFunc(func(context.Context, time.Time, int) (Observation, error) { return Observation{}, nil })
	for _, config := range []LoopConfig{
		{PollInterval: minPollInterval, ObservationTimeout: minObservationTimeout, Limit: 1},
		{PollInterval: minPollInterval - time.Nanosecond, ObservationTimeout: minObservationTimeout, Limit: 1, Registry: registry},
		{PollInterval: minPollInterval, ObservationTimeout: minObservationTimeout - time.Nanosecond, Limit: 1, Registry: registry},
		{PollInterval: minPollInterval, ObservationTimeout: maxObservationTimeout + time.Nanosecond, Limit: 1, Registry: registry},
		{PollInterval: minPollInterval, ObservationTimeout: minObservationTimeout, Limit: 0, Registry: registry},
		{PollInterval: minPollInterval, ObservationTimeout: minObservationTimeout, Limit: maximumStepLimit + 1, Registry: registry},
	} {
		if _, err := NewLoop(stepper, config); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("NewLoop(%#v) error = %v", config, err)
		}
	}
	defaultConfig := DefaultLoopConfig(health.NewRegistry(time.Second))
	if defaultConfig.ObservationTimeout != 2*time.Second || defaultConfig.validate() != nil {
		t.Fatalf("DefaultLoopConfig() = %#v", defaultConfig)
	}
	clock := &testClock{now: time.Now()}
	loop, _ := newTestLoop(t, stepper, clock)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
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

func TestMetricsNeverExposeNonFrozenKindLabels(t *testing.T) {
	loop := &Loop{clock: systemClock{}, observation: Observation{Kinds: []KindObservation{{Kind: "tenant-secret", DuePending: 9}}}}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(metrics.String(), "tenant-secret") || strings.Count(metrics.String(), "sync_dispatch_observer_due_pending{kind=") != len(frozenKinds) {
		t.Fatalf("metrics leaked unbounded labels:\n%s", metrics.String())
	}
}

func TestMetricsOmitLastSuccessAgeBeforeFirstSuccess(t *testing.T) {
	loop := &Loop{clock: systemClock{}}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(metrics.String(), "sync_dispatch_observer_last_success_age_seconds") {
		t.Fatalf("pre-success metrics exported a fabricated age:\n%s", metrics.String())
	}
}
