package syncreconciler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
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
		// CHAOS-4097. A healthy pass must publish an explicit zero for both
		// gauges rather than omitting them: an absent series and a quiet one
		// are indistinguishable to a scraper, and "no data" is exactly the
		// state an alert on this cannot afford to confuse with "no problem".
		"sync_dispatch_runaway_dispatch_wakeups 0",
		"sync_dispatch_unreclaimable_candidates 0",
		"# TYPE sync_dispatch_runaway_dispatch_wakeups gauge",
		"# TYPE sync_dispatch_unreclaimable_candidates gauge",
		"bounded Python claim-order window",
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
	// This replaced a blanket `!strings.Contains(metrics, " counter\n")`, whose
	// intent is preserved verbatim from WritePrometheus: "It never accumulates
	// snapshots: counters would misrepresent current queued work after rows are
	// dispatched or claims expire." That rationale is about queued work, and it
	// still holds for every metric it covered. This form is strictly tighter --
	// the old substring check would have accepted a *gauge* named `_total`,
	// which is the instrument that actually lies about a recovery event.
	//
	// Everything derived from an Observation snapshot must stay a gauge: a
	// counter would keep reporting queued work that has since dispatched or
	// expired. The exhausted-delivery total is the one deliberate exception --
	// it counts events that already happened, where a gauge would instead
	// erase the evidence on the next step (CHAOS-3951) -- so it is pinned by
	// name here rather than blanket-permitting counters.
	//
	// CHAOS-4097 adds two more deliberate exceptions, and they are ENUMERATED
	// rather than pattern-matched so that adding a third is a decision someone
	// makes in this list instead of a name that happens to slip through:
	//
	//   - wakeup_report_failures_total counts passes on which the runaway
	//     detector could not run. A gauge would clear on the next pass and
	//     erase the only evidence the measurement layer was ever blind.
	//   - unreclaimable_terminalized_total counts units the sweep destroyed.
	//     Destruction is an event; a gauge would report "0 destroyed" one tick
	//     after it destroyed 100.
	//
	// The two CHAOS-4097 GAUGES (runaway_dispatch_wakeups,
	// unreclaimable_candidates) are deliberately not here: both describe a
	// current condition that must fall back to zero when it clears.
	permittedCounters := map[string]bool{
		"# TYPE sync_dispatch_exhausted_delivery_recoveries_total counter": true,
		"# TYPE sync_dispatch_wakeup_report_failures_total counter":        true,
		"# TYPE sync_dispatch_unreclaimable_terminalized_total counter":    true,
		// The sweep-failure counter is an event count for the same reason:
		// CHAOS-4035's 42501-per-second sweep was survivable for its whole
		// production life precisely because nothing outside a log could see
		// it, and a gauge would clear the evidence on the next pass.
		"# TYPE sync_dispatch_unreclaimable_sweep_failures_total counter": true,
	}
	for _, line := range strings.Split(metrics.String(), "\n") {
		if !strings.HasSuffix(line, " counter") {
			continue
		}
		if !permittedCounters[line] {
			t.Fatalf("unexpected counter metric %q:\n%s", line, metrics.String())
		}
	}
	// Every permitted counter must actually be EMITTED, not merely allowed. A
	// permit list is only a guard while the thing it permits exists; without
	// this, deleting a series would silently pass.
	for permitted := range permittedCounters {
		if !strings.Contains(metrics.String(), permitted) {
			t.Fatalf("permitted counter %q was never emitted:\n%s", permitted, metrics.String())
		}
	}
	if strings.Contains(metrics.String(), PredicateVersion) ||
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

// TestLoopDegradedStageOnInitialStepDoesNotFailStartAndSelfHeals pins
// CHAOS-4239's fix for the ticket's primary observed failure window: the
// first ~40s after each container restart. Before this fix, ANY Step error
// on the very first tick failed Start and took the whole runtime down with
// it -- exactly the crash-loop CHAOS-4239 exists to end. A stage wrapped in
// ErrDegradedStage must instead leave Start (and the process) running,
// leaving only readiness closed until a tick succeeds.
func TestLoopDegradedStageOnInitialStepDoesNotFailStartAndSelfHeals(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)}
	degraded := fmt.Errorf("observer stage: %w: %w", ErrDegradedStage, errors.New("boom"))
	calls := make(chan struct{}, 2)
	callIndex := 0
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		callIndex++
		defer func() { calls <- struct{}{} }()
		if callIndex == 1 {
			return Observation{}, degraded
		}
		return testObservation(), nil
	}), clock)
	openReadinessGate(t, registry)

	if err := loop.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v, want nil: a degraded stage must not fail startup", err)
	}
	<-calls // the degraded initial step
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("readiness after a degraded initial step = %#v, want closed until a tick succeeds", readiness)
	}
	select {
	case err := <-loop.Errors():
		t.Fatalf("degraded initial step unexpectedly surfaced on Errors(): %v", err)
	default:
	}

	clock.mu.Lock()
	clock.now = clock.now.Add(time.Second)
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now()
	<-calls // the next, successful tick
	if readiness := registry.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("readiness after the next successful tick = %#v, want the loop to self-heal", readiness)
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestLoopDegradedStageDuringPollingKeepsTickingAndSelfHeals is the periodic
// counterpart: a degraded tick after Start must not close over Errors() or
// stop the ticker loop the way every OTHER Step error still correctly does
// (TestLoopPeriodicErrorClosesReadinessAndSurfacesError, unchanged). The loop
// keeps ticking, readiness closes for the degraded tick, and the very next
// successful tick reopens it -- the process survives what used to be fatal.
func TestLoopDegradedStageDuringPollingKeepsTickingAndSelfHeals(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)}
	degraded := fmt.Errorf("observer stage: %w: %w", ErrDegradedStage, errors.New("boom"))
	calls := make(chan struct{}, 3)
	callIndex := 0
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		callIndex++
		defer func() { calls <- struct{}{} }()
		if callIndex == 2 {
			return Observation{}, degraded
		}
		return testObservation(), nil
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-calls // initial, successful step
	if readiness := registry.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("readiness after the initial step = %#v, want open", readiness)
	}

	clock.mu.Lock()
	clock.now = clock.now.Add(time.Second)
	ticker := clock.ticker
	clock.mu.Unlock()

	ticker.ticks <- clock.Now()
	<-calls // the degraded tick
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("readiness during a degraded tick = %#v, want closed", readiness)
	}
	select {
	case err := <-loop.Errors():
		t.Fatalf("a degraded tick killed the loop -- Errors() = %v, want the process to survive it", err)
	default:
	}

	ticker.ticks <- clock.Now().Add(time.Second)
	<-calls // the next, successful tick -- proves the loop kept ticking
	if readiness := registry.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("readiness after the recovery tick = %#v, want the loop to self-heal", readiness)
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

// TestLoopWithoutALoggerDoesNotFallBackToSlogDefault proves the nil-logger
// path is inert: a loop given no Logger must not panic on a failed step, and
// it must not fall back to slog.Default() -- that would send output to a
// sink other than the process's configured JSON logger, so a log-capturing
// test could pass while production ships nothing (CHAOS-3907).
func TestLoopWithoutALoggerDoesNotFallBackToSlogDefault(t *testing.T) {
	var buf bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(original)

	failure := errors.New("initial observation probe failure")
	stepper := loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		return Observation{}, failure
	})
	loop, _ := newTestLoop(t, stepper, &testClock{})
	if loop.config.Logger != nil {
		t.Fatal("test loop unexpectedly has a logger")
	}
	if err := loop.Start(context.Background()); err == nil {
		t.Fatal("Start() = nil, want the scripted initial observation failure")
	}
	if buf.Len() != 0 {
		t.Fatalf("nil logger fell back to slog.Default(): %s", buf.String())
	}
}

// TestLoopAccumulatesExhaustedDeliveryRecoveriesAcrossSteps pins the recovery
// total as monotonic. CHAOS-3951's reclaim returns a wedged coordinator
// delivery to 'pending', which is indistinguishable from a healthy row the
// moment it is republished; a repeat of this metric is the only thing that
// separates a run that recovered once from one cycling forever. Reporting the
// last step's count instead of the running total would show zero for every
// scrape that did not land inside the same step as the reclaim.
func TestLoopAccumulatesExhaustedDeliveryRecoveriesAcrossSteps(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)}
	recoveries := []int64{2, 0, 1}
	var index atomic.Int64
	calls := make(chan struct{}, len(recoveries))
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		observation := testObservation()
		position := int(index.Add(1)) - 1
		if position < len(recoveries) {
			observation.ExhaustedDeliveriesRecovered = recoveries[position]
		}
		calls <- struct{}{}
		return observation, nil
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := loop.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	<-calls
	assertRecoveryTotal(t, loop, 2)

	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	for step := 1; step < len(recoveries); step++ {
		ticker.ticks <- clock.Now().Add(time.Duration(step) * time.Second)
		<-calls
	}
	// The middle step recovered nothing; the total must still carry the first
	// step's two, and the last step's one must add rather than replace.
	assertRecoveryTotal(t, loop, 3)
}

func assertRecoveryTotal(t *testing.T, loop *Loop, want int) {
	t.Helper()
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	line := fmt.Sprintf("sync_dispatch_exhausted_delivery_recoveries_total %d\n", want)
	if !strings.Contains(metrics.String(), line) {
		t.Fatalf("metrics missing %q:\n%s", strings.TrimSuffix(line, "\n"), metrics.String())
	}
}

// TestLoopCountsExhaustedDeliveryRecoveriesFromAFailedStep pins the count
// against the step's own outcome. The repair commits before any later stage
// runs, so its reclaims are durable even when the step as a whole fails.
// Counting only successful steps would drop reclaims that happened while the
// pipeline was degraded -- which is precisely when a delivery cycling on
// exhaustion is a plausible cause of the degradation.
func TestLoopCountsExhaustedDeliveryRecoveriesFromAFailedStep(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)}
	calls := make(chan struct{}, 2)
	var steps atomic.Int64
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		observation := testObservation()
		defer func() { calls <- struct{}{} }()
		// The loop refuses to start on a failed initial observation, so the
		// failure under test has to be the second step.
		if steps.Add(1) == 1 {
			return observation, nil
		}
		observation.ExhaustedDeliveriesRecovered = 4
		return observation, ErrUnknownKind
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := loop.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	<-calls
	assertRecoveryTotal(t, loop, 0)
	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	<-calls
	select {
	case err := <-loop.Errors():
		if !errors.Is(err, ErrUnknownKind) {
			t.Fatalf("loop error = %v, want ErrUnknownKind", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loop never reported the failed step")
	}
	assertRecoveryTotal(t, loop, 4)
}

// CHAOS-4097 shipped its reporting as log lines only, justified by "counters
// do not export from this deployment (CHAOS-4094)". That is true of the OTel
// pipeline and false of this one: WritePrometheus is a scrape endpoint and has
// been serving sync_dispatch_exhausted_delivery_recoveries_total all along. A
// signal only an operator reading logs can find is not one an alert can fire
// on, and CHAOS-4093 was precisely a condition nobody was told about for
// twenty-two hours.
//
// The two instrument choices are asserted, not just the values, because the
// choice is the design: a counter that keeps climbing after a runaway clears
// would page forever, and a gauge that resets would erase the evidence a
// destructive sweep ever ran.
func TestMetricsCarryTheRunawayAndSweepSeries(t *testing.T) {
	loop := &Loop{clock: systemClock{}}
	loop.observation = Observation{
		RunawayDispatchWakeups:  7,
		UnreclaimableCandidates: 4,
	}
	loop.wakeupReportFailures = 3
	loop.unreclaimableTerminalized = 11

	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# TYPE sync_dispatch_runaway_dispatch_wakeups gauge",
		"sync_dispatch_runaway_dispatch_wakeups 7",
		"# TYPE sync_dispatch_wakeup_report_failures_total counter",
		"sync_dispatch_wakeup_report_failures_total 3",
		"# TYPE sync_dispatch_unreclaimable_candidates gauge",
		"sync_dispatch_unreclaimable_candidates 4",
		"# TYPE sync_dispatch_unreclaimable_terminalized_total counter",
		"sync_dispatch_unreclaimable_terminalized_total 11",
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
	// The threshold belongs in the HELP text: it is what an operator needs to
	// interpret the gauge, and they are not reading materializer.go at 3am.
	if !strings.Contains(metrics.String(), fmt.Sprintf("exceeded %d attempts", runawayDispatchAttempts)) {
		t.Fatalf("the runaway gauge does not state its threshold:\n%s", metrics.String())
	}
}

// The gauges must FALL BACK to zero when the condition clears. A runaway that
// stayed latched at its worst value would keep an alert firing after the
// incident ended, which is how a real signal becomes one people mute.
func TestRunawayAndCandidateGaugesClearWithTheCondition(t *testing.T) {
	loop := &Loop{clock: systemClock{}}
	loop.observation = Observation{RunawayDispatchWakeups: 9, UnreclaimableCandidates: 5}
	var latched bytes.Buffer
	if err := loop.WritePrometheus(&latched); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(latched.String(), "sync_dispatch_runaway_dispatch_wakeups 9") {
		t.Fatalf("gauge never reported the condition:\n%s", latched.String())
	}

	loop.observation = Observation{}
	var cleared bytes.Buffer
	if err := loop.WritePrometheus(&cleared); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sync_dispatch_runaway_dispatch_wakeups 0",
		"sync_dispatch_unreclaimable_candidates 0",
	} {
		if !strings.Contains(cleared.String(), want) {
			t.Fatalf("gauge stayed latched after the condition cleared, missing %q:\n%s",
				want, cleared.String())
		}
	}
}

// accumulateCount is the shared arithmetic behind all three counters, and both
// refusals it makes are safety properties rather than tidiness: a counter that
// goes BACKWARDS makes every rate() over it report a spike that never
// happened, which is worse than one that stops moving.
func TestAccumulateCountRefusesNegativesAndOverflow(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		total uint64
		delta int64
		want  uint64
	}{
		{"ordinary add", 5, 3, 8},
		{"zero is a no-op", 5, 0, 5},
		{"a negative count cannot be represented and is refused", 5, -2, 5},
		{"overflow would wrap and read as a drop, so it is refused", math.MaxUint64 - 1, 5, math.MaxUint64 - 1},
		{"an exact fit still lands", math.MaxUint64 - 5, 5, math.MaxUint64},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := accumulateCount(testCase.total, testCase.delta); got != testCase.want {
				t.Fatalf("accumulateCount(%d, %d) = %d, want %d",
					testCase.total, testCase.delta, got, testCase.want)
			}
		})
	}
}

// The counters must accumulate through the LOOP, not merely be printable when
// set by hand. A red control caught this gap: mutating
// accumulateRecoveriesLocked to drop the new counters left every other test in
// this file green, because they all wrote the field directly. A metric whose
// only proven path is the test's own assignment is not instrumented at all.
//
// Both new counters are driven together, and both are asserted to ADD rather
// than replace across steps -- the same property CHAOS-3951 needed from the
// exhausted-delivery total, for the same reason: a scrape that does not land
// inside the step that saw the event would otherwise report zero.
func TestLoopAccumulatesTheRunawayAndSweepCountersAcrossSteps(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)}
	calls := make(chan struct{}, 4)
	var index atomic.Int64
	// A middle step with nothing to add proves the total is not simply the
	// last step's value wearing a counter's name.
	failures := []int64{1, 0, 1}
	terminalized := []int64{4, 0, 3}
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		observation := testObservation()
		position := int(index.Add(1)) - 1
		if position < len(failures) {
			observation.WakeupReportFailures = failures[position]
			observation.UnreclaimableTerminalized = terminalized[position]
			observation.UnreclaimableSweepFailures = failures[position]
		}
		calls <- struct{}{}
		return observation, nil
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := loop.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	<-calls
	assertMetricLine(t, loop, "sync_dispatch_wakeup_report_failures_total 1")
	assertMetricLine(t, loop, "sync_dispatch_unreclaimable_terminalized_total 4")
	assertMetricLine(t, loop, "sync_dispatch_unreclaimable_sweep_failures_total 1")

	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	for step := 1; step < len(failures); step++ {
		ticker.ticks <- clock.Now().Add(time.Duration(step) * time.Second)
		<-calls
	}
	assertMetricLine(t, loop, "sync_dispatch_wakeup_report_failures_total 2")
	assertMetricLine(t, loop, "sync_dispatch_unreclaimable_terminalized_total 7")
	assertMetricLine(t, loop, "sync_dispatch_unreclaimable_sweep_failures_total 2")
}

// assertMetricLine waits for the scrape to show a line rather than sampling it
// once, and the difference is a race I shipped into CI.
//
// The loop-driven tests signal from INSIDE the stepper, so the test resumes
// while the loop goroutine is still between "stepper returned" and
// "observation stored". Reading the scrape at that instant samples whichever
// side of the store the scheduler happened to be on. It passed locally and
// failed on CI, which is the signature of the whole class.
//
// Polling the exported state is a wait on the thing itself, not on a proxy
// for it. The deadline is a test-harness bound, not a correctness knob: a
// value that is going to appear appears in microseconds, and one that never
// appears fails with the same message it always did.
func assertMetricLine(t *testing.T, loop *Loop, want string) {
	t.Helper()
	var metrics bytes.Buffer
	deadline := time.Now().Add(5 * time.Second)
	for {
		metrics.Reset()
		if err := loop.WritePrometheus(&metrics); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(metrics.String(), want+"\n") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics never showed %q:\n%s", want, metrics.String())
		}
		time.Sleep(time.Millisecond)
	}
}

// assertMetricLineStable is for the opposite claim -- that a value did NOT
// change. Polling cannot express that on its own, because the value being
// asserted is already present, so the caller must first wait for evidence the
// pass under test has been processed. Both are stored under one lock, so a
// counter that has moved proves the gauge merge beside it has run too.
func assertMetricLineStable(t *testing.T, loop *Loop, processed, want string) {
	t.Helper()
	assertMetricLine(t, loop, processed)
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics.String(), want+"\n") {
		t.Fatalf("the pass was processed and %q did not survive it:\n%s", want, metrics.String())
	}
}

// A SWEEP THAT HAS STOPPED WORKING MUST BE VISIBLE TO A DASHBOARD
// (adversarial review finding).
//
// The pipeline deliberately does not fail a pass when the sweep errors --
// taking lease repair down with the safety net would trade a bounded strand
// for an unbounded one -- so a persistent permission, schema or connection
// fault leaves the observer healthy and the candidate gauge at zero, which is
// exactly what a system with nothing to sweep reports.
//
// This is not a hypothetical failure mode. CHAOS-4035 was this component
// answering 42501 once a second from its first production deploy, and what
// let that survive was that nothing but a log line could see it.
func TestMetricsCarryTheSweepFailureCounter(t *testing.T) {
	loop := &Loop{clock: systemClock{}}
	loop.unreclaimableSweepFailures = 5

	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# TYPE sync_dispatch_unreclaimable_sweep_failures_total counter",
		"sync_dispatch_unreclaimable_sweep_failures_total 5",
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
	// The pairing has to be discoverable from the metric itself: a zero
	// candidate gauge means nothing while this counter is moving.
	if !strings.Contains(metrics.String(), "Unproven while sync_dispatch_unreclaimable_sweep_failures_total is climbing") {
		t.Fatalf("the candidate gauge does not name its staleness signal:\n%s", metrics.String())
	}
}

// THE DEGRADED PASS IS THE INCIDENT (adversarial review finding).
//
// MutationPipeline carries the runaway and sweep figures through every return
// because both components commit before later stages run. If Loop then only
// stored observations from SUCCESSFUL passes, a kernel or observer fault
// arriving just after a pass measured a runaway would leave Prometheus showing
// the last healthy snapshot — hiding the incident during precisely the pass
// where the system is already misbehaving.
//
// The principle is not new here: the ErrUnknownKind branch already keeps a
// failed observation's bounded total "as valuable operator evidence" while
// readiness carries the health signal. This applies it to the new gauges.
func TestFailedPassStillPublishesTheRunawayAndSweepGauges(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)}
	failure := errors.New("scripted kernel failure after the report was taken")
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		// Exactly the shape MutationPipeline returns from a late failure: the
		// already-committed figures, plus the error.
		return Observation{
			RunawayDispatchWakeups:  83,
			UnreclaimableCandidates: 12,
			// Both were genuinely taken before the later stage faulted, which
			// is what makes them publishable.
			RunawayMeasured:       true,
			UnreclaimableMeasured: true,
		}, failure
	}), clock)
	openReadinessGate(t, registry)

	if err := loop.Start(context.Background()); err == nil {
		t.Fatal("Start() = nil, want the scripted failure")
	}

	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sync_dispatch_runaway_dispatch_wakeups 83",
		"sync_dispatch_unreclaimable_candidates 12",
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Fatalf("a degraded pass dropped its measured evidence, missing %q:\n%s",
				want, metrics.String())
		}
	}
	// ...and it must still say the process is unhealthy. Publishing the
	// evidence must not launder a failed pass into a healthy one.
	if !strings.Contains(metrics.String(), "sync_dispatch_observer_up 0") {
		t.Fatalf("a failed pass reported the observer healthy:\n%s", metrics.String())
	}
}

// NON-VACUITY for the merge: a failed pass must NOT publish the observer's own
// queue gauges, which are unreliable when the pass did not complete. Trading
// one stale number for several wrong ones is not an improvement.
func TestFailedPassDoesNotPublishTheObserversOwnGauges(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)}
	failure := errors.New("scripted failure carrying junk queue figures")
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		return Observation{
			RunawayDispatchWakeups: 4,
			RunawayMeasured:        true,
			// A half-built observation from an aborted pass.
			CeleryDuePending:  999,
			RiverDuePending:   999,
			SampledCandidates: 999,
		}, failure
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err == nil {
		t.Fatal("Start() = nil, want the scripted failure")
	}

	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics.String(), "sync_dispatch_runaway_dispatch_wakeups 4") {
		t.Fatalf("the merged gauge did not survive:\n%s", metrics.String())
	}
	if strings.Contains(metrics.String(), "999") {
		t.Fatalf("a failed pass published the observer's unreliable queue figures:\n%s",
			metrics.String())
	}
}

// AN UNMEASURED PASS MUST NOT CLEAR A REAL INCIDENT (adversarial review
// finding, and the inverse of the one before it).
//
// Two rounds argued opposite sides of this single field: one that a failed
// pass must publish its gauges because the degraded pass is where the incident
// is, the next that a failed pass must not, because overwriting 83 with an
// unmeasured zero clears a gauge-based alert exactly while measurement is
// unavailable. Both are right, and the conflict only exists while one number
// is asked to mean two things — "I looked and found none" versus "I did not
// look".
//
// The measured bit settles both. This test is the second half; the pair above
// is the first, and neither passes without the other under the wrong design.
func TestAnUnmeasuredPassLeavesTheLastMeasuredGaugeStanding(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)}
	var steps atomic.Int64
	calls := make(chan struct{}, 3)
	failure := errors.New("scripted detector outage")
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		defer func() { calls <- struct{}{} }()
		if steps.Add(1) == 1 {
			// A healthy pass that measured a real incident.
			observation := testObservation()
			observation.RunawayDispatchWakeups = 83
			observation.UnreclaimableCandidates = 12
			observation.RunawayMeasured = true
			observation.UnreclaimableMeasured = true
			return observation, nil
		}
		// The detector then goes down: zero values, and nothing measured them.
		return Observation{WakeupReportFailures: 1, UnreclaimableSweepFailures: 1}, failure
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loop.Shutdown(context.Background()) }()
	<-calls
	assertMetricLine(t, loop, "sync_dispatch_runaway_dispatch_wakeups 83")

	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	<-calls

	// The failure counter moving is the proof the unmeasured pass was
	// processed; it and the gauge merge are stored under one lock, so once it
	// reads 1 the merge has already run or been declined.
	assertMetricLineStable(t, loop,
		"sync_dispatch_wakeup_report_failures_total 1",
		"sync_dispatch_runaway_dispatch_wakeups 83")
	assertMetricLineStable(t, loop,
		"sync_dispatch_unreclaimable_sweep_failures_total 1",
		"sync_dispatch_unreclaimable_candidates 12")
	// The staleness is declared, so nobody mistakes the held value for a
	// fresh one.
	assertMetricLine(t, loop, "sync_dispatch_observer_up 0")
}

// ...and a MEASURED zero must still clear it, or the gauge latches forever and
// a resolved incident never resolves on the dashboard.
func TestAMeasuredZeroClearsTheGauge(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)}
	var steps atomic.Int64
	calls := make(chan struct{}, 3)
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		defer func() { calls <- struct{}{} }()
		observation := testObservation()
		observation.RunawayMeasured = true
		observation.UnreclaimableMeasured = true
		if steps.Add(1) == 1 {
			observation.RunawayDispatchWakeups = 83
			observation.UnreclaimableCandidates = 12
		}
		return observation, nil
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loop.Shutdown(context.Background()) }()
	<-calls
	assertMetricLine(t, loop, "sync_dispatch_runaway_dispatch_wakeups 83")

	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	<-calls
	assertMetricLine(t, loop, "sync_dispatch_runaway_dispatch_wakeups 0")
	assertMetricLine(t, loop, "sync_dispatch_unreclaimable_candidates 0")
}

// THE FAILURES THAT MATTER MOST HERE ARE NOT ERRORS (adversarial review
// finding, and the gap left by gating only the error path).
//
// The pipeline deliberately swallows a sweep failure — taking lease repair
// down with the safety net would trade a bounded strand for an unbounded one —
// and a failed runaway report likewise returns a SUCCESSFUL step. Both
// therefore arrive on the success path carrying zero gauges with Measured
// false, where the wholesale snapshot store clobbered a measured 83 with them
// while sync_dispatch_observer_up still read 1.
//
// That is the worst of the three variants: the alert clears on missing data,
// and nothing anywhere claims to be degraded.
func TestASwallowedFailureOnASuccessfulPassDoesNotClearTheGauges(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)}
	var steps atomic.Int64
	calls := make(chan struct{}, 3)
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		defer func() { calls <- struct{}{} }()
		observation := testObservation()
		if steps.Add(1) == 1 {
			observation.RunawayDispatchWakeups = 83
			observation.UnreclaimableCandidates = 12
			observation.RunawayMeasured = true
			observation.UnreclaimableMeasured = true
			return observation, nil
		}
		// Exactly what MutationPipeline returns when the sweep errors and the
		// report cannot run: a NIL error, zero gauges, nothing measured.
		observation.WakeupReportFailures = 1
		observation.UnreclaimableSweepFailures = 1
		return observation, nil
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loop.Shutdown(context.Background()) }()
	<-calls
	assertMetricLine(t, loop, "sync_dispatch_runaway_dispatch_wakeups 83")

	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	<-calls

	// The failure counters are the only staleness signal on this path, which
	// is exactly why they must move -- and why they are the processed-proof.
	assertMetricLineStable(t, loop,
		"sync_dispatch_wakeup_report_failures_total 1",
		"sync_dispatch_runaway_dispatch_wakeups 83")
	assertMetricLineStable(t, loop,
		"sync_dispatch_unreclaimable_sweep_failures_total 1",
		"sync_dispatch_unreclaimable_candidates 12")
	// The observer's OWN figures must still update on a successful pass --
	// holding those would be the opposite bug.
	assertMetricLine(t, loop, "sync_dispatch_observer_up 1")
}

// SHADOW MODE'S SIGNAL, end to end, because shadow is the DEFAULT and it
// writes nothing at all: the candidate gauge is the only durable trace a
// shadow deployment leaves. If it never moves, a shadow sweep selecting the
// same strand on every tick is invisible to every dashboard, which is the
// state CHAOS-4097 already had to fix once at the log layer.
//
// The gauge must MOVE under a synthesized selection, not merely be present in
// the scrape, and the terminalized counter must stay flat — shadow selects
// without destroying, and a counter that moved here would report work that
// never happened.
func TestShadowSelectionMovesTheCandidateGaugeWithoutTheTerminalizedCounter(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)}
	var steps atomic.Int64
	calls := make(chan struct{}, 3)
	loop, registry := newTestLoop(t, loopStepFunc(func(context.Context, time.Time, int) (Observation, error) {
		defer func() { calls <- struct{}{} }()
		observation := testObservation()
		observation.UnreclaimableMeasured = true
		observation.RunawayMeasured = true
		if steps.Add(1) > 1 {
			// A shadow pass that WOULD have terminalized seven units.
			observation.UnreclaimableCandidates = 7
			observation.UnreclaimableTerminalized = 0
		}
		return observation, nil
	}), clock)
	openReadinessGate(t, registry)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loop.Shutdown(context.Background()) }()
	<-calls
	// Baseline: nothing selected yet. Without this the assertion below could
	// pass against a gauge that was always 7.
	assertMetricLine(t, loop, "sync_dispatch_unreclaimable_candidates 0")

	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now().Add(time.Second)
	<-calls

	// Shadow destroys nothing, so the counter must not have moved. The gauge
	// reaching 7 is the proof the selecting pass was processed.
	assertMetricLineStable(t, loop,
		"sync_dispatch_unreclaimable_candidates 7",
		"sync_dispatch_unreclaimable_terminalized_total 0")
}

// THE DEADLINE PATH ALSO CARRIES COMMITTED EVIDENCE (adversarial review).
//
// A pipeline can commit sweep and materializer work and then have the bounded
// observation context expire on its way out. The deadline return used to sit
// ahead of the gauge merge, so that evidence was discarded and Prometheus held
// the prior value — possibly zero — while the pass was already unhealthy.
//
// The merge now happens once, ahead of every return, which is why this passes
// without a branch of its own.
func TestADeadlinePassStillCarriesItsMeasuredGauges(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)}
	loop, registry := newTestLoopWithTimeout(t, loopStepFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
		// Work committed, then the bounded context expires on the way out.
		observation := Observation{
			RunawayDispatchWakeups:  83,
			UnreclaimableCandidates: 12,
			RunawayMeasured:         true,
			UnreclaimableMeasured:   true,
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("the step context carried no deadline, so this proves nothing")
		}
		// Wait on the CONTEXT, never on the wall clock. The first cut of this
		// slept until time.Now() passed ctx.Deadline(), which is a proxy for
		// the state it actually needed: cancellation is delivered by a timer,
		// so the wall clock can pass the deadline a hair before ctx.Err()
		// becomes non-nil. The loop then saw a nil error and the test failed
		// on CI roughly one run in three while passing locally every time.
		//
		// ctx.Done() closes exactly when cancellation is observable, so
		// ctx.Err() is guaranteed non-nil after it and there is no race left
		// to lose.
		<-ctx.Done()
		return observation, nil
	}), clock, 10*time.Millisecond)
	openReadinessGate(t, registry)

	if err := loop.Start(context.Background()); err == nil {
		t.Fatal("Start() = nil, want the deadline error")
	}

	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sync_dispatch_runaway_dispatch_wakeups 83",
		"sync_dispatch_unreclaimable_candidates 12",
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Fatalf("a deadline discarded evidence of work that already committed, "+
				"missing %q:\n%s", want, metrics.String())
		}
	}
}
