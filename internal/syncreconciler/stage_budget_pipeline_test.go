package syncreconciler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// budgetsWithOverride starts from DefaultStageBudgets and replaces one
// entry, keeping the set exactly the six stages MutationPipelineConfig.valid
// requires.
func budgetsWithOverride(stage StageName, budget time.Duration) StageBudgets {
	budgets := DefaultStageBudgets()
	budgets[stage] = budget
	return budgets
}

func pipelineConfigWithBudget(stage StageName, budget time.Duration) MutationPipelineConfig {
	config := DefaultMutationPipelineConfig()
	config.StageBudgets = budgetsWithOverride(stage, budget)
	return config
}

// TestRunStageBudgetIsIndependentOfItsSiblings is CHAOS-4239's core claim: a
// stage that blocks past ITS OWN (short) budget is bounded by that budget
// alone, not by whatever the whole pipeline call happens to be wrapped in --
// its five siblings, given generous budgets, are neither starved nor slowed
// by it. Materializer is a continue-safe stage (see the classification
// comment on MutationPipeline.Step), so the tick still succeeds: kernel and
// the observer both still run, and Step returns nil.
func TestRunStageBudgetIsIndependentOfItsSiblings(t *testing.T) {
	var kernelRan, observerRan bool
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(ctx context.Context, _ time.Time, _ time.Time, _ int) (MaterializerResult, error) {
			// Respects its context like every real stage does; it simply has
			// nothing left to wait for once its own short budget expires.
			<-ctx.Done()
			return MaterializerResult{}, ctx.Err()
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			kernelRan = true
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			observerRan = true
			return Observation{}, nil
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		nil,
		noopTerminalOutboxClose(),
		pipelineConfigWithBudget(StageMaterializer, 5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}

	start := time.Now()
	_, stepErr := pipeline.Step(context.Background(), time.Now().UTC(), 10)
	elapsed := time.Since(start)

	if stepErr != nil {
		t.Fatalf("Step() error = %v, want nil: materializer is continue-safe", stepErr)
	}
	if !kernelRan || !observerRan {
		t.Fatalf("a stage exceeding its own budget blocked its siblings: kernel=%v observer=%v", kernelRan, observerRan)
	}
	// A generous ceiling: this pins that the WHOLE call did not wait out some
	// leftover flat deadline, only materializer's 5ms one. Any budget well
	// under the package's default stage budgets (hundreds of ms) proves the
	// point without being a flaky low-single-digit-millisecond assertion.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("Step() took %s; a 5ms stage budget should not let a blocked stage stall the whole call", elapsed)
	}

	failures, _ := pipeline.stages.snapshot()
	if failures[StageMaterializer] != 1 {
		t.Fatalf("stage failure count for materializer = %d, want 1", failures[StageMaterializer])
	}
}

// TestObserverExceedingItsBudgetDegradesRatherThanFails pins the one stage
// whose own failure Step still reports as an error (wrapped in
// ErrDegradedStage, not a bare error) -- see the classification comment on
// MutationPipeline.Step for why the observer is different from its five
// siblings.
func TestObserverExceedingItsBudgetDegradesRatherThanFails(t *testing.T) {
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
			<-ctx.Done()
			return Observation{}, ctx.Err()
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		nil,
		noopTerminalOutboxClose(),
		pipelineConfigWithBudget(StageObserver, 5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	_, stepErr := pipeline.Step(context.Background(), time.Now().UTC(), 10)
	if stepErr == nil {
		t.Fatal("Step() = nil, want the observer's degraded error")
	}
	if !errors.Is(stepErr, ErrDegradedStage) {
		t.Fatalf("Step() error = %v, want it wrapped in ErrDegradedStage so Loop.run keeps ticking instead of tearing the process down", stepErr)
	}
	if !errors.Is(stepErr, context.DeadlineExceeded) {
		t.Fatalf("Step() error = %v, want the underlying deadline preserved via %%w", stepErr)
	}
}

// TestStageFailureIsTelemetered pins CHAOS-4239's "mandatory telemetry in the
// same PR" requirement: a structured log line and the WritePrometheus
// fragment must both carry the failure and its duration.
func TestStageFailureIsTelemetered(t *testing.T) {
	captured, restore := captureSlogRecords(t)
	defer restore()

	failure := errors.New("scripted materializer failure")
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
			return MaterializerResult{}, failure
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			return Observation{}, nil
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		nil,
		noopTerminalOutboxClose(),
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	if _, err := pipeline.Step(context.Background(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("materializer is continue-safe, Step() error = %v, want nil", err)
	}

	record, found := findSlogRecord(*captured, "syncreconciler.stage_failed")
	if !found {
		t.Fatal("no syncreconciler.stage_failed log record")
	}
	if record["stage"] != string(StageMaterializer) {
		t.Fatalf("record stage = %v, want %q", record["stage"], StageMaterializer)
	}
	if record["error"] != failure.Error() {
		t.Fatalf("record error = %v, want %q", record["error"], failure.Error())
	}
	if _, ok := record["budget_ms"]; !ok {
		t.Fatal("record missing budget_ms")
	}
	if _, ok := record["elapsed_ms"]; !ok {
		t.Fatal("record missing elapsed_ms")
	}

	var metrics strings.Builder
	if err := pipeline.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	text := metrics.String()
	for _, want := range []string{
		`sync_reconciler_stage_failures_total{stage="materializer"} 1`,
		`sync_reconciler_stage_failures_total{stage="observer"} 0`,
		`sync_reconciler_stage_budget_seconds{stage="kernel"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
	if !strings.Contains(text, `sync_reconciler_stage_duration_seconds{stage="materializer"}`) {
		t.Fatalf("metrics missing the materializer duration gauge:\n%s", text)
	}
}

// TestStageCancellationSQLStateIsTelemetered pins CHAOS-4262's fix for the
// masking pattern CHAOS-4239's telemetry left in place: every stage failure
// -- a real statement cancellation, a permission fault, an actual outage --
// folds into the same ErrUnavailable/"database unavailable" text. The
// structured log line and a dedicated counter must both carry the recovered
// SQLSTATE distinctly, and the duration histogram must observe the pass
// regardless of outcome.
func TestStageCancellationSQLStateIsTelemetered(t *testing.T) {
	captured, restore := captureSlogRecords(t)
	defer restore()

	canceled := materializerUnavailable(materializerStepFinalize, &pgconn.PgError{
		Code: "57014", Message: "canceling statement due to statement timeout",
	})
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
			return MaterializerResult{}, canceled
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			return Observation{}, nil
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		nil,
		noopTerminalOutboxClose(),
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	if _, err := pipeline.Step(context.Background(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("materializer is continue-safe, Step() error = %v, want nil", err)
	}

	record, found := findSlogRecord(*captured, "syncreconciler.stage_failed")
	if !found {
		t.Fatal("no syncreconciler.stage_failed log record")
	}
	if record["sqlstate"] != "57014" {
		t.Fatalf("record sqlstate = %v, want 57014", record["sqlstate"])
	}
	// The driver's own message must never reach the log line.
	if strings.Contains(record["error"].(string), "canceling statement") {
		t.Fatalf("record error leaked driver text: %v", record["error"])
	}

	var metrics strings.Builder
	if err := pipeline.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	text := metrics.String()
	for _, want := range []string{
		`dev_health_reconciler_stage_cancelled_total{stage="materializer",sqlstate="57014"} 1`,
		`dev_health_reconciler_stage_duration_seconds_count{stage="materializer"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, `sqlstate="57014",sqlstate=`) {
		t.Fatalf("cancellation counter emitted a duplicate label:\n%s", text)
	}
	if !strings.Contains(text, `dev_health_reconciler_stage_duration_seconds_bucket{stage="materializer",le="+Inf"} 1`) {
		t.Fatalf("metrics missing the materializer duration histogram +Inf bucket:\n%s", text)
	}
}

// TestRunStageDoesNotTelemeterParentCancellation pins runStage's exclusion:
// a failure caused by the LOOP's own shutdown (parent context canceled), not
// by this stage's own tighter budget, must not be counted or logged as a
// stage failure -- it is the process stopping, not a stage degrading -- and
// Step must propagate it as a plain error rather than swallowing it.
func TestRunStageDoesNotTelemeterParentCancellation(t *testing.T) {
	captured, restore := captureSlogRecords(t)
	defer restore()

	ctx, cancel := context.WithCancel(context.Background())

	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			// Simulates the loop shutting down WHILE this stage is in
			// flight: the parent ctx is canceled from outside, not by this
			// stage's own (unrelated) budget expiring.
			cancel()
			return LeaseRepairResult{}, context.Canceled
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			t.Fatal("must not run after the parent context was already canceled")
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
			t.Fatal("must not run after the parent context was already canceled")
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			t.Fatal("must not run after the parent context was already canceled")
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			t.Fatal("must not run after the parent context was already canceled")
			return Observation{}, nil
		}),
		nil,
		nil,
		nil,
		noopTerminalOutboxClose(),
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	if _, stepErr := pipeline.Step(ctx, time.Now().UTC(), 10); !errors.Is(stepErr, context.Canceled) {
		t.Fatalf("Step() error = %v, want context.Canceled propagated (shutdown, not a stage failure)", stepErr)
	}
	// Confirmed above via the t.Fatal calls the other stages must never
	// reach: repair canceling the parent context must abort the rest of the
	// tick exactly the way any other repair failure does.
	if _, found := findSlogRecord(*captured, "syncreconciler.stage_failed"); found {
		t.Fatal("parent cancellation must not be telemetered as a stage failure")
	}
}

// TestStageDegradesReadinessAfterThreeConsecutiveFailuresAndClearsOnSuccess
// pins chris's CHAOS-4239 readiness ruling: counters alone are not enough --
// a stage failing on EVERY tick (the ticket's own cold-start symptom shape)
// must be visible on readyz BY STAGE NAME, not only to someone reading a
// Prometheus counter. But a single blip must not flap overall readiness, so
// the threshold is consecutiveFailureDegradeThreshold (3) failures in a row,
// and one success clears it immediately.
func TestStageDegradesReadinessAfterThreeConsecutiveFailuresAndClearsOnSuccess(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	openReadinessGate(t, registry)

	materializerCalls := 0
	config := DefaultMutationPipelineConfig()
	config.Registry = registry
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
			materializerCalls++
			if materializerCalls <= consecutiveFailureDegradeThreshold {
				return MaterializerResult{}, errors.New("scripted materializer failure")
			}
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			return Observation{}, nil
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		nil,
		noopTerminalOutboxClose(),
		config,
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}

	for tick := 1; tick <= consecutiveFailureDegradeThreshold; tick++ {
		if _, stepErr := pipeline.Step(context.Background(), time.Now().UTC(), 10); stepErr != nil {
			t.Fatalf("tick %d: Step() error = %v, want nil: materializer is continue-safe", tick, stepErr)
		}
	}
	// A single blip must not have already flapped readiness -- only the
	// THIRD consecutive failure crosses the threshold, so check it stayed
	// open through ticks 1 and 2 by construction (nothing to assert
	// mid-loop here beyond Step() succeeding, already checked above); the
	// real assertion is the state after all three.
	readiness := registry.Readiness(context.Background())
	if readiness.Ready {
		t.Fatalf("readiness after %d consecutive materializer failures = %#v, want degraded",
			consecutiveFailureDegradeThreshold, readiness)
	}
	wantName := stageReadinessCheckName(StageMaterializer)
	found := false
	for _, check := range readiness.Checks {
		if check.Name != wantName {
			continue
		}
		found = true
		if !check.Failed {
			t.Fatalf("check %q = %#v, want Failed", wantName, check)
		}
	}
	if !found {
		t.Fatalf("readyz did not name the degraded stage %q: %#v", wantName, readiness.Checks)
	}

	// One success clears it immediately -- no separate recovery threshold.
	if _, stepErr := pipeline.Step(context.Background(), time.Now().UTC(), 10); stepErr != nil {
		t.Fatalf("recovery tick: Step() error = %v, want nil", stepErr)
	}
	readiness = registry.Readiness(context.Background())
	if !readiness.Ready {
		t.Fatalf("readiness after the materializer's first success = %#v, want open", readiness)
	}
}

// TestExplicitOuterEnvelopeBelowStageSumDegradesRatherThanKillsTheProcess is
// codex adversarial-review round 2's finding on CHAOS-4239, proven end to
// end through a real MutationPipeline wrapped in a real Loop (loop_test.go's
// TestLoopPeriodicObservationDeadlineDegradesAndSelfHeals already covers the
// generic Loop-level mechanism with a synthetic Stepper; this closes the
// same gap at the seam codex actually flagged).
//
// An operator can explicitly configure LoopConfig.ObservationTimeout below
// what the mutation pipeline's composed default stage-budget sum needs
// (dependencies.go WARNs but still honors that choice --
// config.Config.SyncObservationTimeoutExplicit). In that case the OUTER
// envelope, not any single stage's own (comfortably sufficient) budget, is
// what trips first. Before ErrStepEnvelopeExceeded this reproduced the exact
// crash-loop CHAOS-4239 exists to end, for the one input path its own
// precedence fix deliberately still allows through.
func TestExplicitOuterEnvelopeBelowStageSumDegradesRatherThanKillsTheProcess(t *testing.T) {
	clock := &testClock{now: time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)}

	// Every stage sleeps briefly, respecting ctx, comfortably inside its own
	// generous default per-stage budget (400ms-1000ms) -- no stage fails on
	// its own -- but five stages at 30ms each sum to ~150ms, well past a
	// deliberately small 100ms outer envelope.
	sleepBriefly := func(ctx context.Context) {
		select {
		case <-time.After(30 * time.Millisecond):
		case <-ctx.Done():
		}
	}
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(ctx context.Context, _ time.Time, _ int) (LeaseRepairResult, error) {
			sleepBriefly(ctx)
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(ctx context.Context, _ time.Time, _ int) (TerminalDeliveryRepairResult, error) {
			sleepBriefly(ctx)
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(ctx context.Context, _ time.Time, _ time.Time, _ int) (MaterializerResult, error) {
			sleepBriefly(ctx)
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(ctx context.Context, _ time.Time, _ int, _ time.Duration, _ AtLeastOncePublisher, _ PostSyncHandoff) (KernelResult, error) {
			sleepBriefly(ctx)
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
			sleepBriefly(ctx)
			return testObservation(), nil
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		nil,
		noopTerminalOutboxClose(),
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}

	// The explicit, WARNED-but-honored operator choice: below
	// DefaultStageBudgets().Sum(), reproducing exactly the configuration
	// dependencies.go's SyncObservationTimeoutExplicit path allows through.
	loop, registry := newTestLoopWithTimeout(t, pipeline, clock, 100*time.Millisecond)
	openReadinessGate(t, registry)

	if startErr := loop.Start(context.Background()); startErr != nil {
		t.Fatalf("Start() error = %v, want nil: an outer envelope below the stage-budget sum must degrade, not fail startup", startErr)
	}
	select {
	case fatal := <-loop.Errors():
		t.Fatalf("an explicit outer envelope below the stage-budget sum killed the process: %v", fatal)
	case <-time.After(50 * time.Millisecond):
	}
	if readiness := registry.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("readiness after the outer-envelope tick = %#v, want closed", readiness)
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestStageIgnoringContextFlipsReadinessAndClearsOnReturn is chris's
// CHAOS-4239 round-3 requirement: every non-fatal degradation mechanism this
// file builds (ErrDegradedStage, ErrStepEnvelopeExceeded, the
// consecutive-failure degrade) depends on a stage's call actually
// RETURNING. A stage that ignores its own stageCtx and blocks forever
// produces no failure, no success, and no duration sample -- it would be
// completely invisible, trading CHAOS-4239's loud pre-fix crash-loop for a
// silent zombie hang.
//
// This stubs a stage that deliberately never checks ctx.Done() and blocks
// on a channel the test controls, proving detectOverrun (evaluated lazily
// from the readiness-check goroutine, not a background timer, since the
// goroutine actually running the stub stays genuinely blocked the whole
// time) flips readyz -- naming the exact stage -- once its budget plus
// margin has elapsed, and clears the moment the call is unblocked and
// returns.
func TestStageIgnoringContextFlipsReadinessAndClearsOnReturn(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	openReadinessGate(t, registry)

	entered := make(chan struct{})
	unblock := make(chan struct{})
	materializerCalls := 0

	config := DefaultMutationPipelineConfig()
	config.Registry = registry
	// A tiny budget keeps the test fast: the watchdog's threshold is
	// budget+stepOverrunMargin, and stepOverrunMargin (250ms) dominates
	// regardless, so this does not need to be unrealistically small to prove
	// the mechanism -- it only needs to not be the multi-hundred-ms default.
	const materializerBudget = 10 * time.Millisecond
	config.StageBudgets = budgetsWithOverride(StageMaterializer, materializerBudget)

	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(ctx context.Context, _ time.Time, _ time.Time, _ int) (MaterializerResult, error) {
			materializerCalls++
			if materializerCalls == 1 {
				// Deliberately does NOT select on ctx.Done() -- this is
				// exactly the bug class this test exists to catch: a call
				// that ignores cancellation entirely, the way a wedged
				// driver or a genuine deadlock would.
				close(entered)
				<-unblock
			}
			return MaterializerResult{}, nil
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			return Observation{}, nil
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		nil,
		noopTerminalOutboxClose(),
		config,
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}

	stepDone := make(chan struct{})
	go func() {
		defer close(stepDone)
		pipeline.Step(context.Background(), time.Now().UTC(), 10)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("materializer stub never started")
	}

	// PROVE THE WATCHDOG IS ARMED, NOT LAZY (chris's round-3 follow-up
	// review): sleep past the threshold WITHOUT ever touching readiness
	// during the wait. If detection depended on something polling
	// registry.Readiness (the earlier, rejected design), nothing would have
	// happened yet no matter how long this sleeps -- the state below has to
	// already be set by the time this reads it for the first time.
	time.Sleep(materializerBudget + stepOverrunMargin + 100*time.Millisecond)

	wantName := stageReadinessCheckName(StageMaterializer)
	readiness := registry.Readiness(context.Background()) // first-ever touch
	if readiness.Ready {
		t.Fatalf("readiness = %#v, want closed: the watchdog should have fired unpolled", readiness)
	}
	found := false
	for _, check := range readiness.Checks {
		if check.Name == wantName && check.Failed {
			found = true
		}
	}
	if !found {
		t.Fatalf("readyz did not name the stuck materializer stage: %#v", readiness.Checks)
	}

	var metrics strings.Builder
	if err := pipeline.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics.String(), `sync_reconciler_step_overrun_total{stage="materializer"} 1`) {
		t.Fatalf("metrics missing the overrun counter:\n%s", metrics.String())
	}

	// Unblock the stuck call -- the operator fixed it, or it was never truly
	// infinite, whichever story this maps to in production -- and confirm
	// readiness clears once it actually returns.
	close(unblock)
	select {
	case <-stepDone:
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline.Step never returned after the stub was unblocked")
	}
	// markIdle clears overrunActive synchronously the instant the stage
	// returns (runStage's deferred watchdog.Stop()+markIdle already ran by
	// the time stepDone closed), so this is a single check, not a poll loop.
	if readiness := registry.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("readiness after the stuck stage returned = %#v, want open", readiness)
	}

	// The next (fresh, non-blocking) tick must stay healthy -- the overrun
	// was a one-time episode, not a stuck flag.
	if _, stepErr := pipeline.Step(context.Background(), time.Now().UTC(), 10); stepErr != nil {
		t.Fatalf("next tick: Step() error = %v, want nil", stepErr)
	}
	if readiness := registry.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("readiness after the next healthy tick = %#v, want open", readiness)
	}
}
