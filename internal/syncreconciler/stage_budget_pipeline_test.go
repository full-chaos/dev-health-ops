package syncreconciler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
