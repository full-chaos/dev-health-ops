package syncreconciler

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// TestStageBudgetSpentErrorKeepsTheCauseChain: runStage's budget error is
// ErrStageBudgetSpent AND still everything its cause was, so every caller that
// branches on ErrUnavailable (or reads the materializer step) is unchanged.
func TestStageBudgetSpentErrorKeepsTheCauseChain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pipeline := newStageBehaviorPipeline(t, pipelineConfigWithBudget(StageMaterializer, 5*time.Millisecond), nil)
		err := pipeline.runStage(context.Background(), StageMaterializer, afterStageDeadline(func(ctx context.Context) error {
			return materializerUnavailable(materializerStepCommit, ctx.Err())
		}))
		if !errors.Is(err, ErrStageBudgetSpent) {
			t.Fatalf("errors.Is(err, ErrStageBudgetSpent) = false for %v", err)
		}
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("errors.Is(err, ErrUnavailable) = false for %v: callers branching on it would change", err)
		}
		if step := MaterializerStepIdentity(err); step != materializerStepCommit {
			t.Fatalf("MaterializerStepIdentity = %q, want %q", step, materializerStepCommit)
		}
		if state := MaterializerStepSQLState(err); state != stageContextDeadlineLabel {
			t.Fatalf("MaterializerStepSQLState = %q, want %q", state, stageContextDeadlineLabel)
		}
	})
}

// TestStageBudgetSpentErrorOnlyOnTheStageDeadline: a database error is not
// ErrStageBudgetSpent and comes back unchanged.
func TestStageBudgetSpentErrorOnlyOnTheStageDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pipeline := newStageBehaviorPipeline(t, DefaultMutationPipelineConfig(), nil)
		cause := materializerUnavailable(materializerStepDispatch, serverPermissionDenied)
		err := pipeline.runStage(context.Background(), StageMaterializer, func(context.Context) error { return cause })
		if errors.Is(err, ErrStageBudgetSpent) {
			t.Fatalf("a database error is ErrStageBudgetSpent: %v", err)
		}
		if err != cause {
			t.Fatalf("runStage changed a database error: got %v, want %v", err, cause)
		}
	})
}

// TestObserverBudgetSpentStillDegradesTheStep: the observer's Step error keeps
// ErrDegradedStage (Loop keeps ticking) and carries ErrStageBudgetSpent.
func TestObserverBudgetSpentStillDegradesTheStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pipeline := newStageBehaviorPipeline(t, pipelineConfigWithBudget(StageObserver, 5*time.Millisecond),
			stageBehaviors{StageObserver: afterStageDeadline(func(ctx context.Context) error { return ctx.Err() })})
		_, stepErr := pipeline.Step(context.Background(), time.Now().UTC(), 10)
		if !errors.Is(stepErr, ErrDegradedStage) || !errors.Is(stepErr, ErrStageBudgetSpent) ||
			!errors.Is(stepErr, context.DeadlineExceeded) {
			t.Fatalf("Step() error = %v, want ErrDegradedStage + ErrStageBudgetSpent + DeadlineExceeded", stepErr)
		}
	})
}
