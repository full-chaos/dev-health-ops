package syncreconciler

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// CHAOS-13743: a stage that ends on its OWN budget deadline logs
// reason="stage budget spent"; every other stage_failed line is unchanged.

type pipelineSweepFunc func(context.Context, time.Time, int) (UnreclaimableSweepResult, error)

func (function pipelineSweepFunc) Step(ctx context.Context, now time.Time, limit int) (UnreclaimableSweepResult, error) {
	return function(ctx, now, limit)
}

// stageBehaviors maps a stage to what its fake does; a stage not in the map
// succeeds at once.
type stageBehaviors map[StageName]func(context.Context) error

func newStageBehaviorPipeline(t *testing.T, config MutationPipelineConfig, behaviors stageBehaviors) *MutationPipeline {
	t.Helper()
	run := func(stage StageName, ctx context.Context) error {
		if behavior, ok := behaviors[stage]; ok {
			return behavior(ctx)
		}
		return nil
	}
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(ctx context.Context, _ time.Time, _ int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, run(StageLeaseRepair, ctx)
		}),
		pipelineTerminalDeliveryRepairFunc(func(ctx context.Context, _ time.Time, _ int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, run(StageTerminalDeliveryRepair, ctx)
		}),
		pipelineMaterializerFunc(func(ctx context.Context, _ time.Time, _ time.Time, _ int) (MaterializerResult, error) {
			return MaterializerResult{}, run(StageMaterializer, ctx)
		}),
		pipelineKernelFunc(func(ctx context.Context, _ time.Time, _ int, _ time.Duration, _ AtLeastOncePublisher, _ PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, run(StageKernel, ctx)
		}),
		pipelineObserverFunc(func(ctx context.Context, _ time.Time, _ int) (Observation, error) {
			return Observation{}, run(StageObserver, ctx)
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		pipelineSweepFunc(func(ctx context.Context, _ time.Time, _ int) (UnreclaimableSweepResult, error) {
			return UnreclaimableSweepResult{}, run(StageUnreclaimableSweep, ctx)
		}),
		pipelineTerminalOutboxCloseFunc(func(ctx context.Context, _ time.Time, _ int) (TerminalOutboxCloseResult, error) {
			return TerminalOutboxCloseResult{}, run(StageTerminalOutboxClose, ctx)
		}),
		pipelineOrphanedUnitRepairFunc(func(ctx context.Context, _ time.Time, _ int) (OrphanedUnitRepairResult, error) {
			return OrphanedUnitRepairResult{}, run(StageOrphanedUnitRepair, ctx)
		}),
		config,
	)
	if err != nil {
		t.Fatalf("construct pipeline: %v", err)
	}
	return pipeline
}

// afterStageDeadline waits for the stage context to end, then returns the
// error the stage builds from it.
func afterStageDeadline(build func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		<-ctx.Done()
		return build(ctx)
	}
}

func stageFailedRecordFor(t *testing.T, records []map[string]any, stage StageName) map[string]any {
	t.Helper()
	for _, record := range records {
		if record["msg"] == "syncreconciler.stage_failed" && record["stage"] == string(stage) {
			return record
		}
	}
	t.Fatalf("no syncreconciler.stage_failed record for stage %q in %v", stage, records)
	return nil
}

var serverPermissionDenied = &pgconn.PgError{Code: "42501", Message: "permission denied for table sync_dispatch_outbox"}

// jsonStageFailedLines runs one Step with a JSON handler (time dropped) and
// returns the stage_failed lines exactly as they are written.
func jsonStageFailedLines(t *testing.T, pipeline *MutationPipeline) []string {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buffer, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 && attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	})))
	defer slog.SetDefault(previous)
	_, _ = pipeline.Step(context.Background(), time.Now().UTC(), 10)
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(buffer.String()), "\n") {
		if strings.Contains(line, `"msg":"syncreconciler.stage_failed"`) {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestStageFailedLinesStayByteEqualWhenNotBudget is the golden over all eight
// stages: a failure that is not the stage's own budget deadline writes the
// same line as before CHAOS-13743, byte for byte.
func TestStageFailedLinesStayByteEqualWhenNotBudget(t *testing.T) {
	cases := []struct {
		stage StageName
		err   error
		want  string
	}{
		{StageLeaseRepair, ErrUnavailable,
			`{"level":"ERROR","msg":"syncreconciler.stage_failed","stage":"lease_repair","budget_ms":400,"elapsed_ms":0,"sqlstate":"","error":"sync dispatch observer database unavailable","phase":"none","phases":""}`},
		{StageUnreclaimableSweep, sweepUnavailable(sweepStepBegin, serverPermissionDenied),
			`{"level":"ERROR","msg":"syncreconciler.stage_failed","stage":"unreclaimable_sweep","budget_ms":600,"elapsed_ms":0,"sqlstate":"42501","error":"unreclaimable sweep begin domain transaction failed (sqlstate 42501): sync dispatch observer database unavailable","phase":"none","phases":""}`},
		{StageTerminalDeliveryRepair, ErrUnavailable,
			`{"level":"ERROR","msg":"syncreconciler.stage_failed","stage":"terminal_delivery_repair","budget_ms":750,"elapsed_ms":0,"sqlstate":"","error":"sync dispatch observer database unavailable","phase":"none","phases":""}`},
		{StageTerminalOutboxClose, terminalOutboxCloseUnavailable(terminalOutboxCloseStepBegin, serverPermissionDenied),
			`{"level":"ERROR","msg":"syncreconciler.stage_failed","stage":"terminal_outbox_close","budget_ms":600,"elapsed_ms":0,"sqlstate":"42501","error":"terminal outbox close begin terminal-outbox-close transaction failed (sqlstate 42501): sync dispatch observer database unavailable","phase":"none","phases":""}`},
		{StageOrphanedUnitRepair, ErrUnavailable,
			`{"level":"ERROR","msg":"syncreconciler.stage_failed","stage":"orphaned_unit_repair","budget_ms":600,"elapsed_ms":0,"sqlstate":"","error":"sync dispatch observer database unavailable","phase":"none","phases":""}`},
		{StageMaterializer, materializerUnavailable(materializerStepDispatch, serverPermissionDenied),
			`{"level":"ERROR","msg":"syncreconciler.stage_failed","stage":"materializer","budget_ms":600,"elapsed_ms":0,"sqlstate":"42501","error":"materializer dispatch materialization upsert of public.sync_dispatch_outbox failed (sqlstate 42501): sync dispatch observer database unavailable","phase":"none","phases":""}`},
		{StageKernel, ErrUnavailable,
			`{"level":"ERROR","msg":"syncreconciler.stage_failed","stage":"kernel","budget_ms":1000,"elapsed_ms":0,"sqlstate":"","error":"sync dispatch observer database unavailable","phase":"none","phases":""}`},
		{StageObserver, ErrUnavailable,
			`{"level":"ERROR","msg":"syncreconciler.stage_failed","stage":"observer","budget_ms":400,"elapsed_ms":0,"sqlstate":"","error":"sync dispatch observer database unavailable","phase":"none","phases":""}`},
	}
	if len(cases) != len(orderedStages) {
		t.Fatalf("golden covers %d stages, the pipeline runs %d", len(cases), len(orderedStages))
	}
	for _, tc := range cases {
		t.Run(string(tc.stage), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				err := tc.err
				pipeline := newStageBehaviorPipeline(t, DefaultMutationPipelineConfig(), stageBehaviors{
					tc.stage: func(context.Context) error { return err },
				})
				lines := jsonStageFailedLines(t, pipeline)
				if len(lines) != 1 || lines[0] != tc.want {
					t.Fatalf("stage_failed lines = %q\nwant exactly [%s]", lines, tc.want)
				}
			})
		})
	}
}

// TestStageBudgetSpentNamesTheBudgetForEveryStage: each stage, ended by its own
// 5 ms budget with the error it really builds from the expired context, logs
// the budget reason and never "database unavailable".
func TestStageBudgetSpentNamesTheBudgetForEveryStage(t *testing.T) {
	cases := []struct {
		stage        StageName
		build        func(context.Context) error
		wantSQLState string
	}{
		{StageLeaseRepair, func(context.Context) error { return ErrUnavailable }, ""},
		{StageUnreclaimableSweep, func(ctx context.Context) error { return sweepUnavailable(sweepStepBegin, ctx.Err()) }, ""},
		{StageTerminalDeliveryRepair, func(context.Context) error { return ErrUnavailable }, ""},
		{StageTerminalOutboxClose, func(ctx context.Context) error {
			return terminalOutboxCloseUnavailable(terminalOutboxCloseStepBegin, ctx.Err())
		}, stageContextDeadlineLabel},
		{StageOrphanedUnitRepair, func(context.Context) error { return ErrUnavailable }, ""},
		{StageMaterializer, func(ctx context.Context) error {
			return materializerUnavailable(materializerStepCommit, ctx.Err())
		}, stageContextDeadlineLabel},
		{StageKernel, func(context.Context) error { return ErrUnavailable }, ""},
		{StageObserver, func(ctx context.Context) error { return ctx.Err() }, ""},
	}
	if len(cases) != len(orderedStages) {
		t.Fatalf("cases cover %d stages, the pipeline runs %d", len(cases), len(orderedStages))
	}
	for _, tc := range cases {
		t.Run(string(tc.stage), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				captured, restore := captureSlogRecords(t)
				defer restore()
				pipeline := newStageBehaviorPipeline(t, pipelineConfigWithBudget(tc.stage, 5*time.Millisecond),
					stageBehaviors{tc.stage: afterStageDeadline(tc.build)})
				_, _ = pipeline.Step(context.Background(), time.Now().UTC(), 10)

				record := stageFailedRecordFor(t, *captured, tc.stage)
				if record["reason"] != "stage budget spent" {
					t.Fatalf("reason = %v, want \"stage budget spent\" (record %v)", record["reason"], record)
				}
				wantError := "syncreconciler: stage " + string(tc.stage) + " budget spent (5ms)"
				if record["error"] != wantError {
					t.Fatalf("error = %v, want %q", record["error"], wantError)
				}
				if record["sqlstate"] != tc.wantSQLState || record["budget_ms"] != int64(5) || record["elapsed_ms"] != int64(5) {
					t.Fatalf("sqlstate/budget_ms/elapsed_ms = %v/%v/%v, want %q/5/5", record["sqlstate"], record["budget_ms"], record["elapsed_ms"], tc.wantSQLState)
				}
				if _, ok := record["phases"]; !ok {
					t.Fatalf("record has no phases attribute: %v", record)
				}
			})
		})
	}
}

func materializerFailureRecord(t *testing.T, budget time.Duration, behavior func(context.Context) error) map[string]any {
	t.Helper()
	var record map[string]any
	synctest.Test(t, func(t *testing.T) {
		captured, restore := captureSlogRecords(t)
		defer restore()
		pipeline := newStageBehaviorPipeline(t, pipelineConfigWithBudget(StageMaterializer, budget),
			stageBehaviors{StageMaterializer: behavior})
		_, _ = pipeline.Step(context.Background(), time.Now().UTC(), 10)
		record = stageFailedRecordFor(t, *captured, StageMaterializer)
	})
	return record
}

// TestStageBudgetSpentRealServerErrorAfterDeadlineKeepsOldLine: a real server
// SQLSTATE wins even when it lands after the stage deadline.
func TestStageBudgetSpentRealServerErrorAfterDeadlineKeepsOldLine(t *testing.T) {
	record := materializerFailureRecord(t, 5*time.Millisecond, afterStageDeadline(func(context.Context) error {
		return materializerUnavailable(materializerStepDispatch, serverPermissionDenied)
	}))
	if _, ok := record["reason"]; ok {
		t.Fatalf("a real server error was labeled a spent budget: %v", record)
	}
	want := "materializer dispatch materialization upsert of public.sync_dispatch_outbox failed (sqlstate 42501): sync dispatch observer database unavailable"
	if record["error"] != want {
		t.Fatalf("error = %v, want %q", record["error"], want)
	}
}

// TestStageBudgetSpentCountsQueryCanceledOnlyAtTheStageDeadline: 57014 at the
// stage's own deadline is the budget; 57014 while the stage context is alive
// (a server statement_timeout) keeps the old line.
func TestStageBudgetSpentCountsQueryCanceledOnlyAtTheStageDeadline(t *testing.T) {
	canceled := &pgconn.PgError{Code: "57014", Message: "canceling statement due to user request"}
	atDeadline := materializerFailureRecord(t, 5*time.Millisecond, afterStageDeadline(func(context.Context) error {
		return materializerUnavailable(materializerStepFinalize, canceled)
	}))
	if atDeadline["reason"] != "stage budget spent" || atDeadline["sqlstate"] != "57014" {
		t.Fatalf("57014 at the stage deadline: reason/sqlstate = %v/%v, want stage budget spent/57014", atDeadline["reason"], atDeadline["sqlstate"])
	}
	alive := materializerFailureRecord(t, 600*time.Millisecond, func(context.Context) error {
		return materializerUnavailable(materializerStepFinalize, canceled)
	})
	if _, ok := alive["reason"]; ok {
		t.Fatalf("57014 with the stage context alive was labeled a spent budget: %v", alive)
	}
}

// TestStageBudgetSpentNeedsTheStageDeadline: a deadline label raised while the
// stage context is still alive (a shorter context inside the stage) is not the
// stage's budget.
func TestStageBudgetSpentNeedsTheStageDeadline(t *testing.T) {
	record := materializerFailureRecord(t, 600*time.Millisecond, func(context.Context) error {
		return materializerUnavailable(materializerStepDispatch, context.DeadlineExceeded)
	})
	if _, ok := record["reason"]; ok {
		t.Fatalf("an inner deadline was labeled the stage budget: %v", record)
	}
	want := "materializer dispatch materialization upsert of public.sync_dispatch_outbox failed (sqlstate ctx_deadline_exceeded): sync dispatch observer database unavailable"
	if record["error"] != want {
		t.Fatalf("error = %v, want %q", record["error"], want)
	}
}

// TestStageBudgetSpentCommitWithoutSQLState is the prod shape of 2026-11-14
// (3 of 6 lines): the report statement was cancelled, then commit failed with
// no SQLSTATE and no deadline in its error.
func TestStageBudgetSpentCommitWithoutSQLState(t *testing.T) {
	record := materializerFailureRecord(t, 5*time.Millisecond, afterStageDeadline(func(context.Context) error {
		return materializerUnavailable(materializerStepCommit, pgx.ErrTxCommitRollback)
	}))
	if record["reason"] != "stage budget spent" || record["sqlstate"] != "" {
		t.Fatalf("reason/sqlstate = %v/%v, want stage budget spent/\"\"", record["reason"], record["sqlstate"])
	}
	if record["error"] != "syncreconciler: stage materializer budget spent (5ms)" {
		t.Fatalf("error = %v", record["error"])
	}
}

// TestStageBudgetSpentKeepsParentDeadlineUntelemetered: the parent (loop
// envelope or shutdown) ending first is not a stage failure; no line at all.
func TestStageBudgetSpentKeepsParentDeadlineUntelemetered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		captured, restore := captureSlogRecords(t)
		defer restore()
		pipeline := newStageBehaviorPipeline(t, DefaultMutationPipelineConfig(), stageBehaviors{
			StageMaterializer: afterStageDeadline(func(ctx context.Context) error {
				return materializerUnavailable(materializerStepCommit, ctx.Err())
			}),
		})
		parent, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
		defer cancel()
		_, stepErr := pipeline.Step(parent, time.Now().UTC(), 10)
		if stepErr == nil {
			t.Fatal("Step() = nil, want the parent deadline propagated")
		}
		for _, record := range *captured {
			if record["msg"] == "syncreconciler.stage_failed" {
				t.Fatalf("parent deadline was telemetered as a stage failure: %v", record)
			}
		}
	})
}

// TestStageBudgetSpentPairsTheWakeupReportLine: the report line stays (the
// report was not delivered) and says the budget was the cause; a database
// error keeps the line without a cause.
func TestStageBudgetSpentPairsTheWakeupReportLine(t *testing.T) {
	run := func(t *testing.T, budget time.Duration, behavior func(context.Context) error) (map[string]any, Observation) {
		var record map[string]any
		var observation Observation
		synctest.Test(t, func(t *testing.T) {
			captured, restore := captureSlogRecords(t)
			defer restore()
			pipeline := newStageBehaviorPipeline(t, pipelineConfigWithBudget(StageMaterializer, budget),
				stageBehaviors{StageMaterializer: behavior})
			observation, _ = pipeline.Step(context.Background(), time.Now().UTC(), 10)
			var found bool
			record, found = findSlogRecord(*captured, "syncreconciler.dispatch_wakeup_report_unavailable")
			if !found {
				t.Fatal("no dispatch_wakeup_report_unavailable record")
			}
		})
		return record, observation
	}
	spent, observation := run(t, 5*time.Millisecond, afterStageDeadline(func(ctx context.Context) error {
		return materializerUnavailable(materializerStepCommit, ctx.Err())
	}))
	if spent["cause"] != "stage budget spent" || spent["step"] != runawayReportStepUpstream {
		t.Fatalf("budget pass: cause/step = %v/%v, want stage budget spent/%q", spent["cause"], spent["step"], runawayReportStepUpstream)
	}
	if observation.WakeupReportFailures != 1 {
		t.Fatalf("WakeupReportFailures = %d, want 1", observation.WakeupReportFailures)
	}
	database, _ := run(t, 600*time.Millisecond, func(context.Context) error {
		return materializerUnavailable(materializerStepDispatch, serverPermissionDenied)
	})
	if _, ok := database["cause"]; ok {
		t.Fatalf("a database failure carries a budget cause: %v", database)
	}
}
