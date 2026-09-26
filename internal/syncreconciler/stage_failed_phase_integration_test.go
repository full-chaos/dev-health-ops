//go:build integration

package syncreconciler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-6936. Production logged syncreconciler.stage_failed cut at exactly the
// stage budget with an empty step: the budget was spent, the log could not say
// on WHAT. A starved pool (work queued behind two connections) and one slow
// statement need opposite fixes. These tests spend a real budget both ways on a
// real connection pool carrying the production tracer, and require the log to
// tell them apart.

func startPhasePool(t *testing.T, maxConns int32) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	config := postgres.DefaultConfig(instance.URI)
	config.MaxConns = maxConns
	config.Tracer = postgres.NewPhaseTracer("coordinator")
	pool, err := postgres.New(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// materializerStagePipeline runs the materializer stage as fn, with a 300 ms
// budget, and returns the pipeline after one Step.
func materializerStagePipeline(t *testing.T, fn func(context.Context) error) *MutationPipeline {
	t.Helper()
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(ctx context.Context, _ time.Time, _ time.Time, _ int) (MaterializerResult, error) {
			return MaterializerResult{}, fn(ctx)
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
		noopOrphanedUnitRepair(),
		pipelineConfigWithBudget(StageMaterializer, 300*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	return pipeline
}

func stageFailedRecord(t *testing.T, pipeline *MutationPipeline) map[string]any {
	t.Helper()
	captured, restore := captureSlogRecords(t)
	defer restore()
	if _, err := pipeline.Step(context.Background(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("materializer is continue-safe, Step() error = %v", err)
	}
	record, found := findSlogRecord(*captured, "syncreconciler.stage_failed")
	if !found {
		t.Fatal("no syncreconciler.stage_failed log record")
	}
	return record
}

func TestStageFailedNamesAPoolAcquireThatSpentTheBudget(t *testing.T) {
	pool := startPhasePool(t, 1)
	held, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	pipeline := materializerStagePipeline(t, func(stageCtx context.Context) error {
		tx, err := pool.Begin(stageCtx)
		if err == nil {
			_ = tx.Rollback(context.Background())
		}
		return err
	})
	record := stageFailedRecord(t, pipeline)

	if record["phase"] != "acquire" || record["phase_name"] != "coordinator" {
		t.Fatalf("stage_failed phase = %v/%v, want acquire/coordinator; record %v", record["phase"], record["phase_name"], record)
	}
	elapsed, _ := record["phase_elapsed_ms"].(int64)
	if elapsed < 250 || elapsed > 700 {
		t.Fatalf("phase_elapsed_ms = %v, want about the 300 ms budget", record["phase_elapsed_ms"])
	}
	if summary, _ := record["phases"].(string); !strings.HasPrefix(summary, "acquire:coordinator=") || !strings.HasSuffix(summary, "!") {
		t.Fatalf("phases = %q, want one errored acquire", record["phases"])
	}

	// The wait is also a series, so the starvation is visible before a stage fails.
	var metrics strings.Builder
	if err := pipeline.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics.String(), `dev_health_reconciler_stage_acquire_wait_seconds_count{stage="materializer"} 1`) {
		t.Fatalf("no acquire-wait observation recorded for the materializer:\n%s", metrics.String())
	}
	if strings.Contains(metrics.String(), `dev_health_reconciler_stage_acquire_wait_seconds_bucket{stage="materializer",le="0.25"} 1`) {
		t.Fatal("a ~300 ms wait landed in the 250 ms bucket")
	}
}

func TestStageFailedNamesTheSlowStatementThatSpentTheBudget(t *testing.T) {
	pool := startPhasePool(t, 2)
	pipeline := materializerStagePipeline(t, func(stageCtx context.Context) error {
		_, err := pool.Exec(stageCtx, "-- reconciler slow probe\nSELECT pg_sleep(5)")
		return err
	})
	record := stageFailedRecord(t, pipeline)

	if record["phase"] != "statement" || record["phase_name"] != "reconciler slow probe" {
		t.Fatalf("stage_failed phase = %v/%v, want statement/reconciler slow probe; record %v", record["phase"], record["phase_name"], record)
	}
	elapsed, _ := record["phase_elapsed_ms"].(int64)
	if elapsed < 250 || elapsed > 700 {
		t.Fatalf("phase_elapsed_ms = %v, want about the 300 ms budget", record["phase_elapsed_ms"])
	}
	// The acquire that came first was quick and is listed, not blamed.
	summary, _ := record["phases"].(string)
	if !strings.HasPrefix(summary, "acquire:coordinator=") || !strings.Contains(summary, ";statement:reconciler slow probe=") {
		t.Fatalf("phases = %q, want a quick acquire then the slow statement", summary)
	}
	if strings.Contains(strings.SplitN(summary, ";", 2)[0], "!") {
		t.Fatalf("the acquire succeeded, but phases marks it errored: %q", summary)
	}
}

func TestStageFailedNamesTheTransactionPhasesAStatementRanIn(t *testing.T) {
	pool := startPhasePool(t, 2)
	pipeline := materializerStagePipeline(t, func(stageCtx context.Context) error {
		tx, err := pool.Begin(stageCtx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		_, err = tx.Exec(stageCtx, "SELECT pg_sleep(5)")
		return err
	})
	record := stageFailedRecord(t, pipeline)
	summary, _ := record["phases"].(string)
	if record["phase"] != "statement" || !strings.HasPrefix(summary, "acquire:coordinator=") ||
		!strings.Contains(summary, ";statement:begin=") {
		t.Fatalf("stage_failed = %v, want the acquire and BEGIN listed before the slow statement", record)
	}
	if name, _ := record["phase_name"].(string); !strings.HasPrefix(name, "SELECT pg_sleep") {
		t.Fatalf("phase_name = %q, want the slow statement, not BEGIN", name)
	}
}
