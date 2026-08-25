//go:build integration

package syncreconciler

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-4262: at realistic scale, materializeFinalizeSQL's planner cost
// estimate for a candidate set that is ALWAYS EMPTY in steady state crosses
// jit_above_cost (Postgres's default 100000), so the statement compiles a
// JIT plan for ~2ms of real work. Reproduced empirically (2026-08-25, raw
// psql against this exact schema/population shape): planner cost 1,714,780
// without an indexed access path for sync_runs's `status NOT IN (...)`
// predicate, JIT firing for ~134ms. See migration 0111's docstring for the
// index half of the fix; this file proves the jit=off half that
// materializer.go's Step now bakes into its own transaction, using the
// WORST case (no index at all) as the red baseline -- the most convincing
// proof that the mitigation does not depend on the migration also landing.

// jitCostSchedulesOccurrences seeds a scheduled_sync_occurrences population
// large enough, with none of it matching any seeded sync_run, that the
// planner's correlated-subquery estimate for `triggered_by = 'schedule'`
// rows dominates the cost the way it does in production (see the migration
// 0111 docstring for the mechanism).
const jitCostScheduledOccurrences = 20000

// jitCostSyncRuns matches the ORDER OF MAGNITUDE CHAOS-4262 measured in
// production ("4,536 rows removed by filter, 0 rows out").
const jitCostSyncRuns = 4536

func seedJITCostPopulation(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.scheduled_sync_occurrences (occurrence_id, sync_run_id, job_run_id, reconcile_status)
		SELECT 'jit-cost-occ-' || g, NULL, NULL, 'completed'
		FROM generate_series(1, $1) AS g`, jitCostScheduledOccurrences); err != nil {
		t.Fatalf("seed scheduled_sync_occurrences: %v", err)
	}
	// Every seeded sync_run is ALREADY TERMINAL -- the steady-state shape
	// CHAOS-4262 measured, where the candidate CTE matches zero rows no
	// matter how many rows sync_runs has accumulated. A THIRD are
	// schedule-triggered so the correlated EXISTS subplan against
	// scheduled_sync_occurrences is actually exercised by the planner's cost
	// model, matching a real cron-driven provider mix.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_runs (id, org_id, triggered_by, status, created_at)
		SELECT gen_random_uuid(), 'org-' || (g % 50),
			CASE WHEN g % 3 = 0 THEN 'schedule' ELSE 'manual' END,
			(ARRAY['success','success','success','success','success','success','success','partial_failed','failed'])[1 + (g % 9)],
			now() - (g || ' minutes')::interval
		FROM generate_series(1, $1) AS g`, jitCostSyncRuns); err != nil {
		t.Fatalf("seed sync_runs: %v", err)
	}
	for _, table := range []string{
		"sync_runs", "scheduled_sync_occurrences", "sync_run_units",
		"sync_run_reference_discoveries", "sync_dispatch_outbox",
	} {
		if _, err := pool.Exec(ctx, "ANALYZE public."+table); err != nil {
			t.Fatalf("analyze %s: %v", table, err)
		}
	}
}

// TestMaterializeFinalizeSQLPlannerCostCrossesJITThresholdWithoutAnIndex is
// the RED half, made deterministic rather than a wall-clock race: it proves
// the ROOT CAUSE claim directly from the planner's own estimate --
// EXPLAIN (no ANALYZE, so nothing executes and nothing is timed) reports a
// total cost over jit_above_cost's default of 100000 for the raw statement
// CHAOS-4262 named, at the realistic population above, with no index on
// sync_runs's `status NOT IN (...)` predicate (migration 0111 adds one; this
// test's fixture deliberately does not, to isolate the estimate itself from
// the mitigation). Whether an actual JIT compile takes long enough to miss a
// given stage budget depends on host speed and plan complexity -- the cost
// crossing the threshold at all does not, which is what actually decides
// whether Postgres invokes JIT for this statement.
func TestMaterializeFinalizeSQLPlannerCostCrossesJITThresholdWithoutAnIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	seedJITCostPopulation(t, ctx, pool)

	var jitAboveCostText string
	if err := pool.QueryRow(ctx, "SHOW jit_above_cost").Scan(&jitAboveCostText); err != nil {
		t.Fatal(err)
	}
	jitAboveCost, err := strconv.Atoi(jitAboveCostText)
	if err != nil {
		t.Fatalf("parse jit_above_cost %q: %v", jitAboveCostText, err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now := time.Now().UTC()
	var explainJSON []byte
	explainSQL := "EXPLAIN (FORMAT JSON) " + materializeFinalizeSQL
	if err := tx.QueryRow(ctx, explainSQL, now, maximumStepLimit).Scan(&explainJSON); err != nil {
		t.Fatal(err)
	}
	var plans []struct {
		Plan struct {
			TotalCost float64 `json:"Total Cost"`
		} `json:"Plan"`
	}
	if err := json.Unmarshal(explainJSON, &plans); err != nil {
		t.Fatalf("parse EXPLAIN output: %v\n%s", err, explainJSON)
	}
	if len(plans) != 1 {
		t.Fatalf("EXPLAIN returned %d plans, want 1:\n%s", len(plans), explainJSON)
	}
	totalCost := plans[0].Plan.TotalCost
	if totalCost <= float64(jitAboveCost) {
		t.Fatalf("planner total cost = %.0f, want it OVER jit_above_cost (%d) -- "+
			"the population fixture no longer reproduces CHAOS-4262's cost blowup and needs revisiting, not this assertion relaxed",
			totalCost, jitAboveCost)
	}
}

// TestMaterializeFinalizeSQLCancellationSurfacesSQLState57014 proves the
// driver-classification half against a REAL Postgres cancellation rather
// than the unit-level fake: the exact statement CHAOS-4262 named, at the
// same cost-inflated population as above (so a real JIT compile is actually
// in flight, not an artificially tiny timeout racing bare execution), is
// canceled by a statement_timeout well under its own observed compile time
// (empirically ~130-150ms locally on 2026-08-25) but with ample margin
// either way, and pgconn.PgError.Code must be exactly 57014 -- the code
// materializerStepError/stageSQLState are built to recover.
func TestMaterializeFinalizeSQLCancellationSurfacesSQLState57014(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	seedJITCostPopulation(t, ctx, pool)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '25ms'"); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	_, execErr := tx.Exec(ctx, materializeFinalizeSQL, now, maximumStepLimit)
	if execErr == nil {
		t.Fatal("materializeFinalizeSQL completed under a 25ms statement_timeout with JIT left on and no index -- " +
			"the population fixture no longer reproduces CHAOS-4262's cost blowup and needs revisiting, not this assertion relaxed")
	}
	var pgErr *pgconn.PgError
	if !errors.As(execErr, &pgErr) {
		t.Fatalf("execErr = %v, want a *pgconn.PgError so its SQLSTATE can be recovered", execErr)
	}
	if pgErr.Code != "57014" {
		t.Fatalf("sqlstate = %q, want 57014 (query_canceled)", pgErr.Code)
	}
}

// TestMaterializerStepCompletesUnderRealisticStageBudgetWithJITOnAtServerLevel
// is the GREEN half: the exact shipped Materializer.Step, unmodified, called
// through its real public API against the same realistic population and the
// same 600ms budget the reconciler pipeline actually uses -- with the
// server's own jit=on default untouched, proving Step's own SET LOCAL
// jit=off is what keeps it under budget, not a container-level setting this
// test happened to control.
func TestMaterializerStepCompletesUnderRealisticStageBudgetWithJITOnAtServerLevel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	seedJITCostPopulation(t, ctx, pool)

	var jitSetting string
	if err := pool.QueryRow(ctx, "SHOW jit").Scan(&jitSetting); err != nil {
		t.Fatal(err)
	}
	if jitSetting != "on" {
		t.Fatalf("server jit = %q, want on -- this test's whole point is proving Step is safe WITH jit enabled", jitSetting)
	}

	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}

	const materializerStageBudget = 600 * time.Millisecond
	stepCtx, stepCancel := context.WithTimeout(ctx, materializerStageBudget)
	defer stepCancel()

	now := time.Now().UTC()
	start := time.Now()
	_, stepErr := materializer.Step(stepCtx, now, now.Add(-time.Hour), maximumStepLimit)
	elapsed := time.Since(start)

	if stepErr != nil {
		t.Fatalf("Step() error = %v (after %s); the JIT-disable statement should keep this well under the %s stage budget",
			stepErr, elapsed, materializerStageBudget)
	}
	if elapsed >= materializerStageBudget {
		t.Fatalf("Step() took %s, want comfortably under the %s stage budget it will actually run under in production",
			elapsed, materializerStageBudget)
	}
}

// TestMaterializerStepContextDeadlineClassifiesAsStageContextDeadline is the
// codex adversarial-review follow-up: the earlier tests in this file force a
// SERVER-side statement_timeout to produce sqlstate 57014, but that is not
// how production actually cancels a stage -- runStage bounds every stage
// with a plain context.WithTimeout (internal/syncreconciler/pipeline.go),
// and pgx v5's context watcher answers a canceled context by tearing the
// connection down CLIENT-side and returning a wrapped
// context.DeadlineExceeded, never a *pgconn.PgError (confirmed empirically
// against a real server, 2026-08-25: the same cancellation that makes
// Postgres log "canceling statement due to user request", sqlstate 57014,
// server-side, reaches the Go caller as a bare context.DeadlineExceeded).
//
// This forces that EXACT client-side path for real -- a row lock held by a
// second session on the finalize statement's own ON CONFLICT target, so the
// materializer's Exec genuinely blocks on the wire and its stage context
// genuinely expires mid-flight -- rather than a pg_sleep or a server-side
// timeout, neither of which exercises the code path this test pins.
func TestMaterializerStepContextDeadlineClassifiesAsStageContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}

	const runID = "00000000-0000-4000-8000-000000004901"
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_runs (id, org_id, status, created_at)
		VALUES ($1, 'org-materializer', 'running', $2)`, runID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'finalize_sync_run',
			'pending', $2, 0, $2, $2
		)`, runID, now); err != nil {
		t.Fatal(err)
	}

	// A second, independent session holds a row lock on the exact outbox row
	// the finalize statement's ON CONFLICT (sync_run_id, kind) DO UPDATE will
	// target -- a real, on-the-wire block, not a synthetic delay.
	blocker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Release()
	blockerTx, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blockerTx.Exec(ctx, `
		SELECT 1 FROM public.sync_dispatch_outbox
		WHERE sync_run_id = $1 AND kind = 'finalize_sync_run' FOR UPDATE`, runID); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blockerTx.Rollback(ctx) }()

	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}

	stepCtx, stepCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer stepCancel()
	_, stepErr := materializer.Step(stepCtx, now, now.Add(-time.Hour), maximumStepLimit)
	if stepErr == nil {
		t.Fatal("Step() completed despite a row lock held for the whole stage budget -- the lock did not actually block the finalize statement")
	}
	if !errors.Is(stepErr, ErrUnavailable) {
		t.Fatalf("Step() error = %v, want it to satisfy errors.Is(err, ErrUnavailable)", stepErr)
	}
	if got := MaterializerStepSQLState(stepErr); got != stageContextDeadlineLabel {
		t.Fatalf("MaterializerStepSQLState(err) = %q, want %q (this is the CHAOS-4262 review finding: "+
			"a real stage-context cancellation must classify distinctly, not silently as \"\")", got, stageContextDeadlineLabel)
	}

	// Release the lock, then prove the pool is still healthy -- a
	// context-canceled Exec must not poison the connection for the next tick.
	if err := blockerTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	recoverCtx, recoverCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recoverCancel()
	if _, err := materializer.Step(recoverCtx, now, now.Add(-time.Hour), maximumStepLimit); err != nil {
		t.Fatalf("Step() after lock release = %v, want the pool to have recovered", err)
	}
}
