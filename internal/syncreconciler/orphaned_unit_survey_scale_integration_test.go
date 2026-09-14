//go:build integration

package syncreconciler

import (
	"context"
	"testing"
	"time"
)

// seedDecoyOutboxHistory writes a large, entirely non-matching population of
// terminal worker_job_outbox rows spanning every OTHER checked-in job kind,
// all old, none of them sync.provider_unit. This is what a long-lived
// deployment's outbox accumulates: nothing schedules a worker_job_terminal
// retention occurrence today, so the table only grows.
//
// It also lays down the production indexes this survey and its driving table
// depend on (0046_add_worker_job_outbox.py,
// 0062_add_sync_provider_canary_quiescence_index.py). createOrphanFixture's
// fixture is otherwise index-free, and without these this test would measure
// an even more pessimistic plan than production's, not a matching one.
func seedDecoyOutboxHistory(t *testing.T, ctx context.Context, h *orphanHarness, now time.Time, n int) {
	t.Helper()
	for _, statement := range []string{
		`CREATE INDEX IF NOT EXISTS ix_worker_job_outbox_terminal
			ON public.worker_job_outbox (status, delivered_at, updated_at)
			WHERE status IN ('delivered', 'dead')`,
		`CREATE INDEX IF NOT EXISTS ix_worker_job_outbox_due
			ON public.worker_job_outbox (status, next_attempt_at, scheduled_at, created_at)
			WHERE status IN ('pending', 'claimed')`,
		`CREATE INDEX IF NOT EXISTS ix_sync_run_units_canary_quiescence
			ON public.sync_run_units (status, provider, dataset_key)
			WHERE status IN ('dispatching', 'running')`,
	} {
		if _, err := h.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	if _, err := h.admin.Exec(ctx, `
		INSERT INTO public.worker_job_outbox
			(id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority,
			 max_attempts, scheduled_at, status, attempt_count, next_attempt_at, river_job_id,
			 delivered_at, created_at, updated_at)
		SELECT
			gen_random_uuid(),
			'decoy:' || generation::text,
			(ARRAY['metrics.daily_finalize', 'workgraph.build',
				'operational.webhook_delivery', 'report.execute_scheduled'])[1 + (generation % 4)],
			1,
			'{"decoy":true}'::json,
			'sha256:' || lpad(to_hex(generation), 64, '0'),
			'default',
			2,
			5,
			delivered_at,
			'delivered',
			1,
			delivered_at,
			200000000 + generation,
			delivered_at,
			delivered_at,
			delivered_at
		FROM (
			SELECT generation, $1::timestamptz - (generation || ' seconds')::interval AS delivered_at
			FROM generate_series(1, $2) AS generation
		) AS rows`, now, n); err != nil {
		t.Fatalf("seed decoy outbox history: %v", err)
	}
	if _, err := h.admin.Exec(ctx, `ANALYZE public.worker_job_outbox, public.sync_run_units`); err != nil {
		t.Fatalf("analyze: %v", err)
	}
}

// decoyOutboxRows is sized to be unambiguous, not merely sufficient: large
// enough that an outbox-driven scan ordered oldest-delivered-first, with no
// lower bound and near-zero selectivity, could never be mistaken for cheap by
// accident. The fixed, unit-driven survey's cost does not depend on this
// number at all -- it is bounded by how many units are CURRENTLY
// 'dispatching', which this fixture holds at one -- so the specific size is
// not load-bearing for the fix; it exists to make the shape unmistakable to a
// reader re-running this test after reverting the query.
const decoyOutboxRows = 300_000

// TestOrphanedUnitSurveyStaysInsideItsStageBudgetAtScale reproduces the miss
// class: a worker_job_outbox with a large accumulated history reaching the
// survey under the exact budget StageOrphanedUnitRepair runs it under in
// production (DefaultStageBudgets), and proves the pass both stays inside
// that budget and still finds and re-arms the one real strand.
func TestOrphanedUnitSurveyStaysInsideItsStageBudgetAtScale(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startOrphanHarness(t, ctx)
	now := time.Date(2026, time.September, 9, 3, 0, 0, 0, time.UTC)
	h.seed(t, ctx, now, liveShapeDefault(now))
	seedDecoyOutboxHistory(t, ctx, h, now, decoyOutboxRows)

	budget := DefaultStageBudgets()[StageOrphanedUnitRepair]
	budgetCtx, budgetCancel := context.WithTimeout(ctx, budget)
	defer budgetCancel()

	started := time.Now()
	result, err := h.repair.Step(budgetCtx, now, 10)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("Step missed its %s stage budget (elapsed %s) against a %d-row decoy "+
			"history: %v", budget, elapsed, decoyOutboxRows, err)
	}
	if result.Found != 1 || result.ReArmed != 1 {
		t.Fatalf("found=%d rearmed=%d, want 1/1 -- the real strand must still be found "+
			"underneath a delivered-row population this large (%+v)", result.Found, result.ReArmed, result)
	}
	t.Logf("survey completed in %s (budget %s) against %d decoy rows plus the live shape",
		elapsed, budget, decoyOutboxRows)
}
