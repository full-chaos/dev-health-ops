//go:build integration

package syncreconciler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// TestTerminalOutboxCloseCandidateScanStaysFastAtScale is CHAOS-6971's red-on-baseline regression
// guard: on origin/main before this fix, terminal_outbox_close's three CTE statements ordered their
// candidate scan `ORDER BY outbox.id` (a random UUID), which the 0118 migration's covering index
// (kind, status, available_at) does not provide -- so the planner fell back to a PK-order scan with an
// inline kind/status/claim filter, which must examine an UNBOUNDED prefix of the table (in random id
// order) to accumulate 100 valid candidates, or to conclusively find NONE (a kind with zero eligible
// rows is the worst case: the scan cannot stop early and must read the whole table). Prod (rev 192,
// post-6956/6937) hit exactly this: terminal_outbox_close stage_failed twice, a CTE statement costing
// 396ms/278ms against the 600ms stage budget.
//
// Executed evidence (this ticket, bigboy, one process/one container):
//   - At ~955k rows (the lead's row-count cap for one bigboy run): Step(limit=100) took 1.79-1.82s on
//     origin/main's `ORDER BY outbox.id` (reproduced twice, identical); 102.9ms after changing to
//     `ORDER BY outbox.available_at, outbox.id` (which the 0118 covering index (kind, status,
//     available_at) DOES provide -- its own docstring anticipated exactly this: "available_at trails
//     the key... in case a future per-kind ordered scan wants it"). EXPLAIN ANALYZE confirmed the
//     mechanism directly: the `outbox.id` order used `sync_dispatch_outbox_pkey` with `Rows Removed by
//     Filter: 19456`+ (an unbounded prefix scanned in random UUID order); the `available_at` order used
//     an index condition on the covering index with a small, bounded row count regardless of table size.
//   - At this test's own scale (this function, run both ways by hand before this PR): 828.6ms on
//     origin/main's `ORDER BY outbox.id` (red, over this test's 400ms budget); 84.4ms after the fix
//     (green). The budget below sits with a wide margin on both sides of these two measured numbers.
func TestTerminalOutboxCloseCandidateScanStaysFastAtScale(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, t, pool); err != nil {
		t.Fatal(err)
	}
	resetMaterializerIntegrationTables(t, ctx, pool)

	const (
		backlogSize   = 2000   // 'dispatched' finalize_sync_run rows -- far more than LIMIT 100
		noiseClosed   = 500000 // already-closed rows -- the historical backlog terminal_outbox_close has already drained; each on its OWN run id, so uq_sync_dispatch_outbox_run_kind never collides
		noisePostSync = 100000 // post_sync rows: PERMANENTLY 'dispatched' by design (excluded from this stage, see the package doc) -- an ever-present low-selectivity noise floor for THIS kind's own candidate scan, including the pathological all-empty case for dispatch_sync_run/reference_discovery's own statements
		terminalMix   = 0.97   // ~99.7% terminal per the 0118 migration's own "local readback" figure
		totalRuns     = backlogSize + noiseClosed + noisePostSync
		// Measured at this scale (executed, both directions): fixed query low tens of ms, pre-fix query
		// ~1s. 400ms sits with a wide margin below the pre-fix cost and a wide margin above the fixed
		// cost, so ordinary CI noise cannot flake this in either direction.
		budget = 400 * time.Millisecond
	)

	// One sync_runs row per outbox row across the whole id space (uq_sync_dispatch_outbox_run_kind is
	// (sync_run_id, kind): giving every noise row its OWN run id, never shared with the real backlog or
	// with another noise row, means it can never collide regardless of which kind it gets).
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status,
			total_units, completed_units, failed_units, created_at)
		SELECT
			('00000000-6971-4000-8000-' || lpad(to_hex(n), 12, '0'))::uuid,
			'org-6971-scale', '%s'::uuid, 'manual', 'incremental',
			CASE WHEN n <= %d AND random() >= %f THEN 'running'
			     ELSE (ARRAY['success','partial_failed','failed'])[1 + floor(random()*3)::int] END,
			0, 0, 0, now() - (n || ' seconds')::interval
		FROM generate_series(1, %d) AS n`,
		pgseed.DefaultSyncIntegrationID, backlogSize, terminalMix, totalRuns)); err != nil {
		t.Fatal(err)
	}
	// Noise: rows this stage has ALREADY closed (one distinct kind per row, cycling, each on its own
	// run id), and a permanently-'dispatched' post_sync set -- both with fully random (gen_random_uuid)
	// outbox ids, exactly like production.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at
		)
		SELECT
			gen_random_uuid(), 'org-6971-scale',
			('00000000-6971-4000-8000-' || lpad(to_hex(%d + n), 12, '0'))::uuid,
			(ARRAY['dispatch_sync_run','finalize_sync_run','reference_discovery'])[1 + (n %% 3)],
			'closed', now() - (n || ' seconds')::interval, 1,
			now() - (n || ' seconds')::interval, now() - (n || ' seconds')::interval
		FROM generate_series(1, %d) AS n`,
		backlogSize, noiseClosed)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		)
		SELECT
			gen_random_uuid(), 'org-6971-scale',
			('00000000-6971-4000-8000-' || lpad(to_hex(%d + n), 12, '0'))::uuid,
			'post_sync', 'dispatched',
			now() - (n || ' seconds')::interval, 1,
			now() - (n || ' seconds')::interval, 'river', 1, 'river-job-postsync-' || n,
			now() - (n || ' seconds')::interval, now() - (n || ' seconds')::interval
		FROM generate_series(1, %d) AS n`,
		backlogSize+noiseClosed, noisePostSync)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		)
		SELECT
			gen_random_uuid(), 'org-6971-scale',
			('00000000-6971-4000-8000-' || lpad(to_hex(n), 12, '0'))::uuid,
			'finalize_sync_run', 'dispatched',
			now() - (n || ' seconds')::interval, 1,
			now() - (n || ' seconds')::interval, 'river', 1, 'river-job-' || n,
			now() - (n || ' seconds')::interval, now() - (n || ' seconds')::interval
		FROM generate_series(1, %d) AS n`,
		backlogSize)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE public.sync_runs, public.sync_dispatch_outbox"); err != nil {
		t.Fatal(err)
	}

	closer, err := NewTerminalOutboxClose(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()

	stepStart := time.Now()
	result, stepErr := closer.Step(ctx, now, 100)
	elapsed := time.Since(stepStart)
	if stepErr != nil {
		t.Fatalf("Step: %v", stepErr)
	}
	if result.Finalize != 100 {
		t.Fatalf("Step closed %d finalize_sync_run row(s), want 100 (LIMIT)", result.Finalize)
	}
	t.Logf("Step(limit=100) took %s against a %d-row table (dispatch=%d finalize=%d discovery=%d)",
		elapsed, totalRuns, result.Dispatch, result.Finalize, result.Discovery)
	if elapsed > budget {
		t.Fatalf("Step took %s, over the %s regression budget: the candidate scan's cost scales with "+
			"the table's total row count again (ORDER BY outbox.id regression?), not with the rows "+
			"actually closed -- see this test's doc comment for the CHAOS-6971 mechanism", elapsed, budget)
	}
}
