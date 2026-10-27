//go:build integration

package syncreconciler

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Population shaped like a large GitHub backfill beside a long history.
const (
	backfillTerminalRuns      = 300000
	backfillActiveRuns        = 30000
	backfillBigRuns           = 40
	backfillPlannedUnitsPerBg = 5000
	backfillDueDiscoveries    = 30000
	backfillTerminalDiscovery = 300000
	backfillLedgerRows        = 300000

	backfillStatementCeiling = 100 * time.Millisecond
	backfillStepCeiling      = 300 * time.Millisecond
)

func seedBackfillBudgetPopulation(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		fmt.Sprintf(`INSERT INTO public.sync_runs (id, org_id, integration_id, mode, total_units, completed_units, failed_units, triggered_by, status, created_at)
			SELECT md5('t' || g)::uuid, 'org-materializer', '%[1]s'::uuid, 'incremental', 0, 0, 0, 'manual', 'success', now() - (g || ' minutes')::interval
			FROM generate_series(1, %[2]d) AS g`, pgseed.DefaultSyncIntegrationID, backfillTerminalRuns),
		fmt.Sprintf(`INSERT INTO public.sync_runs (id, org_id, integration_id, mode, total_units, completed_units, failed_units, triggered_by, status, created_at)
			SELECT md5('a' || g)::uuid, 'org-materializer', '%[1]s'::uuid, 'backfill', 0, 0, 0, 'manual', 'running', now() - (g || ' seconds')::interval
			FROM generate_series(1, %[2]d) AS g`, pgseed.DefaultSyncIntegrationID, backfillActiveRuns),
		// big backfill runs: many planned units each
		fmt.Sprintf(`INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, attempts, created_at, status, available_at, updated_at)
			SELECT md5('bu' || g || '-' || u)::uuid, 'org-materializer', md5('a' || g)::uuid, '%[1]s'::uuid, '%[2]s'::uuid, 'github', 'commits', 'rest_core', 'backfill', 0, now(), 'planned', NULL, now()
			FROM generate_series(1, %[3]d) AS g, generate_series(1, %[4]d) AS u`,
			pgseed.DefaultSyncIntegrationID, pgseed.DefaultSyncSourceID, backfillBigRuns, backfillPlannedUnitsPerBg),
		// every other active run: one running unit
		fmt.Sprintf(`INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, attempts, created_at, status, available_at, updated_at)
			SELECT md5('ru' || g)::uuid, 'org-materializer', md5('a' || g)::uuid, '%[1]s'::uuid, '%[2]s'::uuid, 'github', 'commits', 'rest_core', 'backfill', 0, now(), 'running', NULL, now()
			FROM generate_series(%[3]d + 1, %[4]d) AS g`,
			pgseed.DefaultSyncIntegrationID, pgseed.DefaultSyncSourceID, backfillBigRuns, backfillActiveRuns),
		// terminal history: two finished units per terminal run
		fmt.Sprintf(`INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, attempts, created_at, status, available_at, updated_at)
			SELECT md5('tu' || g || '-' || u)::uuid, 'org-materializer', md5('t' || g)::uuid, '%[1]s'::uuid, '%[2]s'::uuid, 'github', 'commits', 'rest_core', 'incremental', 0, now() - (g || ' minutes')::interval, 'success', NULL, now() - (g || ' minutes')::interval
			FROM generate_series(1, %[3]d) AS g, generate_series(1, 2) AS u`,
			pgseed.DefaultSyncIntegrationID, pgseed.DefaultSyncSourceID, backfillTerminalRuns),
		fmt.Sprintf(`INSERT INTO public.sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
			SELECT md5('ad' || g)::uuid, md5('a' || g)::uuid, 'org-materializer', 'planned', 0, now() - (g || ' seconds')::interval, now(), now()
			FROM generate_series(1, %d) AS g`, backfillDueDiscoveries),
		fmt.Sprintf(`INSERT INTO public.sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, completed_at, created_at, updated_at)
			SELECT md5('td' || g)::uuid, md5('t' || g)::uuid, 'org-materializer', 'completed', 1, now() - (g || ' minutes')::interval, now(), now(), now()
			FROM generate_series(1, %d) AS g`, backfillTerminalDiscovery),
		fmt.Sprintf(`INSERT INTO public.sync_run_post_dispatches (id, org_id, sync_run_id, kind, dispatched_at, recorded_at)
			SELECT md5('pd' || g)::uuid, 'org-materializer', md5('t' || g)::uuid, 'post_sync', now() - (g || ' minutes')::interval, now() - (g || ' minutes')::interval
			FROM generate_series(1, %d) AS g`, backfillLedgerRows),
		fmt.Sprintf(`INSERT INTO public.sync_dispatch_outbox (id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at)
			SELECT md5('po' || g)::uuid, 'org-materializer', md5('t' || g)::uuid, 'post_sync', 'pending', now(), 0, now(), now()
			FROM generate_series(1, %d) AS g`, backfillLedgerRows),
	}
	for i, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("seed statement %d: %v", i, err)
		}
	}
	// VACUUM ANALYZE sets the visibility map and hint bits the way a steady production table has them;
	// without it the first scans pay a one-time cost that production never sees.
	if _, err := pool.Exec(ctx, "VACUUM (ANALYZE)"); err != nil {
		t.Fatalf("vacuum analyze: %v", err)
	}
}

func timeBackfillStatement(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name, sql string, args ...any) time.Duration {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL jit = off"); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS) "+strings.Replace(sql, "RETURNING", "RETURNING", 1), args...)
	if err != nil {
		t.Fatalf("%s explain: %v", name, err)
	}
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("%s explain rows: %v", name, err)
	}
	var elapsed time.Duration
	for _, line := range plan {
		var ms float64
		if _, err := fmt.Sscanf(strings.TrimSpace(line), "Execution Time: %f ms", &ms); err == nil {
			elapsed = time.Duration(ms * float64(time.Millisecond))
		}
	}
	t.Logf("%s: %v\n%s", name, elapsed, strings.Join(plan, "\n"))
	return elapsed
}

// TestMaterializerStatementsFitTheStageBudgetAtBackfillScale is the CHAOS-11430 proof:
// each candidate statement stays under backfillStatementCeiling and the real Step under
// backfillStepCeiling (2x margin on the 600 ms stage budget) on a backfill-shaped history.
func TestMaterializerStatementsFitTheStageBudgetAtBackfillScale(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
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
	if err := createMaterializerIntegrationFixture(ctx, t, pool); err != nil {
		t.Fatal(err)
	}
	seedBackfillBudgetPopulation(t, ctx, pool)

	now := time.Now().UTC()
	cutoff := now.Add(-time.Minute)
	limit := maximumStepLimit
	// Measurement: seed, VACUUM ANALYZE, one untimed warm-up pass of the four statements, then the timed pass
	// below (EXPLAIN ANALYZE "Execution Time" per statement, rolled-back tx, jit off, warm cache), then ONE real
	// Step (wall clock, committed, runs last). The statement sum and Step differ because Step is a single
	// transaction of the same four statements plus the runaway report and commit, with no EXPLAIN overhead.
	timeBackfillStatement(t, ctx, pool, "warmup-dispatch", materializeDispatchSQL, now, cutoff, limit)
	timeBackfillStatement(t, ctx, pool, "warmup-finalize", materializeFinalizeSQL, now, limit)
	timeBackfillStatement(t, ctx, pool, "warmup-discovery", materializeDiscoverySQL, now, limit, cutoff)
	timeBackfillStatement(t, ctx, pool, "warmup-post_sync", materializePostSyncSQL, now, limit)
	timings := map[string]time.Duration{
		"dispatch":  timeBackfillStatement(t, ctx, pool, "dispatch", materializeDispatchSQL, now, cutoff, limit),
		"finalize":  timeBackfillStatement(t, ctx, pool, "finalize", materializeFinalizeSQL, now, limit),
		"discovery": timeBackfillStatement(t, ctx, pool, "discovery", materializeDiscoverySQL, now, limit, cutoff),
		"post_sync": timeBackfillStatement(t, ctx, pool, "post_sync", materializePostSyncSQL, now, limit),
	}
	var sum time.Duration
	for name, elapsed := range timings {
		sum += elapsed
		if elapsed > backfillStatementCeiling {
			t.Errorf("%s took %v, want <= %v", name, elapsed, backfillStatementCeiling)
		}
	}
	t.Logf("statement sum %v", sum)

	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	stepCtx, stepCancel := context.WithTimeout(ctx, 10*time.Second)
	defer stepCancel()
	if _, err := materializer.Step(stepCtx, now, cutoff, limit); err != nil {
		t.Fatalf("Step: %v", err)
	}
	step := time.Since(started)
	t.Logf("Step wall time %v", step)
	if step > backfillStepCeiling {
		t.Errorf("Step took %v, want <= %v (600 ms stage budget with 2x margin)", step, backfillStepCeiling)
	}
}
