//go:build integration

package syncreconciler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// CHAOS-8458: the dispatch statement runs every second. In steady state almost
// every sync_run_units row belongs to a terminal run, and a handful of runs are
// active. The statement must reach units through the active runs, not by
// walking the whole unit table by status.
const (
	dispatchPlanTerminalRuns    = 5000
	dispatchPlanUnitsPerRun     = 11
	dispatchPlanActiveRuns      = 17
	dispatchPlanTerminalPlanned = 1248
	// A probe per active run reads a few rows; a status walk reads tens of thousands.
	dispatchPlanMaxUnitRowsTouched = 2000
)

func seedDispatchPlanPopulation(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_runs (id, org_id, integration_id, mode, total_units, completed_units, failed_units, triggered_by, status, created_at)
		SELECT gen_random_uuid(), 'org-materializer', $2::uuid, 'incremental', 0, 0, 0, 'manual',
			CASE WHEN g <= $1 THEN 'success' ELSE 'running' END,
			now() - (g || ' minutes')::interval
		FROM generate_series(1, $1 + $3) AS g`,
		dispatchPlanTerminalRuns, pgseed.DefaultSyncIntegrationID, dispatchPlanActiveRuns); err != nil {
		t.Fatalf("seed sync_runs: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		WITH numbered AS (
			SELECT id, row_number() OVER (ORDER BY created_at DESC) AS n,
				(status IN ('success','partial_failed','failed')) AS terminal
			FROM public.sync_runs
		)
		INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key,
			cost_class, mode, attempts, created_at, status, available_at, updated_at)
		SELECT gen_random_uuid(), 'org-materializer', numbered.id, $1::uuid, $2::uuid, 'github', 'ds-' || u,
			'rest_core', 'incremental', 0, now(),
			CASE WHEN NOT numbered.terminal THEN (ARRAY['planned','running','success'])[1 + (u % 3)]
				WHEN numbered.n % 4 = 0 AND u = 1 THEN 'planned'
				ELSE 'success' END,
			NULL, now()
		FROM numbered CROSS JOIN generate_series(1, $3) AS u`,
		pgseed.DefaultSyncIntegrationID, pgseed.DefaultSyncSourceID, dispatchPlanUnitsPerRun); err != nil {
		t.Fatalf("seed sync_run_units: %v", err)
	}
	for _, statement := range []string{
		"VACUUM (ANALYZE) public.sync_runs",
		"VACUUM (ANALYZE) public.sync_run_units",
		"ANALYZE public.scheduled_sync_occurrences, public.sync_dispatch_outbox",
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

type dispatchPlanNode struct {
	NodeType    string             `json:"Node Type"`
	Relation    string             `json:"Relation Name"`
	IndexName   string             `json:"Index Name"`
	ActualRows  float64            `json:"Actual Rows"`
	ActualLoops float64            `json:"Actual Loops"`
	RowsRemoved float64            `json:"Rows Removed by Filter"`
	RowsRecheck float64            `json:"Rows Removed by Index Recheck"`
	Plans       []dispatchPlanNode `json:"Plans"`
}

func (n dispatchPlanNode) unitRowsTouched() float64 {
	total := 0.0
	if n.Relation == "sync_run_units" {
		total += (n.ActualRows + n.RowsRemoved + n.RowsRecheck) * n.ActualLoops
	}
	for _, child := range n.Plans {
		total += child.unitRowsTouched()
	}
	return total
}

func TestMaterializeDispatchSQLReachesUnitsThroughActiveRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	seedDispatchPlanPopulation(t, ctx, pool)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL jit = off"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	var explainJSON []byte
	if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+materializeDispatchSQL,
		now, now.Add(-time.Hour), maximumStepLimit).Scan(&explainJSON); err != nil {
		t.Fatal(err)
	}
	var plans []struct {
		Plan dispatchPlanNode `json:"Plan"`
	}
	if err := json.Unmarshal(explainJSON, &plans); err != nil || len(plans) != 1 {
		t.Fatalf("parse EXPLAIN output: %v\n%s", err, explainJSON)
	}
	touched := plans[0].Plan.unitRowsTouched()
	t.Logf("sync_run_units rows touched: %.0f of %d\n%s", touched,
		(dispatchPlanTerminalRuns+dispatchPlanActiveRuns)*dispatchPlanUnitsPerRun, explainJSON)
	if touched > dispatchPlanMaxUnitRowsTouched {
		t.Fatalf("the dispatch statement touched %.0f sync_run_units rows, want at most %d: it walks the unit table instead of probing it from the active runs",
			touched, dispatchPlanMaxUnitRowsTouched)
	}
}
