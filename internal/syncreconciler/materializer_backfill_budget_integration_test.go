//go:build integration

package syncreconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
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

// backfillHistoryTables are the tables that grow with history; CHAOS-11430 was an unbounded read of them.
var backfillHistoryTables = []string{"sync_run_units", "sync_runs", "sync_run_reference_discoveries", "sync_run_post_dispatches"}

type backfillPlanNode struct {
	NodeType     string             `json:"Node Type"`
	RelationName string             `json:"Relation Name"`
	IndexName    string             `json:"Index Name"`
	SharedHit    int64              `json:"Shared Hit Blocks"`
	SharedRead   int64              `json:"Shared Read Blocks"`
	ActualRows   float64            `json:"Actual Rows"`
	ActualLoops  float64            `json:"Actual Loops"`
	RowsFiltered float64            `json:"Rows Removed by Filter"`
	RowsRecheck  float64            `json:"Rows Removed by Index Recheck"`
	Plans        []backfillPlanNode `json:"Plans"`
}

type backfillPlan struct {
	root          backfillPlanNode
	executionTime time.Duration
	indexes       map[string]bool
	seqScans      []string
	historyRows   int64 // rows a scan node read from a history table: (returned + removed by filter or recheck) x loops
}

func (plan backfillPlan) blocks() int64 { return plan.root.SharedHit + plan.root.SharedRead }

func isBackfillHistoryTable(name string) bool {
	for _, table := range backfillHistoryTables {
		if table == name {
			return true
		}
	}
	return false
}

// explainBackfillStatement runs EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) in a rolled-back transaction with jit off (as Step
// sets it) and summarizes the plan: indexes used, Seq Scans on history tables, rows read from history tables and the
// statement's shared hit+read blocks. Blocks and rows depend on the plan and the data, not on host load.
func explainBackfillStatement(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name, sql string, args ...any) backfillPlan {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL jit = off"); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+sql, args...).Scan(&raw); err != nil {
		t.Fatalf("%s explain: %v", name, err)
	}
	var decoded []struct {
		Plan          backfillPlanNode `json:"Plan"`
		ExecutionTime float64          `json:"Execution Time"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil || len(decoded) != 1 {
		t.Fatalf("%s explain json: %v (%d plans)", name, err, len(decoded))
	}
	plan := backfillPlan{
		root:          decoded[0].Plan,
		executionTime: time.Duration(decoded[0].ExecutionTime * float64(time.Millisecond)),
		indexes:       map[string]bool{},
	}
	var lines []string
	var walk func(node backfillPlanNode, depth int)
	walk = func(node backfillPlanNode, depth int) {
		if node.IndexName != "" {
			plan.indexes[node.IndexName] = true
		}
		if isBackfillHistoryTable(node.RelationName) {
			if node.NodeType == "Seq Scan" {
				plan.seqScans = append(plan.seqScans, node.RelationName)
			}
			plan.historyRows += int64((node.ActualRows + node.RowsFiltered + node.RowsRecheck) * node.ActualLoops)
		}
		lines = append(lines, fmt.Sprintf("%s%s %s %s rows=%.2f removed=%.0f loops=%.0f blocks=%d",
			strings.Repeat("  ", depth), node.NodeType, node.RelationName, node.IndexName,
			node.ActualRows, node.RowsFiltered+node.RowsRecheck, node.ActualLoops, node.SharedHit+node.SharedRead))
		for _, child := range node.Plans {
			walk(child, depth+1)
		}
	}
	walk(plan.root, 0)
	t.Logf("%s: blocks=%d history_rows_read=%d execution=%v (load-dependent, not asserted)\n%s",
		name, plan.blocks(), plan.historyRows, plan.executionTime, strings.Join(lines, "\n"))
	return plan
}

// TestMaterializerStatementsFitTheStageBudgetAtBackfillScale is the CHAOS-11430 guard on a backfill-shaped history. It
// asserts what does not move with host load: each candidate statement's plan uses the index the fix relies on, does no
// Seq Scan on a history table, and stays under a shared-block and a history-rows-read bound (about 2x the measured max
// of the fixed code, except post_sync blocks = 50000, sized to admit a legal nested-loop plan of ~40k blocks; for
// post_sync the split from the code before the fix comes from the index, no-Seq-Scan and rows checks). Wall times are logged only; the Step context timeout is a hang ceiling, not a budget.
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
	statements := []struct {
		name        string
		sql         string
		args        []any
		indexes     []string
		maxBlocks   int64
		maxHistRows int64
	}{
		// Bounds measured on this seed (fixed code / code before CHAOS-11430). Blocks: dispatch 121k / 457k, finalize 91k /
		// 91k, discovery 2.8k / 2.8k, post_sync 5.3k / 8.9k. History rows read: dispatch 60k / 560k, finalize 60k / 60k,
		// discovery 60k / 60k, post_sync 10k / 300k. finalize and discovery did not regress at this scale: their bounds
		// guard a future one.
		{"dispatch", materializeDispatchSQL, []any{now, cutoff, limit}, []string{"ix_sync_run_units_open_run"}, 245000, 120000},
		{"finalize", materializeFinalizeSQL, []any{now, limit}, []string{"ix_sync_run_units_open_run"}, 180000, 120000},
		{"discovery", materializeDiscoverySQL, []any{now, limit, cutoff}, nil, 6000, 120000},
		{"post_sync", materializePostSyncSQL, []any{now, limit}, []string{"ix_sync_run_post_dispatches_recorded"}, 50000, 20000},
	}
	for _, statement := range statements {
		plan := explainBackfillStatement(t, ctx, pool, statement.name, statement.sql, statement.args...)
		for _, index := range statement.indexes {
			if !plan.indexes[index] {
				used := make([]string, 0, len(plan.indexes))
				for name := range plan.indexes {
					used = append(used, name)
				}
				sort.Strings(used)
				t.Errorf("%s plan does not use %s (uses %v)", statement.name, index, used)
			}
		}
		if len(plan.seqScans) > 0 {
			t.Errorf("%s plan does a Seq Scan on %v", statement.name, plan.seqScans)
		}
		if plan.blocks() > statement.maxBlocks {
			t.Errorf("%s read %d shared blocks, want <= %d", statement.name, plan.blocks(), statement.maxBlocks)
		}
		if plan.historyRows > statement.maxHistRows {
			t.Errorf("%s read %d history-table rows, want <= %d", statement.name, plan.historyRows, statement.maxHistRows)
		}
	}

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
	t.Logf("Step wall time %v (load-dependent, not asserted)", time.Since(started))
}
