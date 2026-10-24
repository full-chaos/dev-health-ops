//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestFinalizeOverMoreThanOnePageOfUnitsWritesEveryCheckpointAndReadsBoundedPages: 1007 units, one of them failed
// between two page boundaries. Finalize folds all of them and writes one checkpoint per successful unit through
// keyset pages of at most unitPageSize rows.
func TestFinalizeOverMoreThanOnePageOfUnitsWritesEveryCheckpointAndReadsBoundedPages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createFinalizeTables(t, ctx, pool)
	seedFinalizeRoute(t, ctx, pool)

	const total = 2*unitPageSize + 7
	const failedOrdinal = unitPageSize + 200
	insertFinalizeRun(t, ctx, pool, total, "", "")
	pgseed.EnsureSyncIntegration(ctx, t, pool, finalizeTestOrg, finalizeTestIntegration, finalizeTestSource)
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class,
	mode, status, attempts, processor_flags, created_at, updated_at, since_at, before_at)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid, $1, $2::uuid, $3::uuid, $4::uuid,
	'github', 'commits', 'rest_core', 'incremental', 'success', 0, '{}'::json, now(), now(),
	'2026-07-01T00:00:00Z', '2026-07-02T00:00:00Z'
FROM generate_series(1, $5::int) AS g`,
		finalizeTestOrg, finalizeTestRun, finalizeTestIntegration, finalizeTestSource, total); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE sync_run_units SET status = 'failed', error = 'synthetic failure text',
	result = '{"error_category":"synthetic_category","reason":"synthetic_reason"}'::json
WHERE id = ('00000000-0000-4000-8000-' || lpad(to_hex($1::int), 12, '0'))::uuid`, failedOrdinal); err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var pages []int
	seen := map[string]bool{}
	err = forEachUnitPage(unitPageSize,
		func(afterID string, limit int) ([]finalizeSyncRunUnit, error) {
			return loadFinalizeUnitPage(ctx, tx, finalizeTestRun, afterID, limit)
		},
		func(unit finalizeSyncRunUnit) string { return unit.id },
		func(page []finalizeSyncRunUnit) error {
			pages = append(pages, len(page))
			for _, unit := range page {
				if seen[unit.id] {
					return fmt.Errorf("unit %s read twice", unit.id)
				}
				seen[unit.id] = true
			}
			return nil
		})
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{unitPageSize, unitPageSize, 6}; !slices.Equal(pages, want) {
		t.Fatalf("success pages %v, want %v (every page at most %d rows; the failed unit is not a success row)", pages, want, unitPageSize)
	}

	service, err := NewNativeFinalizeSyncRunService(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	var status string
	var completed, failed int
	var category, reason *string
	if err := pool.QueryRow(ctx, `SELECT status, completed_units, failed_units, result->>'error_category', result->>'reason' FROM sync_runs WHERE id=$1`, finalizeTestRun).
		Scan(&status, &completed, &failed, &category, &reason); err != nil {
		t.Fatal(err)
	}
	if status != syncRunStatusPartialFailed || completed != total-1 || failed != 1 {
		t.Fatalf("status=%q completed=%d failed=%d, want %s/%d/1", status, completed, failed, syncRunStatusPartialFailed, total-1)
	}
	if category == nil || *category != "synthetic_category" || reason == nil || *reason != "synthetic_reason" {
		t.Fatalf("first failed unit's category/reason = %v/%v", category, reason)
	}
	var checkpoints, distinctUnits int
	if err := pool.QueryRow(ctx, `
SELECT count(*), count(DISTINCT sync_run_unit_id) FROM sync_compute_checkpoints
WHERE sync_run_id=$1 AND compute_type='work_graph'`, finalizeTestRun).Scan(&checkpoints, &distinctUnits); err != nil {
		t.Fatal(err)
	}
	if checkpoints != total-1 || distinctUnits != total-1 {
		t.Fatalf("checkpoints=%d over %d units, want one per successful unit (%d)", checkpoints, distinctUnits, total-1)
	}
}

// TestPostSyncPlanOverManyWorkItemUnitsPlansTheUnionOfTheirWindowDays: many successful work-items units over a few
// distinct windows, plus a unit of another dataset whose window must not count.
func TestPostSyncPlanOverManyWorkItemUnitsPlansTheUnionOfTheirWindowDays(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createPostSyncTables(t, ctx, pool)

	const (
		orgID         = "00000000-0000-4000-8000-000000000001"
		runID         = "00000000-0000-4000-8000-000000000002"
		outboxID      = "00000000-0000-4000-8000-000000000003"
		integrationID = "00000000-0000-4000-8000-000000000004"
		repositoryID  = "00000000-0000-4000-8000-000000000005"
	)
	seedPostSyncGraph(t, ctx, pool, orgID, runID, outboxID, integrationID, repositoryID,
		"00000000-0000-4000-8000-000000000006", "commits", "", "")
	windows := [][2]time.Time{
		{time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC)},
		{time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)},
		{time.Date(2026, 6, 28, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)},
	}
	const units = 3 * 40
	for i := 0; i < units; i++ {
		window := windows[i%len(windows)]
		since, before := window[0], window[1]
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
			ID:    fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x100+i),
			RunID: runID, OrgID: orgID, IntegrationID: integrationID, SourceID: repositoryID,
			DatasetKey: "work-items", Status: "success", SinceAt: &since, BeforeAt: &before,
		})
	}

	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: postSyncRouteGeneration,
	}}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	plan, err := loadPostSyncPlan(ctx, tx, args, now)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || !plan.WorkItems {
		t.Fatalf("plan %+v, want a work-items plan", plan)
	}
	var runStartedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT COALESCE(started_at, created_at) FROM sync_runs WHERE id=$1::uuid`, runID).Scan(&runStartedAt); err != nil {
		t.Fatal(err)
	}
	distinct := map[time.Time]struct{}{}
	for _, window := range windows {
		since, before := window[0], window[1]
		days, err := providersync.WorkItemsUnitWindowDays(&since, &before, runStartedAt.UTC())
		if err != nil {
			t.Fatal(err)
		}
		for _, day := range days {
			distinct[day] = struct{}{}
		}
	}
	var want []time.Time
	for day := range distinct {
		want = append(want, day)
	}
	slices.SortFunc(want, func(left, right time.Time) int { return left.Compare(right) })
	if !slices.EqualFunc(plan.WorkItemWindowDays, want, func(left, right time.Time) bool { return left.Equal(right) }) {
		t.Fatalf("planned window days\n got %v\nwant %v", plan.WorkItemWindowDays, want)
	}
}
