//go:build integration

package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The domain-role claim cells of CHAOS-14222 are in internal/syncdispatchruntime
// (a posture package); this file holds only the EXPLAIN property cell.

const backfillNextOrgID = "org-backfill-next"

type backfillNextExplainFixture struct {
	pool          *pgxpool.Pool
	integrationID string
	sourceID      string
	costClass     string
}

// startBackfillNextExplainFixture is a PostgreSQL with the migrated schema.
func startBackfillNextExplainFixture(t *testing.T, ctx context.Context) backfillNextExplainFixture {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	capability, ok := Capability("github", "tests")
	if !ok {
		t.Fatal("no github/tests capability")
	}
	integrationID, sourceID := uuid.NewString(), uuid.NewString()
	pgseed.EnsureSyncIntegration(ctx, t, pool, backfillNextOrgID, integrationID, sourceID)
	return backfillNextExplainFixture{pool: pool, integrationID: integrationID, sourceID: sourceID, costClass: string(capability.CostClass)}
}

// CHAOS-14222 (acp condition 3). With many runs of one source, the sibling
// lookup of the claim SQL reads sync_run_units through an index scan keyed by
// sync_run_id, never a sequential scan. The index name is not pinned: the
// planner picks any index that leads with sync_run_id. EXPLAIN only: the claim
// is not executed.
func TestBackfillClaimSiblingLookupIsAnIndexScanBySyncRunID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	fixture := startBackfillNextExplainFixture(t, ctx)
	d := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	run := uuid.NewString()
	pgseed.EnsureSyncRun(ctx, t, fixture.pool, pgseed.SyncRun{ID: run, OrgID: backfillNextOrgID, IntegrationID: fixture.integrationID})
	unit := uuid.NewString()
	since, before := d, d.AddDate(0, 0, 7)
	pgseed.InsertSyncRunUnit(ctx, t, fixture.pool, pgseed.SyncRunUnit{
		ID: unit, RunID: run, OrgID: backfillNextOrgID, IntegrationID: fixture.integrationID, SourceID: fixture.sourceID,
		Provider: "github", DatasetKey: "tests", CostClass: fixture.costClass, Mode: "backfill", Status: "dispatching",
		SinceAt: &since, BeforeAt: &before,
	})
	seedBackfillNextRuns(t, ctx, fixture, 2000, 8)
	if _, err := fixture.pool.Exec(ctx, `ANALYZE public.sync_run_units; ANALYZE public.sync_runs`); err != nil {
		t.Fatal(err)
	}
	rows, err := fixture.pool.Query(ctx, "EXPLAIN "+claimUnitSQL,
		unit, uuid.NewString(), d, d.Add(time.Minute), true, backfillNextOrgID)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPLAIN claimUnitSQL:\n%s", strings.Join(plan, "\n"))
	var runIndexes []string
	indexRows, err := fixture.pool.Query(ctx, `
SELECT indexname || ': ' || indexdef FROM pg_indexes
WHERE schemaname = 'public' AND tablename = 'sync_run_units' AND indexdef LIKE '%(sync_run_id%'
ORDER BY indexname`)
	if err != nil {
		t.Fatal(err)
	}
	for indexRows.Next() {
		var index string
		if err := indexRows.Scan(&index); err != nil {
			t.Fatal(err)
		}
		runIndexes = append(runIndexes, index)
	}
	if err := indexRows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("sync_run_units indexes led by sync_run_id:\n%s", strings.Join(runIndexes, "\n"))
	lateral := strings.Join(plan[backfillNextLateralStart(plan):], "\n")
	if !strings.Contains(lateral, "Index Cond: (sync_run_id = claimed.sync_run_id)") || strings.Contains(lateral, "Seq Scan") {
		t.Errorf("the sibling lookup is not an index scan keyed by sync_run_id:\n%s", lateral)
	}
}

// backfillNextLateralStart is the first plan line of the sibling aggregate.
func backfillNextLateralStart(plan []string) int {
	for index, line := range plan {
		if strings.Contains(line, "Aggregate") {
			return index
		}
	}
	return 0
}

// seedBackfillNextRuns adds runs backfill runs of perRun 7-day units each, all
// of the fixture source and dataset (the prod shape: many runs per source).
func seedBackfillNextRuns(t *testing.T, ctx context.Context, fixture backfillNextExplainFixture, runs, perRun int) {
	t.Helper()
	pool := fixture.pool
	exec := func(pool *pgxpool.Pool, sql string, args ...any) {
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(pool, `
INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units,
	failed_units, created_at)
SELECT md5('run-' || g)::uuid, $1, $2::uuid, 'pgseed', 'backfill', 'success', $3, $3, 0, now()
FROM generate_series(1, $4) AS g`, backfillNextOrgID, fixture.integrationID, perRun, runs)
	exec(pool, `
INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class,
	mode, status, attempts, since_at, before_at, created_at, updated_at)
SELECT gen_random_uuid(), $1, md5('run-' || r)::uuid, $2::uuid, $3::uuid, 'github', 'tests', $4, 'backfill', 'success', 1,
	timestamptz '2026-01-01' + (u * interval '7 days'), timestamptz '2026-01-01' + ((u + 1) * interval '7 days'), now(), now()
FROM generate_series(1, $5) AS r, generate_series(0, $6 - 1) AS u`,
		backfillNextOrgID, fixture.integrationID, fixture.sourceID, fixture.costClass, runs, perRun)
}
