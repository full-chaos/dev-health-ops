//go:build integration

package daily

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The pipeline stability of a repo-day needs a completed run (succeeded or failed): a day whose runs were all
// cancelled, skipped or never finished writes no stability row by design, so it is not "source data with zero rows".
// A day with a completed run and no stability row still is (the guard stays live). Release confidence and quality drag
// keep the any-run source.
func TestSourceDataCheckerAsksPipelineStabilityOnlyForADayWithACompletedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer postgres.Close(context.Background())
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range []string{
		`CREATE TABLE ci_pipeline_runs (
    org_id String, repo_id UUID, run_id String DEFAULT toString(generateUUIDv4()), status Nullable(String),
    started_at Nullable(DateTime64(3, 'UTC')), finished_at Nullable(DateTime64(3, 'UTC'))
) ENGINE = ReplacingMergeTree ORDER BY (org_id, repo_id, run_id)`,
		`CREATE TABLE deployments (
    org_id String, repo_id UUID,
    deployed_at Nullable(DateTime64(3, 'UTC')), finished_at Nullable(DateTime64(3, 'UTC')),
    started_at Nullable(DateTime64(3, 'UTC')), last_synced Nullable(DateTime64(3, 'UTC')),
    status Nullable(String)
) ENGINE = MergeTree ORDER BY (org_id, repo_id)`,
		`CREATE TABLE deploy_metrics_daily (org_id String, repo_id UUID, day Date)
 ENGINE = MergeTree ORDER BY (org_id, repo_id, day)`,
		`CREATE TABLE incident_metrics_daily (org_id String, repo_id UUID, day Date)
 ENGINE = MergeTree ORDER BY (org_id, repo_id, day)`,
		`CREATE TABLE operational_incidents (
    org_id String, id String, service_id Nullable(String), source_revision DateTime64(6, 'UTC'),
    source_conflict_key String, ingest_revision UInt128, is_deleted UInt8,
    started_at Nullable(DateTime64(3, 'UTC')), resolved_at Nullable(DateTime64(3, 'UTC')),
    normalized_status String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree ORDER BY (org_id, id)`,
		`CREATE TABLE operational_service_repository_mappings (
    org_id String, id String, service_id String, repo_id Nullable(UUID),
    source_revision DateTime64(6, 'UTC'), source_conflict_key String, ingest_revision UInt128,
    is_deleted UInt8, is_active UInt8, valid_from Nullable(DateTime64(6, 'UTC')),
    valid_to Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree ORDER BY (org_id, id)`,
		`CREATE TABLE repos (org_id String, id UUID)
 ENGINE = ReplacingMergeTree ORDER BY (org_id, id)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	chschema.CreateTables(ctx, t, conn, "test_suite_results", "testops_release_confidence", "testops_pipeline_stability", "testops_quality_drag", "cicd_metrics_daily")

	const (
		orgID         = "00000000-0000-4000-8000-000000000009"
		notCompleted  = "00000000-0000-4000-8000-000000013802"
		completedRepo = "00000000-0000-4000-8000-000000013803"
	)
	run := func(repo, status, started string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO ci_pipeline_runs (org_id, repo_id, status, started_at, finished_at) VALUES (?, toUUID(?), ?, toDateTime64(?, 3, 'UTC'), NULL)`,
			orgID, repo, status, started); err != nil {
			t.Fatal(err)
		}
	}
	output := func(table, repo string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO `+table+` (org_id, repo_id, day) VALUES (?, toUUID(?), toDate('2026-11-14'))`, orgID, repo); err != nil {
			t.Fatal(err)
		}
	}
	// Repo 13802: cancelled, skipped and running runs only (one padded, as the compute trims). Release confidence and
	// quality drag were written; no stability row.
	run(notCompleted, "cancelled", "2026-11-14 01:00:00")
	run(notCompleted, "  Skipped ", "2026-11-14 02:00:00")
	run(notCompleted, "running", "2026-11-14 03:00:00")
	output("testops_release_confidence", notCompleted)
	output("testops_quality_drag", notCompleted)
	// Repo 13803: one success among cancelled runs; release confidence and quality drag written, stability missing.
	run(completedRepo, "cancelled", "2026-11-14 01:00:00")
	run(completedRepo, "Success", "2026-11-14 02:00:00")
	output("testops_release_confidence", completedRepo)
	output("testops_quality_drag", completedRepo)

	checker, err := NewClickHouseSourceDataChecker(pool, conn)
	if err != nil {
		t.Fatal(err)
	}
	flagged := func(runID, partitionID, repo string) []string {
		t.Helper()
		insertPartitionScope(t, ctx, pool, runID, partitionID, orgID, "2026-11-14", []RepositoryID{RepositoryID(repo)})
		families, err := zeroRowFamiliesNow(ctx, checker, partitionID)
		if err != nil {
			t.Fatal(err)
		}
		return families
	}
	if families := flagged("00000000-0000-4000-8000-000000000130", "00000000-0000-4000-8000-000000000131", notCompleted); slices.Contains(families, "testops_risk") {
		t.Errorf("no completed run: flagged %v, want no testops_risk (a day with no completed run has no stability by design)", families)
	}
	if families := flagged("00000000-0000-4000-8000-000000000132", "00000000-0000-4000-8000-000000000133", completedRepo); !slices.Contains(families, "testops_risk") {
		t.Errorf("a completed run and no stability row: flagged %v, want testops_risk (the guard stays live)", families)
	}
}
