//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestClickHouseSourceDataCheckerMatchesDailyEligibilityIntersection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
    org_id String, repo_id UUID,
    started_at Nullable(DateTime64(3, 'UTC')), finished_at Nullable(DateTime64(3, 'UTC'))
) ENGINE = MergeTree ORDER BY (org_id, repo_id)`,
		`CREATE TABLE cicd_metrics_daily (org_id String, repo_id UUID, day Date)
 ENGINE = MergeTree ORDER BY (org_id, repo_id, day)`,
		`CREATE TABLE deployments (
    org_id String, repo_id UUID,
    deployed_at Nullable(DateTime64(3, 'UTC')), finished_at Nullable(DateTime64(3, 'UTC')),
    started_at Nullable(DateTime64(3, 'UTC')), last_synced Nullable(DateTime64(3, 'UTC'))
) ENGINE = MergeTree ORDER BY (org_id, repo_id)`,
		`CREATE TABLE deploy_metrics_daily (org_id String, repo_id UUID, day Date)
 ENGINE = MergeTree ORDER BY (org_id, repo_id, day)`,
		`CREATE TABLE test_suite_results (
    org_id String, repo_id UUID,
    started_at Nullable(DateTime64(3, 'UTC')), finished_at Nullable(DateTime64(3, 'UTC'))
) ENGINE = MergeTree ORDER BY (org_id, repo_id)`,
		`CREATE TABLE testops_release_confidence (org_id String, repo_id UUID, day Date)
 ENGINE = MergeTree ORDER BY (org_id, repo_id, day)`,
		`CREATE TABLE testops_pipeline_stability (org_id String, repo_id UUID, day Date)
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

	const (
		runID       = "00000000-0000-4000-8000-000000000120"
		partitionID = "00000000-0000-4000-8000-000000000121"
		orgID       = "00000000-0000-4000-8000-000000000009"
		repoID      = "00000000-0000-4000-8000-000000000002"
	)
	insertPartitionScope(t, ctx, pool, runID, partitionID, orgID, "2026-08-25", []RepositoryID{repoID})

	if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs VALUES
('00000000-0000-4000-8000-000000000009', toUUID('00000000-0000-4000-8000-000000000002'),
 toDateTime64('2026-08-24 23:00:00', 3, 'UTC'), toDateTime64('2026-08-25 01:00:00', 3, 'UTC')),
('00000000-0000-4000-8000-000000000009', toUUID('00000000-0000-4000-8000-000000000002'),
 toDateTime64('2026-08-25 02:00:00', 3, 'UTC'), toDateTime64('2026-08-25 03:00:00', 3, 'UTC'))`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO cicd_metrics_daily VALUES
('00000000-0000-4000-8000-000000000009', toUUID('00000000-0000-4000-8000-000000000002'), toDate('2026-08-25'))`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO deployments VALUES
('00000000-0000-4000-8000-000000000009', toUUID('00000000-0000-4000-8000-000000000002'),
 NULL, toDateTime64('2026-08-25 01:00:00', 3, 'UTC'),
 toDateTime64('2026-08-25 00:30:00', 3, 'UTC'), NULL)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO operational_incidents VALUES
('00000000-0000-4000-8000-000000000009', 'incident-eligibility', 'service-1',
 toDateTime64('2026-08-24 01:00:00', 6, 'UTC'), 'source-eligibility', 1, 0,
 toDateTime64('2026-08-24 01:00:00', 3, 'UTC'),
 toDateTime64('2026-08-25 02:00:00', 3, 'UTC'), 'resolved',
 toDateTime64('2026-08-25 02:00:00', 3, 'UTC'))`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO operational_service_repository_mappings VALUES
('00000000-0000-4000-8000-000000000009', 'mapping-eligibility', 'service-1',
 toUUID('00000000-0000-4000-8000-000000000002'),
 toDateTime64('2026-08-24 01:00:00', 6, 'UTC'), 'source-eligibility', 1, 0, 1,
 toDateTime64('2026-08-24 00:00:00', 6, 'UTC'), NULL)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO repos VALUES
('00000000-0000-4000-8000-000000000009', toUUID('00000000-0000-4000-8000-000000000002'))`); err != nil {
		t.Fatal(err)
	}
	checker, err := NewClickHouseSourceDataChecker(pool, conn)
	if err != nil {
		t.Fatal(err)
	}
	families, err := checker.ZeroRowFamiliesWithSourceData(ctx, partitionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 1 || families[0] != "testops_risk" {
		t.Fatalf("flagged families = %v, want [testops_risk]", families)
	}
}
