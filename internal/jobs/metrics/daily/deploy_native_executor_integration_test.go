//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestDeployComputeFamilyWritesRealOrgIDAndIsolatesTenants is CHAOS-4293's
// live-ClickHouse proof, run through the real production entry point
// (DeployExecutor.ComputeFamily), not a unit test of the writer in
// isolation. Mirrors TestComputeFamilyWritesRealOrgIDAndIsolatesTenants
// (repo_user_commit_org_scope_integration_test.go, CHAOS-4341) for the
// `deploy` family:
//
//  1. Correctness end to end: a real `deployments` row, read through
//     LoadDeployments and computed through numerical.ComputeDeployMetrics,
//     lands in deploy_metrics_daily with the exact counts/percentiles the
//     input implies -- not just "a row landed".
//  2. Cross-tenant guard: two orgs, each with its own repo and its own
//     deployment, run in the same process. Org A's org-scoped read must see
//     ONLY org A's row (never org B's, and never a stray "" row), and vice
//     versa.
func TestDeployComputeFamilyWritesRealOrgIDAndIsolatesTenants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

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

	// Real production DDL: migrations/clickhouse/000_raw_tables.sql:108-122
	// (deployments) and 004_quality_delivery_metrics.sql:35-45
	// (deploy_metrics_daily), plus 024_add_org_id.sql's org_id ALTER.
	for _, statement := range []string{
		`CREATE TABLE deployments (
    repo_id UUID, deployment_id String, status Nullable(String), environment Nullable(String),
    started_at Nullable(DateTime64(3, 'UTC')), finished_at Nullable(DateTime64(3, 'UTC')),
    deployed_at Nullable(DateTime64(3, 'UTC')), merged_at Nullable(DateTime64(3, 'UTC')),
    pull_request_number Nullable(UInt32), release_ref String DEFAULT '',
    release_ref_confidence Float64 DEFAULT 0.0, org_id String DEFAULT 'default',
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, deployment_id)`,
		`CREATE TABLE deploy_metrics_daily (
    repo_id UUID, day Date, deployments_count UInt32, failed_deployments_count UInt32,
    deploy_time_p50_hours Nullable(Float64), lead_time_p50_hours Nullable(Float64),
    computed_at DateTime('UTC'), org_id String DEFAULT 'default'
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgA = "00000000-0000-4000-8000-0000000000a0"
		orgB = "00000000-0000-4000-8000-0000000000b0"
	)
	repoA := "00000000-0000-4000-8000-0000000000a1"
	repoB := "00000000-0000-4000-8000-0000000000b1"
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

	if err := conn.Exec(ctx, `
INSERT INTO deployments (repo_id, deployment_id, status, started_at, finished_at, deployed_at, merged_at, org_id, last_synced) VALUES
(toUUID('`+repoA+`'), 'a-d1', 'success', toDateTime64('2026-08-24 10:00:00', 3, 'UTC'), toDateTime64('2026-08-24 11:00:00', 3, 'UTC'), toDateTime64('2026-08-24 12:00:00', 3, 'UTC'), toDateTime64('2026-08-24 09:00:00', 3, 'UTC'), '`+orgA+`', now64(3)),
(toUUID('`+repoA+`'), 'a-d2', 'failure', NULL, NULL, toDateTime64('2026-08-24 14:00:00', 3, 'UTC'), NULL, '`+orgA+`', now64(3)),
(toUUID('`+repoB+`'), 'b-d1', 'success', NULL, NULL, toDateTime64('2026-08-24 13:00:00', 3, 'UTC'), NULL, '`+orgB+`', now64(3))`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewDeployExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}

	runA := Run{OrganizationID: orgA, TargetDay: targetDay}
	partitionA := Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []RepositoryID{RepositoryID(repoA)},
	}
	writtenA, err := executor.ComputeFamily(ctx, runA, partitionA)
	if err != nil {
		t.Fatalf("org A partition: %v", err)
	}
	if writtenA != 1 {
		t.Fatalf("org A partition: wrote %d rows, want 1 (one repo)", writtenA)
	}

	runB := Run{OrganizationID: orgB, TargetDay: targetDay}
	partitionB := Partition{
		ID: "00000000-0000-4000-8000-0000000000c3", RunID: "00000000-0000-4000-8000-0000000000c2",
		RepoIDs: []RepositoryID{RepositoryID(repoB)},
	}
	if _, err := executor.ComputeFamily(ctx, runB, partitionB); err != nil {
		t.Fatalf("org B partition: %v", err)
	}

	// Point 1: correctness -- repoA has 2 deployments (1 success + 1
	// failure), deploy_time_p50=1h (only d1 has started/finished), no
	// lead_time (only d1 has merged_at, but d2 lacks it -- still 1 sample,
	// p50 of a single value equals that value: 3h).
	var (
		deploymentsCount, failedCount uint32
		deployP50, leadP50            *float64
	)
	row := conn.QueryRow(ctx, `SELECT deployments_count, failed_deployments_count, deploy_time_p50_hours, lead_time_p50_hours FROM deploy_metrics_daily WHERE org_id = ? AND repo_id = ?`, orgA, repoA)
	if err := row.Scan(&deploymentsCount, &failedCount, &deployP50, &leadP50); err != nil {
		t.Fatalf("scan org A deploy metrics: %v", err)
	}
	if deploymentsCount != 2 || failedCount != 1 {
		t.Fatalf("org A: deployments_count=%d failed=%d, want 2/1", deploymentsCount, failedCount)
	}
	if deployP50 == nil || *deployP50 != 1.0 {
		t.Fatalf("org A: deploy_time_p50_hours=%v, want 1.0", deployP50)
	}
	if leadP50 == nil || *leadP50 != 3.0 {
		t.Fatalf("org A: lead_time_p50_hours=%v, want 3.0", leadP50)
	}

	// Point 2: cross-tenant guard -- org A's read must NOT see org B's row,
	// org B's read must NOT see org A's, and neither org's read may pick up
	// a stray org_id="" row (the exact pre-CHAOS-4341-fix shape those other
	// families hit).
	assertOrgScopedCount(ctx, t, conn, "deploy_metrics_daily", orgA, 1)
	assertOrgScopedCount(ctx, t, conn, "deploy_metrics_daily", orgB, 1)
	assertOrgScopedCount(ctx, t, conn, "deploy_metrics_daily", "", 0)
}
