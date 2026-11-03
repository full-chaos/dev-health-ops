//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestCICDComputeFamilyDedupesUnmergedRunVersions proves two unmerged
// versions of one natural key count once. Merges are stopped so both parts
// stay on disk; the precondition asserts count() > count() FINAL.
func TestCICDComputeFamilyDedupesUnmergedRunVersions(t *testing.T) {
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

	for _, statement := range []string{
		`CREATE TABLE ci_pipeline_runs (
    repo_id UUID, run_id String, status Nullable(String),
    queued_at Nullable(DateTime64(3, 'UTC')), started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')), last_synced DateTime64(3, 'UTC'),
    org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, run_id)`,
		`CREATE TABLE cicd_metrics_daily (
    repo_id UUID, day Date, pipelines_count UInt32, success_rate Float64,
    avg_duration_minutes Nullable(Float64), p90_duration_minutes Nullable(Float64),
    avg_queue_minutes Nullable(Float64), computed_at DateTime, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)`,
		`SYSTEM STOP MERGES ci_pipeline_runs`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const org = "00000000-0000-4000-8000-0000000000a0"
	const repo = "00000000-0000-4000-8000-0000000000a1"

	// Separate INSERTs make separate parts.
	// run-1: v1 failed, v2 success (same window). run-2: v1 finished in the
	// window, v2 finished the next day (live version is outside the window).
	for _, statement := range []string{
		`INSERT INTO ci_pipeline_runs VALUES (toUUID('` + repo + `'), 'run-1', 'failed',
 toDateTime64('2026-08-24 09:55:00', 3, 'UTC'), toDateTime64('2026-08-24 10:00:00', 3, 'UTC'),
 toDateTime64('2026-08-24 10:10:00', 3, 'UTC'), toDateTime64('2026-08-24 11:00:00', 3, 'UTC'), '` + org + `')`,
		`INSERT INTO ci_pipeline_runs VALUES (toUUID('` + repo + `'), 'run-1', 'success',
 toDateTime64('2026-08-24 09:55:00', 3, 'UTC'), toDateTime64('2026-08-24 10:00:00', 3, 'UTC'),
 toDateTime64('2026-08-24 10:10:00', 3, 'UTC'), toDateTime64('2026-08-24 12:00:00', 3, 'UTC'), '` + org + `')`,
		`INSERT INTO ci_pipeline_runs VALUES (toUUID('` + repo + `'), 'run-2', 'failed',
 toDateTime64('2026-08-24 13:55:00', 3, 'UTC'), toDateTime64('2026-08-24 14:00:00', 3, 'UTC'),
 toDateTime64('2026-08-24 14:10:00', 3, 'UTC'), toDateTime64('2026-08-24 15:00:00', 3, 'UTC'), '` + org + `')`,
		`INSERT INTO ci_pipeline_runs VALUES (toUUID('` + repo + `'), 'run-2', 'success',
 toDateTime64('2026-08-24 13:55:00', 3, 'UTC'), toDateTime64('2026-08-24 14:00:00', 3, 'UTC'),
 toDateTime64('2026-08-25 01:10:00', 3, 'UTC'), toDateTime64('2026-08-24 16:00:00', 3, 'UTC'), '` + org + `')`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	var raw, merged uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM ci_pipeline_runs").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, "SELECT count() FROM ci_pipeline_runs FINAL").Scan(&merged); err != nil {
		t.Fatal(err)
	}
	if raw != 4 || merged != 2 {
		t.Fatalf("precondition: count()=%d count() FINAL=%d, want 4 and 2", raw, merged)
	}

	executor, err := NewCICDExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: org, TargetDay: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)}
	partition := Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []RepositoryID{RepositoryID(repo)},
	}
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("compute: %v", err)
	}

	var pipelines uint32
	var successRate float64
	if err := conn.QueryRow(ctx,
		"SELECT pipelines_count, success_rate FROM cicd_metrics_daily WHERE org_id = ? AND repo_id = ?",
		org, repo).Scan(&pipelines, &successRate); err != nil {
		t.Fatalf("query metrics: %v", err)
	}
	// Live rows: run-1 success (finished in window), run-2 finished next day
	// (excluded). Unmerged read would count 3 pipelines at 2/3.
	if pipelines != 1 || successRate != 1.0 {
		t.Fatalf("pipelines_count=%d success_rate=%v, want 1 and 1.0", pipelines, successRate)
	}
}
