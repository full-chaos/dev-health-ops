//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestCICDComputeFamilyWritesRealOrgIDAndIsolatesTenants is CICDExecutor's
// live-ClickHouse proof, run through the real production entry point
// (CICDExecutor.ComputeFamily), not a unit test of the writer in isolation --
// same shape as repo_user_commit's TestComputeFamilyWritesRealOrgIDAndIsolatesTenants:
//
//  1. Red-on-baseline shape: a cicd partition for org A must leave org-scoped
//     rows behind -- `SELECT count() FROM cicd_metrics_daily WHERE org_id =
//     <org A>` > 0.
//  2. Cross-tenant guard: two orgs, each with its own repo, run in the same
//     process. Org A's org-scoped read must see ONLY org A's row (never org
//     B's, and never a stray "" row), and vice versa.
//  3. Window-filter proof: a pipeline run counts on the UTC day it finishes,
//     so a run that started inside the target day but finished the NEXT day is
//     not counted on the target day -- exercised via repo B, which has one
//     in-window run and one cross-midnight run; its pipelines_count on the
//     target day must be 1, not 2.
func TestCICDComputeFamilyWritesRealOrgIDAndIsolatesTenants(t *testing.T) {
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

	chschema.CreateTables(ctx, t, conn, "cicd_metrics_daily")
	for _, statement := range []string{
		// Exact production shape: 000_raw_tables.sql + org_id added by
		// migration 027 (027_add_org_id_to_sorting_keys.py's
		// TABLES_NEEDING_ORG_ID_COLUMN list does NOT include ci_pipeline_runs
		// as needing the column added -- it already had it via migration 024
		// -- but the sorting key stays (repo_id, run_id) per that file's
		// TABLES map having no ci_pipeline_runs entry; org_id is filtered,
		// not keyed).
		`CREATE TABLE ci_pipeline_runs (
    repo_id UUID, run_id String, status Nullable(String),
    queued_at Nullable(DateTime64(3, 'UTC')), started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')), last_synced DateTime64(3, 'UTC'),
    org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id)`,
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

	if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, org_id) VALUES
(toUUID('`+repoA+`'), 'a-run-1', 'success',
 toDateTime64('2026-08-24 09:55:00', 3, 'UTC'), toDateTime64('2026-08-24 10:00:00', 3, 'UTC'),
 toDateTime64('2026-08-24 10:10:00', 3, 'UTC'), now64(3), '`+orgA+`'),
(toUUID('`+repoB+`'), 'b-run-in-window', 'failed',
 toDateTime64('2026-08-24 11:55:00', 3, 'UTC'), toDateTime64('2026-08-24 12:00:00', 3, 'UTC'),
 toDateTime64('2026-08-24 12:20:00', 3, 'UTC'), now64(3), '`+orgB+`'),
(toUUID('`+repoB+`'), 'b-run-cross-midnight', 'success',
 toDateTime64('2026-08-24 23:50:00', 3, 'UTC'), toDateTime64('2026-08-24 23:55:00', 3, 'UTC'),
 toDateTime64('2026-08-25 00:10:00', 3, 'UTC'), now64(3), '`+orgB+`')`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewCICDExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

	runA := Run{OrganizationID: orgA, TargetDay: targetDay}
	partitionA := Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []RepositoryID{RepositoryID(repoA)},
	}
	if _, err := executor.ComputeFamily(ctx, runA, partitionA); err != nil {
		t.Fatalf("org A partition: %v", err)
	}

	runB := Run{OrganizationID: orgB, TargetDay: targetDay}
	partitionB := Partition{
		ID: "00000000-0000-4000-8000-0000000000c3", RunID: "00000000-0000-4000-8000-0000000000c2",
		RepoIDs: []RepositoryID{RepositoryID(repoB)},
	}
	if _, err := executor.ComputeFamily(ctx, runB, partitionB); err != nil {
		t.Fatalf("org B partition: %v", err)
	}

	assertOrgScopedCount(ctx, t, conn, "cicd_metrics_daily", orgA, 1)
	assertOrgScopedCount(ctx, t, conn, "cicd_metrics_daily", orgB, 1)
	assertOrgScopedCount(ctx, t, conn, "cicd_metrics_daily", "", 0)

	// Point 3: window-filter proof -- repo B's pipelines_count must be 1
	// (the cross-midnight run finishes on 2026-08-25, so it is not counted
	// on 2026-08-24).
	row := conn.QueryRow(ctx, "SELECT pipelines_count FROM cicd_metrics_daily WHERE org_id = ? AND repo_id = ?", orgB, repoB)
	var pipelinesCount uint32
	if err := row.Scan(&pipelinesCount); err != nil {
		t.Fatalf("query row: %v", err)
	}
	if pipelinesCount != 1 {
		t.Fatalf("repo B pipelines_count = %d, want 1 (the cross-midnight run counts on the day it finishes)", pipelinesCount)
	}
}

// TestCICDComputeFamilyCountsRunOnFinishDay is the midnight-crossing cell: a
// run that starts on day D and finishes on D+1 counts on D+1, never on no day.
func TestCICDComputeFamilyCountsRunOnFinishDay(t *testing.T) {
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

	chschema.CreateTables(ctx, t, conn, "cicd_metrics_daily")
	for _, statement := range []string{
		`CREATE TABLE ci_pipeline_runs (
    repo_id UUID, run_id String, status Nullable(String),
    queued_at Nullable(DateTime64(3, 'UTC')), started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')), last_synced DateTime64(3, 'UTC'),
    org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const org = "00000000-0000-4000-8000-0000000000d0"
	const repo = "00000000-0000-4000-8000-0000000000d1"
	if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 'run-a', 'success', NULL,
 toDateTime64('2026-01-02 10:00:00', 3, 'UTC'), toDateTime64('2026-01-02 10:30:00', 3, 'UTC'), now64(3), '`+org+`'),
(toUUID('`+repo+`'), 'run-b', 'success', NULL,
 toDateTime64('2026-01-02 23:50:00', 3, 'UTC'), toDateTime64('2026-01-03 00:20:00', 3, 'UTC'), now64(3), '`+org+`')`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewCICDExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	for index, day := range []time.Time{
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
	} {
		run := Run{OrganizationID: org, TargetDay: day}
		partition := Partition{
			ID: "00000000-0000-4000-8000-0000000000e" + string(rune('1'+index)), RunID: "00000000-0000-4000-8000-0000000000e0",
			RepoIDs: []RepositoryID{RepositoryID(repo)},
		}
		if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("day %s: %v", day.Format(time.DateOnly), err)
		}
	}

	for day, want := range map[string]uint64{"2026-01-02": 1, "2026-01-03": 1} {
		var count uint64
		row := conn.QueryRow(ctx, "SELECT sum(pipelines_count) FROM cicd_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = toDate(?)", org, repo, day)
		if err := row.Scan(&count); err != nil {
			t.Fatalf("day %s: %v", day, err)
		}
		if count != want {
			t.Fatalf("day %s pipelines_count = %d, want %d", day, count, want)
		}
	}
}

func startCICDDayDB(t *testing.T) (context.Context, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clickhouseInstance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	chschema.CreateTables(ctx, t, conn, "cicd_metrics_daily")
	for _, statement := range []string{
		`CREATE TABLE ci_pipeline_runs (
    repo_id UUID, run_id String, status Nullable(String),
    queued_at Nullable(DateTime64(3, 'UTC')), started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')), last_synced DateTime64(3, 'UTC'),
    org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	return ctx, conn
}

func computeCICDDay(ctx context.Context, t *testing.T, executor *CICDExecutor, org, repo, day string, at time.Time) {
	t.Helper()
	parsed, err := time.Parse(time.DateOnly, day)
	if err != nil {
		t.Fatal(err)
	}
	executor.nowUTC = func() time.Time { return at }
	partition := Partition{
		ID: "00000000-0000-4000-8000-0000000000f1", RunID: "00000000-0000-4000-8000-0000000000f0",
		RepoIDs: []RepositoryID{RepositoryID(repo)},
	}
	if _, err := executor.ComputeFamily(ctx, Run{OrganizationID: org, TargetDay: parsed}, partition); err != nil {
		t.Fatalf("day %s: %v", day, err)
	}
}

func storedCICDCount(ctx context.Context, t *testing.T, conn driver.Conn, org, repo, day string) uint64 {
	t.Helper()
	var count uint64
	row := conn.QueryRow(ctx, "SELECT sum(pipelines_count) FROM cicd_metrics_daily FINAL WHERE org_id = ? AND repo_id = ? AND day = toDate(?)", org, repo, day)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("day %s: %v", day, err)
	}
	return count
}

// A run the provider has not finished carries its last update time in
// finished_at; it counts on no day.
func TestCICDComputeFamilyLeavesOpenRunsOut(t *testing.T) {
	ctx, conn := startCICDDayDB(t)
	const org = "00000000-0000-4000-8000-0000000000d0"
	const repo = "00000000-0000-4000-8000-0000000000d2"
	if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 'open-running', 'running', NULL,
 toDateTime64('2026-01-02 23:50:00', 3, 'UTC'), toDateTime64('2026-01-03 00:20:00', 3, 'UTC'), now64(3), '`+org+`'),
(toUUID('`+repo+`'), 'open-queued', 'queued', NULL,
 toDateTime64('2026-01-02 23:55:00', 3, 'UTC'), toDateTime64('2026-01-03 00:25:00', 3, 'UTC'), now64(3), '`+org+`'),
(toUUID('`+repo+`'), 'open-same-day', 'running', NULL,
 toDateTime64('2026-01-03 10:00:00', 3, 'UTC'), toDateTime64('2026-01-03 10:30:00', 3, 'UTC'), now64(3), '`+org+`')`); err != nil {
		t.Fatal(err)
	}
	executor, err := NewCICDExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	computeCICDDay(ctx, t, executor, org, repo, "2026-01-03", time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC))
	if got := storedCICDCount(ctx, t, conn, org, repo, "2026-01-03"); got != 0 {
		t.Fatalf("2026-01-03 pipelines_count = %d, want 0 (open runs count on no day)", got)
	}
}

// A recompute of a day that now has no run writes nothing: no zero-run row (a
// reader would take it for a measurement) and no overwrite of a count another
// job published. A stored count for a day whose run moved stays until a
// retraction design with a write fence exists.
func TestCICDComputeFamilyEmptyRecomputeWritesNothing(t *testing.T) {
	ctx, conn := startCICDDayDB(t)
	const org = "00000000-0000-4000-8000-0000000000d0"
	const repo = "00000000-0000-4000-8000-0000000000d3"
	insert := func(synced, started, finished string) {
		t.Helper()
		if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 'moved', 'success', NULL, toDateTime64('`+started+`', 3, 'UTC'), toDateTime64('`+finished+`', 3, 'UTC'),
 toDateTime64('`+synced+`', 3, 'UTC'), '`+org+`')`); err != nil {
			t.Fatal(err)
		}
	}
	executor, err := NewCICDExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	insert("2026-01-02 12:00:00", "2026-01-02 10:00:00", "2026-01-02 10:30:00")
	computeCICDDay(ctx, t, executor, org, repo, "2026-01-02", time.Date(2026, 1, 3, 1, 0, 0, 0, time.UTC))
	insert("2026-01-03 12:00:00", "2026-01-03 10:00:00", "2026-01-03 10:30:00")
	computeCICDDay(ctx, t, executor, org, repo, "2026-01-02", time.Date(2026, 1, 4, 1, 0, 0, 0, time.UTC))
	var zeroRows uint64
	row := conn.QueryRow(ctx, "SELECT count() FROM cicd_metrics_daily WHERE org_id = ? AND repo_id = ? AND pipelines_count = 0", org, repo)
	if err := row.Scan(&zeroRows); err != nil {
		t.Fatal(err)
	}
	if zeroRows != 0 {
		t.Fatalf("%d zero-run rows stored, want none", zeroRows)
	}
	if got := storedCICDCount(ctx, t, conn, org, repo, "2026-01-02"); got != 1 {
		t.Fatalf("2026-01-02 pipelines_count = %d, want the published 1 left as it was", got)
	}
}

// assertOrgScopedCount is defined in repo_user_commit_org_scope_integration_test.go
// (same package, same build tag) -- reused here rather than redefined.
