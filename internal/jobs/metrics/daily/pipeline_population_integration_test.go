//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// cicd_metrics_daily and testops_pipeline_metrics_daily are written from the same ci_pipeline_runs but NOT from the
// same population of a day (CHAOS-13842): cicd keeps the runs that started AND finished in the day, testops every run
// that started in the day. The Home CI Success tile reads the testops table, as the Pipelines series and the Delivery
// Risk factor do, so a run that crosses midnight counts on its start day and a repo-day that only has unfinished runs
// has a row. This test records that difference with the real writers on a real ClickHouse; it is the proof for the
// choice, and it fails if the two windows are ever made the same (then the choice can be revisited).
func TestCICDAndTestopsPipelineMetricsDifferOnMidnightAndUnfinishedRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(inst.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, inst)

	const org = "00000000-0000-4000-8000-0000000000a0"
	repo1, repo2 := uuid.New(), uuid.New()
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	insert := func(repo uuid.UUID, run, status, started, finished string) {
		t.Helper()
		finishedSQL := "NULL"
		if finished != "" {
			finishedSQL = "toDateTime64('" + finished + "', 3, 'UTC')"
		}
		if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, finished_at, last_synced, org_id) VALUES ('%s', '%s', '%s', toDateTime64('%s', 3, 'UTC'), %s, toDateTime64('2026-08-25 06:00:00', 3, 'UTC'), '%s')`,
			repo, run, status, started, finishedSQL, org)); err != nil {
			t.Fatal(err)
		}
	}
	// repo 1: a run that finished in the day, one that started at 23:50 and finished after midnight, one still running.
	insert(repo1, "r1", "success", "2026-08-24 10:00:00", "2026-08-24 10:10:00")
	insert(repo1, "r2", "failed", "2026-08-24 23:50:00", "2026-08-25 00:20:00")
	insert(repo1, "r3", "running", "2026-08-24 12:00:00", "")
	// repo 2: only a run that has not finished.
	insert(repo2, "r4", "running", "2026-08-24 13:00:00", "")

	run := Run{OrganizationID: org, TargetDay: day}
	partition := Partition{ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []RepositoryID{RepositoryID(repo1.String()), RepositoryID(repo2.String())}}
	cicd, err := NewCICDExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cicd.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("cicd: %v", err)
	}
	pipeline, err := NewTestopsPipelineExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("testops pipeline: %v", err)
	}

	type counts struct{ pipelines, success, failure uint32 }
	cicdRows := map[uuid.UUID]counts{}
	rows, err := conn.Query(ctx, `SELECT repo_id, pipelines_count, toUInt32(round(success_rate * pipelines_count)) FROM cicd_metrics_daily`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var repo uuid.UUID
		var c counts
		if err := rows.Scan(&repo, &c.pipelines, &c.success); err != nil {
			t.Fatal(err)
		}
		cicdRows[repo] = c
	}
	rows.Close()
	testopsRows := map[uuid.UUID]counts{}
	rows, err = conn.Query(ctx, `SELECT repo_id, pipelines_count, success_count, failure_count FROM testops_pipeline_metrics_daily`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var repo uuid.UUID
		var c counts
		if err := rows.Scan(&repo, &c.pipelines, &c.success, &c.failure); err != nil {
			t.Fatal(err)
		}
		testopsRows[repo] = c
	}
	rows.Close()

	// repo 1: cicd holds only the run that started and finished in the day; testops holds all three runs, one
	// success and one failure completed.
	if got := cicdRows[repo1]; got != (counts{pipelines: 1, success: 1}) {
		t.Errorf("cicd repo 1 = %+v, want 1 run, 1 success", got)
	}
	if got := testopsRows[repo1]; got != (counts{pipelines: 3, success: 1, failure: 1}) {
		t.Errorf("testops repo 1 = %+v, want 3 runs, 1 success, 1 failure", got)
	}
	// repo 2: no cicd row at all; testops has a row with no completed run.
	if _, ok := cicdRows[repo2]; ok {
		t.Errorf("cicd has a row for a repo-day with only an unfinished run: %+v", cicdRows[repo2])
	}
	if got, ok := testopsRows[repo2]; !ok || got != (counts{pipelines: 1}) {
		t.Errorf("testops repo 2 = %+v (present %v), want 1 run, none completed", got, ok)
	}
}
