//go:build integration

package daily

// A recompute of a testops pipeline day that counts no run any more (the provider deleted the runs) retires the day's stored
// row with a marker row: the newest row of the key reads retracted = 1 and carries the team and the service of the row it
// retires. A repository with no stored row gets nothing; a second empty recompute writes nothing; the day is live again when
// its runs return; an older empty compute never retires a newer measured row; a repository outside the run's partition is
// never retired.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestTestopsPipelineDayWithNoRunIsRetired(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, instance)

	const orgID = "14148000-0000-4000-8000-000000000a00"
	repoID := uuid.MustParse("14148000-0000-4000-8000-000000000a01")
	other := uuid.MustParse("14148000-0000-4000-8000-000000000a02")
	outside := uuid.MustParse("14148000-0000-4000-8000-000000000a03")
	day := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)

	writeRun := func(repo uuid.UUID, runID, status string, synced time.Time) {
		t.Helper()
		started := day.Add(9 * time.Hour)
		if err := conn.Exec(ctx, `INSERT INTO ci_pipeline_runs
(repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, duration_seconds, queue_seconds, retry_count, team_id, service_id, org_id)
VALUES (?, ?, ?, ?, ?, ?, ?, 100, 5, 0, NULL, 'svc-a', ?)`,
			repo, runID, status, started.Add(-5*time.Second), started, started.Add(100*time.Second), synced, orgID); err != nil {
			t.Fatalf("write %s: %v", runID, err)
		}
	}
	executor, err := NewTestopsPipelineExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	compute := func(id string, repos ...uuid.UUID) int {
		t.Helper()
		ids := make([]RepositoryID, 0, len(repos))
		for _, repo := range repos {
			ids = append(ids, RepositoryID(repo.String()))
		}
		written, err := executor.ComputeFamily(ctx, Run{ID: id, OrganizationID: orgID, TargetDay: day},
			Partition{ID: "part-" + id, RunID: id, RepoIDs: ids})
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		time.Sleep(2100 * time.Millisecond) // a retiring marker sits one second below the next snapshot: two seconds apart
		return written
	}
	live := func(repo uuid.UUID) bool {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM (SELECT repo_id FROM testops_pipeline_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? GROUP BY repo_id HAVING argMax(retracted, computed_at) = 0)`, orgID, repo, day).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}
	stored := func(repo uuid.UUID) uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM testops_pipeline_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?`,
			orgID, repo, day).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	writeRun(repoID, "run-1", "success", day.Add(10*time.Hour))
	writeRun(outside, "run-out", "success", day.Add(10*time.Hour))
	if written := compute("first", repoID, other); written != 1 {
		t.Fatalf("the first compute wrote %d rows, want 1 (the second repository has no run and no stored row)", written)
	}
	if written := compute("outside-first", outside); written != 1 {
		t.Fatalf("the compute of the outside repository wrote %d rows, want 1", written)
	}
	if !live(repoID) || !live(outside) {
		t.Fatal("after the first computes the two repositories with runs are not live")
	}

	// The provider deletes the runs: a newer version with the stored status deleted is no run.
	writeRun(repoID, "run-1", "deleted", day.Add(20*time.Hour))
	writeRun(outside, "run-out", "deleted", day.Add(20*time.Hour))
	if written := compute("second", repoID, other); written != 1 {
		t.Fatalf("the recompute with no run wrote %d rows, want 1 marker row (none for the repository with no stored row)", written)
	}
	if live(repoID) {
		t.Fatal("a pipeline day with no run is still live; want it retired")
	}
	if got := stored(other); got != 0 {
		t.Fatalf("the repository with no stored row has %d rows, want none (missing stays missing)", got)
	}
	var retracted uint8
	var service, marker string
	if err := conn.QueryRow(ctx, `SELECT argMax(retracted, computed_at), ifNull(argMax(service_id, computed_at), ''),
toString(argMax(pooled_samples_stored, computed_at)) FROM testops_pipeline_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?`,
		orgID, repoID, day).Scan(&retracted, &service, &marker); err != nil {
		t.Fatal(err)
	}
	t.Logf("PIPELINE14148 newest row of the retired key: retracted=%d service_id=%q pooled_samples_stored=%s", retracted, service, marker)
	if retracted != 1 || service != "svc-a" {
		t.Fatalf("the newest row holds retracted %d, service %q; want 1 and the retired row's service svc-a", retracted, service)
	}
	// The run of another partition never retires a repository outside its set, also when that repository has no run any more.
	if !live(outside) {
		t.Fatal("a compute of another partition retired a repository outside its set")
	}
	if written := compute("third", repoID, other); written != 0 {
		t.Fatalf("a recompute of an already retired day wrote %d rows, want 0", written)
	}

	// The run returns: the next compute writes a measured row, live again.
	writeRun(repoID, "run-1", "success", day.Add(30*time.Hour))
	if written := compute("fourth", repoID, other); written != 1 {
		t.Fatalf("the recompute with a run wrote %d rows, want 1 measured row", written)
	}
	if !live(repoID) {
		t.Fatal("after the run came back the day is not live")
	}

	// An older empty compute never retires a newer measured row: the marker carries the run's own instant.
	writeRun(repoID, "run-1", "deleted", day.Add(40*time.Hour))
	if err := conn.Exec(ctx, `INSERT INTO testops_pipeline_metrics_daily (repo_id, day, pipelines_count, success_count, org_id, computed_at)
VALUES (?, ?, 1, 1, ?, ?)`, repoID, day, orgID, time.Now().UTC().Add(time.Hour).Truncate(time.Second)); err != nil {
		t.Fatal(err)
	}
	compute("fifth", repoID, other)
	if !live(repoID) {
		t.Fatal("an older empty compute retired a newer run's row")
	}
}
