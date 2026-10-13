//go:build integration

package daily

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// A repository with two main owners (a full ownership tie, D5990) gets one
// TestOps metrics row per main owner, each counting the repository whole. When
// the owners change, the next compute writes the new owners with a newer
// computed_at, and the row of an owner that left stays in an older run.
func TestTestopsPipelineWritesOneRowPerMainOwnerOfATiedRepository(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := repoOwnerFamiliesDay
	t0 := day.Add(-72 * time.Hour)

	for _, team := range []string{"team-a", "team-b"} {
		seedRepoOwnerFamiliesTeam(t, ctx, conn, team, 1, t0)
		seedRepoOwnerFamiliesOwnership(t, ctx, conn, team, 1, t0, t0)
	}
	if err := conn.Exec(ctx, `INSERT INTO ci_pipeline_runs
(repo_id, run_id, status, started_at, finished_at, last_synced, duration_seconds, queue_seconds, retry_count, team_id, service_id, org_id)
VALUES (?, 'run-1', 'success', ?, ?, ?, 60, 1, 0, NULL, NULL, ?)`,
		repoOwnerFamiliesRepo, day.Add(2*time.Hour), day.Add(3*time.Hour), day.Add(4*time.Hour), repoOwnerFamiliesOrg); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	computeTestopsPipeline(t, ctx, conn, day.Add(30*time.Hour))
	if got := newestTestopsPipelineTeams(t, ctx, conn); !reflect.DeepEqual(got, []string{"team-a", "team-b"}) {
		t.Fatalf("teams of the newest run = %v, want both main owners of the tie", got)
	}
	var counts []uint32
	rows, err := conn.Query(ctx, `SELECT pipelines_count FROM testops_pipeline_metrics_daily WHERE org_id = ? ORDER BY team_id`, repoOwnerFamiliesOrg)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var count uint32
		if err := rows.Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts = append(counts, count)
	}
	_ = rows.Close()
	if !reflect.DeepEqual(counts, []uint32{1, 1}) {
		t.Fatalf("pipelines_count by owner = %v, want 1 in each: every owner counts the repository whole", counts)
	}

	// team-b leaves and team-c takes its place as the second main owner.
	later := day.Add(40 * time.Hour)
	seedRepoOwnerFamiliesTeam(t, ctx, conn, "team-b", 0, later)
	seedRepoOwnerFamiliesTeam(t, ctx, conn, "team-c", 1, later)
	seedRepoOwnerFamiliesOwnership(t, ctx, conn, "team-c", 1, t0, later)
	computeTestopsPipeline(t, ctx, conn, day.Add(50*time.Hour))
	if got := newestTestopsPipelineTeams(t, ctx, conn); !reflect.DeepEqual(got, []string{"team-a", "team-c"}) {
		t.Fatalf("teams of the newest run = %v, want team-a and team-c; team-b left", got)
	}
}

func computeTestopsPipeline(t *testing.T, ctx context.Context, conn driver.Conn, clock time.Time) {
	t.Helper()
	executor, err := NewTestopsPipelineExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	executor.nowUTC = func() time.Time { return clock }
	run := Run{ID: uuid.NewString(), OrganizationID: repoOwnerFamiliesOrg, TargetDay: repoOwnerFamiliesDay}
	partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repoOwnerFamiliesRepo.String())}}
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("testops_pipeline at %s: %v", clock.Format(time.RFC3339), err)
	}
}

func newestTestopsPipelineTeams(t *testing.T, ctx context.Context, conn driver.Conn) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT ifNull(team_id, '') FROM testops_pipeline_metrics_daily
WHERE org_id = ? AND computed_at = (SELECT max(computed_at) FROM testops_pipeline_metrics_daily WHERE org_id = ?)`,
		repoOwnerFamiliesOrg, repoOwnerFamiliesOrg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var teams []string
	for rows.Next() {
		var team string
		if err := rows.Scan(&team); err != nil {
			t.Fatal(err)
		}
		teams = append(teams, team)
	}
	sort.Strings(teams)
	return teams
}
