//go:build integration

package daily

// CHAOS-14008 C1, C2: a recompute of a testops coverage day that no longer has a coverage record retires the day's stored
// row with a marker row (the newest row of the key reads retracted = 1), writes nothing for a repository with no stored row, and
// the day is live again when a later compute finds a record. Run 1's pipeline row is re-synced to a later day (a cross-day
// re-run), so the coverage read of the first day finds no record.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestCoverageDayWithNoRecordIsRetired(t *testing.T) {
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

	const orgID = "00000000-0000-4000-8000-000000000014"
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000000b1")
	day := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	seedTestopsDifferentialFixture(ctx, t, conn, orgID, repoID, day)
	seedTestopsAuthoritativeOwner(ctx, t, conn, orgID, repoID, day)

	executor, err := NewTestopsCoverageExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	other := uuid.MustParse("00000000-0000-4000-8000-0000000000b2")
	compute := func(id string, repos ...uuid.UUID) int {
		t.Helper()
		run := Run{ID: id, OrganizationID: orgID, TargetDay: day}
		ids := make([]RepositoryID, 0, len(repos))
		for _, repo := range repos {
			ids = append(ids, RepositoryID(repo.String()))
		}
		part := Partition{ID: "part-" + id, RunID: id, RepoIDs: ids}
		written, err := executor.ComputeFamily(ctx, run, part)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		time.Sleep(1100 * time.Millisecond)
		return written
	}
	// liveRows counts the keys of the day whose NEWEST row is a measurement (retracted = 0); storedRows counts every row.
	liveRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM (SELECT repo_id FROM testops_coverage_metrics_daily
WHERE org_id = ? AND day = ? GROUP BY repo_id, day HAVING argMax(retracted, computed_at) = 0)`, orgID, day).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	storedRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM testops_coverage_metrics_daily WHERE org_id = ? AND day = ?`, orgID, day).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	if written := compute("first", repoID, other); written != 1 {
		t.Fatalf("the first compute wrote %d rows, want 1 (the second repository has no stored row and no record)", written)
	}
	if got := liveRows(); got != 1 {
		t.Fatalf("after the first compute: %d live keys, want 1", got)
	}

	// Move run-1 and run-prior to a later day with a newer version: their coverage snapshots leave both windows.
	later := day.AddDate(0, 0, 40).Add(9 * time.Hour)
	moveRuns := func(at time.Time, synced time.Time) {
		t.Helper()
		for _, runID := range []string{"run-1", "run-prior"} {
			if err := conn.Exec(ctx, `INSERT INTO ci_pipeline_runs
(repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, duration_seconds, queue_seconds, retry_count, team_id, service_id, org_id)
VALUES (?, ?, 'success', ?, ?, ?, ?, 100, 0.1, 0, NULL, NULL, ?)`,
				repoID, runID, at, at, at.Add(time.Minute), synced, orgID); err != nil {
				t.Fatalf("move %s: %v", runID, err)
			}
		}
	}
	moveRuns(later, day.Add(47*time.Hour))
	// C2: the recompute retires the key of the repository that has a live row and writes nothing for the one that has none.
	if written := compute("second", repoID, other); written != 1 {
		t.Fatalf("the recompute with no record wrote %d rows, want 1 marker row (none for the repository with no stored row)", written)
	}
	if got := liveRows(); got != 0 {
		t.Fatalf("a coverage day with no record still has %d live key(s); want the day retired", got)
	}
	if got := storedRows(); got != 2 {
		t.Fatalf("the table holds %d rows for the day, want the measured row and its marker row", got)
	}
	var team, retracted string
	if err := conn.QueryRow(ctx, `SELECT ifNull(argMax(team_id, computed_at), ''), toString(argMax(retracted, computed_at))
FROM testops_coverage_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?`, orgID, repoID, day).Scan(&team, &retracted); err != nil {
		t.Fatal(err)
	}
	t.Logf("C2CELL newest row of the retired key: retracted=%s team_id=%q", retracted, team)
	if retracted != "1" {
		t.Fatalf("the newest row retracted = %s, want 1", retracted)
	}
	// A recompute again with no record does not stack a second marker row.
	if written := compute("third", repoID, other); written != 0 {
		t.Fatalf("a recompute of an already retired day wrote %d rows, want 0", written)
	}

	// The record comes back (the run returns to the day): the next compute writes a measured row, live again.
	moveRuns(day.Add(9*time.Hour), day.Add(72*time.Hour))
	if written := compute("fourth", repoID, other); written != 1 {
		t.Fatalf("the recompute with a record wrote %d rows, want 1 measured row", written)
	}
	if got := liveRows(); got != 1 {
		t.Fatalf("after the record came back: %d live keys, want 1", got)
	}
}
