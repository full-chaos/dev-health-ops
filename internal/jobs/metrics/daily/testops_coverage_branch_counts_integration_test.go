//go:build integration

package daily

// The daily coverage row stores the branch counts of the newest coverage snapshot beside branch_coverage_pct, so a
// benchmarking point can pool them. The counts and the stored pct come from one snapshot row and agree; a snapshot with no
// counts stores NULL counts, and the row that retires a day stores none.

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestCoverageRowStoresBranchCounts(t *testing.T) {
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

	const orgID = "00000000-0000-4000-8000-000000014181"
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000141b1")
	day := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	seedTestopsDifferentialFixture(ctx, t, conn, orgID, repoID, day)
	seedTestopsAuthoritativeOwner(ctx, t, conn, orgID, repoID, day)

	executor, err := NewTestopsCoverageExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	compute := func() {
		t.Helper()
		n++
		id := time.Now().Format("150405.000") + string(rune('a'+n))
		run := Run{ID: id, OrganizationID: orgID, TargetDay: day}
		part := Partition{ID: "part-" + id, RunID: id, RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
		if _, err := executor.ComputeFamily(ctx, run, part); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1100 * time.Millisecond)
	}
	type stored struct {
		total, covered *uint32
		pct            *float64
	}
	read := func() stored {
		t.Helper()
		var s stored
		if err := conn.QueryRow(ctx, `SELECT (argMax(tuple(branches_total), computed_at)).1, (argMax(tuple(branches_covered), computed_at)).1, (argMax(tuple(branch_coverage_pct), computed_at)).1
FROM testops_coverage_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ?`, orgID, repoID, day).Scan(&s.total, &s.covered, &s.pct); err != nil {
			t.Fatal(err)
		}
		return s
	}
	// run-1/snap-b is the newest snapshot of the day. A newer version of it carries the branch counts.
	snapshot := func(version time.Time, total, covered *uint32, pct *float64) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO coverage_snapshots
(repo_id, run_id, snapshot_id, lines_total, lines_covered, line_coverage_pct, branches_total, branches_covered, branch_coverage_pct, team_id, service_id, org_id, last_synced)
VALUES (?, 'run-1', 'snap-b', 1000, 815, 81.5, ?, ?, ?, NULL, NULL, ?, ?)`, repoID, total, covered, pct, orgID, version); err != nil {
			t.Fatal(err)
		}
	}
	u := func(v uint32) *uint32 { return &v }
	f := func(v float64) *float64 { return &v }

	snapshot(time.Now().UTC().Add(time.Hour), u(200), u(40), f(20))
	compute()
	got := read()
	if got.total == nil || got.covered == nil || *got.total != 200 || *got.covered != 40 {
		t.Fatalf("stored branch counts = %v/%v, want 40/200", got.covered, got.total)
	}
	if got.pct == nil || math.Abs(*got.pct-100*float64(*got.covered)/float64(*got.total)) > 1e-9 {
		t.Fatalf("stored branch pct %v does not agree with the counts", got.pct)
	}

	// A snapshot with no counts stores NULL counts, never 0.
	snapshot(time.Now().UTC().Add(2*time.Hour), nil, nil, nil)
	compute()
	got = read()
	if got.total != nil || got.covered != nil {
		t.Fatalf("a snapshot with no counts stored %v/%v, want NULL", got.covered, got.total)
	}

	// The row that retires the day carries no counts: put counts back, then move the runs to a later day.
	snapshot(time.Now().UTC().Add(3*time.Hour), u(200), u(40), f(20))
	compute()
	if got = read(); got.total == nil {
		t.Fatal("the counts did not come back")
	}
	later := day.AddDate(0, 0, 40).Add(9 * time.Hour)
	for _, runID := range []string{"run-1", "run-prior"} {
		if err := conn.Exec(ctx, `INSERT INTO ci_pipeline_runs
(repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, duration_seconds, queue_seconds, retry_count, team_id, service_id, org_id)
VALUES (?, ?, 'success', ?, ?, ?, ?, 100, 0.1, 0, NULL, NULL, ?)`,
			repoID, runID, later, later, later.Add(time.Minute), time.Now().UTC().Add(4*time.Hour), orgID); err != nil {
			t.Fatal(err)
		}
	}
	compute()
	var retracted uint8
	if err := conn.QueryRow(ctx, `SELECT argMax(retracted, computed_at) FROM testops_coverage_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ?`, orgID, repoID, day).Scan(&retracted); err != nil {
		t.Fatal(err)
	}
	if retracted != 1 {
		t.Fatalf("the day was not retired (retracted = %d)", retracted)
	}
	if got = read(); got.total != nil || got.covered != nil {
		t.Fatalf("the row that retires the day stored counts %v/%v, want NULL", got.covered, got.total)
	}
}
