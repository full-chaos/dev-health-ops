//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func seedRemovedRepositoryRow(t *testing.T, ctx context.Context, conn driver.Conn, org, repo, name string, synced time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
VALUES (?, ?, ?, ?, ?, 'github')`, repo, name, synced, synced, org); err != nil {
		t.Fatalf("seed repos %s: %v", name, err)
	}
}

// seedTestopsInputRow writes one (repo_id, day) row of a TestOps input table.
func seedTestopsInputRow(t *testing.T, ctx context.Context, conn driver.Conn, table, org, repo string, day, computedAt time.Time, retracted uint8) {
	t.Helper()
	statements := map[string]string{
		"testops_pipeline_metrics_daily": `INSERT INTO testops_pipeline_metrics_daily
(repo_id, day, team_id, service_id, success_count, failure_count, success_rate, failure_rate, rerun_rate,
 median_duration_seconds, p95_duration_seconds, avg_queue_seconds, p95_queue_seconds, retracted,
 computed_at, org_id) VALUES
(toUUID(?), ?, NULL, NULL, 9, 1, 0.9, 0.1, 0.05, 100.0, 200.0, 10.0, 20.0, ?, ?, ?)`,
		"testops_test_metrics_daily": `INSERT INTO testops_test_metrics_daily
(repo_id, day, total_cases, flake_rate, retry_dependency_rate, failure_recurrence_score, retracted, computed_at, org_id) VALUES
(toUUID(?), ?, 10, 0.2, 0.1, 0.5, ?, ?, ?)`,
		"testops_coverage_metrics_daily": `INSERT INTO testops_coverage_metrics_daily
(repo_id, day, line_coverage_pct, branch_coverage_pct, uncovered_files_count, coverage_regression_count, team_id,
 retracted, computed_at, org_id) VALUES
(toUUID(?), ?, 80, 70, 0, 0, NULL, ?, ?, ?)`,
	}
	if err := conn.Exec(ctx, statements[table], repo, day, retracted, computedAt, org); err != nil {
		t.Fatalf("seed %s %s: %v", table, day.Format("2006-01-02"), err)
	}
}

// A whole-organization run whose every repository was removed upstream still
// gets a partition per stale repos row (discovery is not changed), so finalize
// takes the compute path, not the empty-run path. The day's benchmarking rows
// retire when no input key of the window [D-89, D] is live in any partition.
// Live input in the window, in any one of the three TestOps tables (also old
// input of a removed repository at D-89), keeps every row, and so does a run
// that is not of the whole organization.
func TestBenchmarkingRetiresTheDayWhenNoInputOfTheWindowIsLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	conn := capabilityRetireClickHouse(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	now := day.Add(40 * time.Hour)
	stored := day.Add(30 * time.Hour)

	const (
		pipeline = "testops_pipeline_metrics_daily"
		tests    = "testops_test_metrics_daily"
		coverage = "testops_coverage_metrics_daily"
	)
	type input struct {
		table      string
		repo       int
		offsetDays int
		retracted  bool
	}
	cells := []struct {
		name    string
		org     string
		repos   []string
		inputs  []input
		fullOrg bool
		retired bool
	}{
		{"every partition has no input (two stale repos rows)", "00000000-0000-4000-8000-000000141170",
			[]string{"00000000-0000-4000-8000-00000014117a", "00000000-0000-4000-8000-00000014117b"}, nil, true, true},
		{"one partition with live input at D-3", "00000000-0000-4000-8000-000000141171",
			[]string{"00000000-0000-4000-8000-00000014117c", "00000000-0000-4000-8000-00000014117d"}, []input{{pipeline, 1, -3, false}}, true, false},
		{"input only before the window (D-90)", "00000000-0000-4000-8000-000000141172",
			[]string{"00000000-0000-4000-8000-00000014117e"}, []input{{pipeline, 0, -90, false}}, true, true},
		{"input in the window whose newest row is a retraction", "00000000-0000-4000-8000-000000141173",
			[]string{"00000000-0000-4000-8000-00000014117f"}, []input{{pipeline, 0, -3, false}, {pipeline, 0, -3, true}}, true, true},
		{"removed repository, old input at the window start D-89", "00000000-0000-4000-8000-000000141174",
			[]string{"00000000-0000-4000-8000-000000141180"}, []input{{pipeline, 0, -89, false}}, true, false},
		{"live input only in the test table", "00000000-0000-4000-8000-000000141175",
			[]string{"00000000-0000-4000-8000-000000141181"}, []input{{tests, 0, -3, false}}, true, false},
		{"live input only in the coverage table", "00000000-0000-4000-8000-000000141176",
			[]string{"00000000-0000-4000-8000-000000141182"}, []input{{coverage, 0, -3, false}}, true, false},
		{"no input, run not of the whole organization", "00000000-0000-4000-8000-000000141177",
			[]string{"00000000-0000-4000-8000-000000141183"}, nil, false, false},
	}
	untouchedOrg := "00000000-0000-4000-8000-000000141178"

	for _, cell := range cells {
		for _, repo := range cell.repos {
			seedRemovedRepositoryRow(t, ctx, conn, cell.org, repo, "acme/"+repo[len(repo)-4:], day.Add(-120*24*time.Hour))
		}
		seedBenchmarkingOutputs(t, ctx, conn, cell.org, cell.repos[0], day, stored)
		for index, in := range cell.inputs {
			var marker uint8
			if in.retracted {
				marker = 1
			}
			seedTestopsInputRow(t, ctx, conn, in.table, cell.org, cell.repos[in.repo], day.AddDate(0, 0, in.offsetDays),
				day.Add(time.Duration(index+1)*time.Hour), marker)
		}
	}
	// Another day of a retired organization, and an organization never finalized.
	seedBenchmarkingOutputs(t, ctx, conn, cells[0].org, cells[0].repos[0], day.AddDate(0, 0, -1), stored)
	seedBenchmarkingOutputs(t, ctx, conn, untouchedOrg, cells[0].repos[0], day, stored)

	discoverer, err := NewClickHouseRepositoryDiscoverer(conn)
	if err != nil {
		t.Fatal(err)
	}
	discovered, err := discoverer.RepositoryIDs(ctx, cells[0].org)
	if err != nil {
		t.Fatalf("discovery: %v", err)
	}
	t.Logf("discovery of the organization whose repositories were removed: %v", discovered)
	if len(discovered) != 2 {
		t.Errorf("discovery gives %d partition(s), want 2: each stale repos row stays a partition", len(discovered))
	}

	finalize := func(org string, fullOrg bool) (int, error) {
		store := &fakeStore{partitionTotal: 2, partitionSucceeded: 2,
			run: Run{ID: "run-14117c", OrganizationID: org, TargetDay: day, FullOrg: fullOrg}}
		executor, err := NewBenchmarkingFinalizeExecutor(store, conn, nil)
		if err != nil {
			t.Fatal(err)
		}
		executor.nowUTC = func() time.Time { return now }
		return executor.ComputeFinalizeFamily(ctx, Run{ID: "run-14117c", OrganizationID: org, TargetDay: day})
	}
	liveOf := func(org string, on time.Time) map[string]uint64 {
		live := map[string]uint64{}
		for table := range benchmarkingOutputKeys {
			live[table] = benchmarkingLiveKeys(t, ctx, conn, table, org, on)
		}
		return live
	}

	for _, cell := range cells {
		written, computeErr := finalize(cell.org, cell.fullOrg)
		live := liveOf(cell.org, day)
		t.Logf("%s: finalize written=%d err=%v live=%v", cell.name, written, computeErr, live)
		if computeErr != nil {
			t.Errorf("%s: finalize: %v", cell.name, computeErr)
		}
		for table, keys := range live {
			switch {
			case cell.retired && keys != 0:
				t.Errorf("%s: %s %d live key(s) of the day, want 0 (no live input in the window)", cell.name, table, keys)
			case !cell.retired && keys == 0:
				t.Errorf("%s: %s 0 live key(s) of the day, want the stored key kept", cell.name, table)
			}
		}
		if cell.retired && written != len(benchmarkingOutputKeys) {
			t.Errorf("%s: written=%d, want %d retraction rows", cell.name, written, len(benchmarkingOutputKeys))
		}
	}

	again, err := finalize(cells[0].org, true)
	t.Logf("second finalize of %s: written=%d err=%v", cells[0].name, again, err)
	if err != nil || again != 0 {
		t.Errorf("second finalize = (%d, %v), want (0, nil): a retired key gets no second marker", again, err)
	}
	for name, live := range map[string]map[string]uint64{
		"the day before of the retired organization": liveOf(cells[0].org, day.AddDate(0, 0, -1)),
		"an organization never finalized":            liveOf(untouchedOrg, day),
	} {
		t.Logf("%s: live=%v", name, live)
		for table, keys := range live {
			if keys != 1 {
				t.Errorf("%s: %s %d live key(s), want 1 (not touched)", name, table, keys)
			}
		}
	}
}
