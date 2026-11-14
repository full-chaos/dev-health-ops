//go:build integration

package daily

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const defaultBranchTestOrg = "00000000-0000-4000-8000-000000000019"

func startDefaultBranchClickHouse(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	chschema.Apply(ctx, t, instance)
	return conn
}

type defaultBranchRepo struct {
	id            uuid.UUID
	settings      string
	prBranch      string // branch of the "pull request" runs in this repo
	recordedInSQL bool   // false: no repos row at all
	prCasesOnly   bool   // no case rows for the default-branch run (suites stay)
	noCases       bool   // no case rows at all (suites stay)
	noPipelineRow string // run id that gets suite and case rows but NO ci_pipeline_runs row
}

type defaultBranchWindow struct {
	day, historyStart, start, end time.Time
}

func newDefaultBranchWindow() defaultBranchWindow {
	day := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	return defaultBranchWindow{
		day: day, historyStart: day.AddDate(0, 0, -29), start: day, end: day.AddDate(0, 0, 1),
	}
}

func execAll(ctx context.Context, t *testing.T, conn driver.Conn, statement string, rows [][]any) {
	t.Helper()
	for _, row := range rows {
		if err := conn.Exec(ctx, statement, row...); err != nil {
			t.Fatalf("%s %v: %v", statement, row, err)
		}
	}
}

// seedDefaultBranchRepo writes one repository whose day holds a default-branch
// run and a "pull request" run, plus a pull-request failure 5 days earlier:
//
//	r-main  (branch main)      suite 3 total / 2 passed / 1 failed
//	                           cases C passed, T passed (retry 0), R failed
//	r-pr    (branch prBranch)  suite 2 total / 1 passed / 1 failed
//	                           cases C failed, T passed (retry 1)
//	r-hist  (branch prBranch)  5 days earlier; case R failed
//
// When prBranch is the default branch the same rows ARE the control: C flakes,
// T needs its retry, and R recurs.
func seedDefaultBranchRepo(ctx context.Context, t *testing.T, conn driver.Conn, w defaultBranchWindow, repo defaultBranchRepo) {
	t.Helper()
	synced := w.day.Add(20 * time.Hour)
	if repo.recordedInSQL {
		execAll(ctx, t, conn, `INSERT INTO repos (id, repo, created_at, settings, last_synced, org_id) VALUES (?, ?, ?, ?, ?, ?)`,
			[][]any{{repo.id, "acme/" + repo.id.String(), synced, repo.settings, synced, defaultBranchTestOrg}})
	}
	type run struct {
		id, branch string
		started    time.Time
	}
	runs := []run{
		{"r-main", "main", w.day.Add(10 * time.Hour)},
		{"r-pr", repo.prBranch, w.day.Add(11 * time.Hour)},
		{"r-hist", repo.prBranch, w.day.AddDate(0, 0, -5).Add(11 * time.Hour)},
	}
	for _, r := range runs {
		if r.id != repo.noPipelineRow {
			execAll(ctx, t, conn, `INSERT INTO ci_pipeline_runs
(repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, retry_count, org_id, branch)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				[][]any{{repo.id, r.id, "success", r.started, r.started, r.started.Add(time.Minute), synced, uint32(0), defaultBranchTestOrg, r.branch}})
		}
		execAll(ctx, t, conn, `INSERT INTO test_suite_results
(repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count,
 quarantined_count, duration_seconds, started_at, finished_at, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[][]any{{repo.id, r.id, "s-" + r.id, "suite", suiteTotal(r.id), suitePassed(r.id), suiteFailed(r.id),
				uint32(0), uint32(0), uint32(0), 1.0, r.started, r.started.Add(time.Minute), defaultBranchTestOrg, synced}})
	}
	cases := [][]any{
		{"r-main", "c-main-C", "C", "passed", uint32(0)},
		{"r-main", "c-main-T", "T", "passed", uint32(0)},
		{"r-main", "c-main-R", "R", "failed", uint32(0)},
		{"r-pr", "c-pr-C", "C", "failed", uint32(0)},
		{"r-pr", "c-pr-T", "T", "passed", uint32(1)},
		{"r-hist", "c-hist-R", "R", "failed", uint32(0)},
	}
	for _, c := range cases {
		if repo.noCases || (repo.prCasesOnly && c[0] == "r-main") {
			continue
		}
		execAll(ctx, t, conn, `INSERT INTO test_case_results
(repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[][]any{{repo.id, c[0], "s-" + c[0].(string), c[1], c[2], c[3], c[4], defaultBranchTestOrg, synced}})
	}
	// Coverage: the pull-request run's id sorts AFTER the default branch's, so
	// a reader with no branch condition picks it.
	for _, r := range []struct{ runID, snapshot string }{{"r-main", "snap"}, {"r-pr", "snap"}} {
		pct := 80.0
		if r.runID == "r-pr" {
			pct = 10.0
		}
		execAll(ctx, t, conn, `INSERT INTO coverage_snapshots
(repo_id, run_id, snapshot_id, lines_total, lines_covered, line_coverage_pct, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			[][]any{{repo.id, r.runID, r.snapshot, uint32(100), uint32(pct), pct, defaultBranchTestOrg, synced}})
	}
}

func suiteTotal(runID string) uint32 {
	if runID == "r-main" {
		return 3
	}
	return 2
}

func suitePassed(runID string) uint32 {
	if runID == "r-main" {
		return 2
	}
	return 1
}

func suiteFailed(string) uint32 { return 1 }

func computeDefaultBranchTestMetric(ctx context.Context, t *testing.T, conn driver.Conn, w defaultBranchWindow, repoID uuid.UUID) testops.TestMetric {
	t.Helper()
	metrics, err := computeTestopsTestMetricsWithoutTeams(ctx, conn, defaultBranchTestOrg, repoID, w.historyStart, w.start, w.end)
	if err != nil {
		t.Fatalf("compute test metrics: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("got %d records, want 1", len(metrics))
	}
	return metrics[0]
}

// Flake, retry-dependency and failure-recurrence compare a case name's statuses
// across runs. A pull request that fails, is fixed and passes on a new commit
// is not a flake, so those three read the default branch's runs only. Pass and
// failure counts come from suite rows and keep counting every branch.
func TestTestopsFlakeRetryRecurrenceCountDefaultBranchRunsOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startDefaultBranchClickHouse(ctx, t)
	w := newDefaultBranchWindow()

	prRepo := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000d1"),
		settings: `{"source":"github","default_branch":"main"}`, prBranch: "feature/x", recordedInSQL: true}
	controlRepo := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000d2"),
		settings: `{"source":"github","default_branch":"main"}`, prBranch: "main", recordedInSQL: true}
	seedDefaultBranchRepo(ctx, t, conn, w, prRepo)
	seedDefaultBranchRepo(ctx, t, conn, w, controlRepo)

	control := computeDefaultBranchTestMetric(ctx, t, conn, w, controlRepo.id)
	if control.FlakeCases != 1 || control.DistinctCases != 3 || control.RetryDependencyRate == 0 || control.FailureRecurrence != 0.5 {
		t.Fatalf("control (the same rows, all on the default branch) must flake C, need T's retry and recur R: %+v", control)
	}

	got := computeDefaultBranchTestMetric(ctx, t, conn, w, prRepo.id)
	if got.FlakeCases != 0 || got.FlakeRate != 0 {
		t.Errorf("a pull-request failure of C counted as a flake: flake_cases=%d flake_rate=%v, want 0", got.FlakeCases, got.FlakeRate)
	}
	if got.RetryDependencyRate != 0 {
		t.Errorf("a pull-request retry counted: retry_dependency_rate=%v, want 0", got.RetryDependencyRate)
	}
	if got.FailureRecurrence != 0 {
		t.Errorf("a pull-request failure 5 days ago made R recur: failure_recurrence=%v, want 0", got.FailureRecurrence)
	}
	if got.DistinctCases != 3 {
		t.Errorf("distinct_cases=%d, want 3 (C, T, R from the default branch run)", got.DistinctCases)
	}
	// The chris decision: the pass rate keeps counting pull-request runs.
	if got.TotalCases != 5 || got.PassedCount != 3 || got.FailedCount != 2 {
		t.Errorf("suite counts total=%d passed=%d failed=%d, want 5/3/2 over both runs of the day",
			got.TotalCases, got.PassedCount, got.FailedCount)
	}
}

// A run is dropped only when it is KNOWN to sit on another branch. Without a
// recorded default branch the reader cannot tell, so it keeps every run, which
// is what it did before the filter existed. A run whose branch is NULL or empty
// is not known to be off the default branch either.
func TestTestopsDefaultBranchFilterKeepsUnknownRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startDefaultBranchClickHouse(ctx, t)
	w := newDefaultBranchWindow()

	noRow := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000d3"), prBranch: "feature/x"}
	seedDefaultBranchRepo(ctx, t, conn, w, noRow)
	if got := computeDefaultBranchTestMetric(ctx, t, conn, w, noRow.id); got.FlakeCases != 1 {
		t.Errorf("no repos row: flake_cases=%d, want 1 (nothing may be dropped when the default branch is unknown)", got.FlakeCases)
	}
	coverage, err := loadNativeTestopsLatestCoverage(ctx, conn, defaultBranchTestOrg, noRow.id, w.start, w.end)
	if err != nil || len(coverage) != 1 || coverage[0].RunID != "r-pr" {
		t.Errorf("no repos row: coverage=%+v err=%v, want the unfiltered latest (r-pr)", coverage, err)
	}

	nullBranch := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000d4"),
		settings: `{"default_branch":"main"}`, prBranch: "x", recordedInSQL: true}
	seedDefaultBranchRepo(ctx, t, conn, w, nullBranch)
	if err := conn.Exec(ctx, `ALTER TABLE ci_pipeline_runs UPDATE branch = NULL WHERE repo_id = ? AND run_id IN ('r-pr', 'r-hist') SETTINGS mutations_sync = 2`, nullBranch.id); err != nil {
		t.Fatal(err)
	}
	emptyBranch := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000d8"),
		settings: `{"default_branch":"main"}`, prBranch: "", recordedInSQL: true}
	seedDefaultBranchRepo(ctx, t, conn, w, emptyBranch)

	for name, repo := range map[string]defaultBranchRepo{"NULL branch": nullBranch, "empty branch": emptyBranch} {
		if got := computeDefaultBranchTestMetric(ctx, t, conn, w, repo.id); got.FlakeCases != 1 {
			t.Errorf("%s runs: flake_cases=%d, want 1 (a run with no recorded branch is not known to be off the default branch)", name, got.FlakeCases)
		}
		coverage, err := loadNativeTestopsLatestCoverage(ctx, conn, defaultBranchTestOrg, repo.id, w.start, w.end)
		if err != nil || len(coverage) != 1 || coverage[0].RunID != "r-pr" {
			t.Errorf("%s runs: coverage=%+v err=%v, want the unfiltered latest (r-pr)", name, coverage, err)
		}
	}
}

// A repo-day whose only case rows are of runs left out of the flake measures
// reads flake as unknown, not as 0 flakes; a day with at least one default
// branch case row, or with no case rows at all, is not marked.
func TestTestopsFlakeIsUnknownWhenOnlyOffDefaultBranchRunsHaveCases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startDefaultBranchClickHouse(ctx, t)
	w := newDefaultBranchWindow()

	repos := map[string]struct {
		repo        defaultBranchRepo
		wantUnknown bool
	}{
		"only pull-request case rows": {defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000e1"),
			settings: `{"default_branch":"main"}`, prBranch: "feature/x", recordedInSQL: true, prCasesOnly: true}, true},
		"default branch and pull-request case rows": {defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000e2"),
			settings: `{"default_branch":"main"}`, prBranch: "feature/x", recordedInSQL: true}, false},
		"suites, no case rows at all": {defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000e3"),
			settings: `{"default_branch":"main"}`, prBranch: "feature/x", recordedInSQL: true, noCases: true}, false},
		"default branch not recorded": {defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000e4"),
			prBranch: "feature/x", prCasesOnly: true}, false},
	}
	for name, test := range repos {
		seedDefaultBranchRepo(ctx, t, conn, w, test.repo)
		got := computeDefaultBranchTestMetric(ctx, t, conn, w, test.repo.id)
		if got.FlakeUnknown != test.wantUnknown {
			t.Errorf("%s: FlakeUnknown=%v, want %v (flake_cases=%d distinct=%d)", name, got.FlakeUnknown, test.wantUnknown, got.FlakeCases, got.DistinctCases)
		}
	}
	// The written row carries the unknown as a 0/0 count pair.
	unknownRepo := repos["only pull-request case rows"].repo
	metrics, err := computeTestopsTestMetricsWithoutTeams(ctx, conn, defaultBranchTestOrg, unknownRepo.id, w.historyStart, w.start, w.end)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeTestopsTestMetrics(ctx, conn, defaultBranchTestOrg, w.day, w.day, metrics); err != nil {
		t.Fatal(err)
	}
	var flakeCases, distinctCases *uint32
	if err := conn.QueryRow(ctx, `SELECT flake_cases, distinct_cases FROM testops_test_metrics_daily WHERE repo_id = ?`, unknownRepo.id).Scan(&flakeCases, &distinctCases); err != nil {
		t.Fatal(err)
	}
	if flakeCases == nil || distinctCases == nil || *flakeCases != 0 || *distinctCases != 0 {
		t.Errorf("written flake_cases/distinct_cases = %v/%v, want 0/0 for an unknown day (a NULL pair blanks the org series)", flakeCases, distinctCases)
	}
}

// The coverage tile takes the latest snapshot by run id. A pull-request run
// whose id sorts later must not win; the filter is on ci_pipeline_runs, so it
// holds whatever the test sync fetches reports for.
func TestTestopsLatestCoverageIgnoresNonDefaultBranchRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startDefaultBranchClickHouse(ctx, t)
	w := newDefaultBranchWindow()

	prRepo := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000d5"),
		settings: `{"default_branch":"main"}`, prBranch: "feature/x", recordedInSQL: true}
	controlRepo := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000d6"),
		settings: `{"default_branch":"main"}`, prBranch: "main", recordedInSQL: true}
	seedDefaultBranchRepo(ctx, t, conn, w, prRepo)
	seedDefaultBranchRepo(ctx, t, conn, w, controlRepo)

	control, err := loadNativeTestopsLatestCoverage(ctx, conn, defaultBranchTestOrg, controlRepo.id, w.start, w.end)
	if err != nil || len(control) != 1 || control[0].RunID != "r-pr" {
		t.Fatalf("control (both runs on the default branch) must pick the later run id r-pr: %+v err=%v", control, err)
	}
	got, err := loadNativeTestopsLatestCoverage(ctx, conn, defaultBranchTestOrg, prRepo.id, w.start, w.end)
	if err != nil || len(got) != 1 || got[0].RunID != "r-main" {
		t.Fatalf("latest coverage = %+v err=%v, want the default branch run r-main", got, err)
	}
}

// legacyCaseAggregateSQL is the case read as it was before the default-branch
// exclusion: scoped only by the two semi-joins.
const legacyCaseAggregateSQL = `
SELECT
  case_name,
  groupUniqArrayIf(ifNull(toString(status), ''), in_today) AS statuses,
  maxIf(retry_attempt, in_today) AS max_retry,
  max(in_today) AS seen_today,
  max(failed_in_history) AS failed_before
FROM (
  SELECT
    case_name,
    status,
    retry_attempt,
    run_id IN (
      SELECT run_id FROM test_suite_results FINAL
      WHERE org_id = ? AND repo_id = ?
        AND coalesce(started_at, finished_at) >= ? AND coalesce(started_at, finished_at) < ?
    ) AS run_in_today,
    run_in_today AND (run_id, suite_id) IN (
      SELECT run_id, suite_id FROM test_suite_results FINAL
      WHERE org_id = ? AND repo_id = ?
        AND coalesce(started_at, finished_at) < ?
    ) AS in_today,
    NOT run_in_today
      AND lower(trim(status)) IN (?, ?, ?, ?, ?, ?)
      AND (run_id, suite_id) IN (
        SELECT run_id, suite_id FROM test_suite_results FINAL
        WHERE org_id = ? AND repo_id = ?
          AND coalesce(started_at, finished_at) >= ? AND coalesce(started_at, finished_at) < ?
      ) AS failed_in_history
  FROM test_case_results FINAL
  WHERE org_id = ? AND repo_id = ? AND case_name != ''
)
WHERE in_today OR failed_in_history
GROUP BY case_name
ORDER BY case_name`

func legacyCaseAggregate(ctx context.Context, t *testing.T, conn driver.Conn, repoID uuid.UUID, w defaultBranchWindow) (testops.TestMetric, int) {
	t.Helper()
	return runCaseAggregateSQL(ctx, t, conn, repoID, w, legacyCaseAggregateSQL, []any{
		defaultBranchTestOrg, repoID, w.start.UTC(), w.end.UTC(),
		defaultBranchTestOrg, repoID, w.end.UTC(),
		"failure", "failed", "error", "errors", "timeout", "timed_out",
		defaultBranchTestOrg, repoID, w.historyStart.UTC(), w.start.UTC(),
		defaultBranchTestOrg, repoID})
}

// previousExclusionCaseAggregate is the case read as the first version of the
// default-branch exclusion wrote it: the legacy SQL plus a NOT IN on the case
// rows, so the excluded runs' case rows are read and then dropped.
func previousExclusionCaseAggregate(ctx context.Context, t *testing.T, conn driver.Conn, repoID uuid.UUID, w defaultBranchWindow, defaultBranch string) (testops.TestMetric, int) {
	t.Helper()
	sql := strings.Replace(legacyCaseAggregateSQL, "  WHERE org_id = ? AND repo_id = ? AND case_name != ''\n)", `  WHERE org_id = ? AND repo_id = ? AND case_name != ''
    AND run_id NOT IN (
      SELECT run_id FROM ci_pipeline_runs FINAL
      WHERE org_id = ? AND repo_id = ?
        AND run_id IN (
          SELECT run_id FROM test_suite_results FINAL
          WHERE org_id = ? AND repo_id = ?
            AND coalesce(started_at, finished_at) >= ? AND coalesce(started_at, finished_at) < ?
        )
        AND branch IS NOT NULL AND branch != '' AND branch != ?
    )
)`, 1)
	if sql == legacyCaseAggregateSQL {
		t.Fatal("the previous-exclusion SQL was not derived from the legacy SQL")
	}
	return runCaseAggregateSQL(ctx, t, conn, repoID, w, sql, []any{
		defaultBranchTestOrg, repoID, w.start.UTC(), w.end.UTC(),
		defaultBranchTestOrg, repoID, w.end.UTC(),
		"failure", "failed", "error", "errors", "timeout", "timed_out",
		defaultBranchTestOrg, repoID, w.historyStart.UTC(), w.start.UTC(),
		defaultBranchTestOrg, repoID,
		defaultBranchTestOrg, repoID, defaultBranchTestOrg, repoID, w.historyStart.UTC(), w.end.UTC(), defaultBranch})
}

func runCaseAggregateSQL(ctx context.Context, t *testing.T, conn driver.Conn, repoID uuid.UUID, w defaultBranchWindow, sql string, args []any) (testops.TestMetric, int) {
	t.Helper()
	accumulator := testops.NewTestAccumulator(repoID, "", nil)
	if err := loadNativeTestopsSuites(ctx, conn, accumulator, defaultBranchTestOrg, repoID, w.start, w.end); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		t.Fatalf("case aggregate: %v", err)
	}
	defer rows.Close()
	history := map[string]struct{}{}
	groups := 0
	for rows.Next() {
		var group testops.CaseGroup
		var seenToday, failedBefore uint8
		if err := rows.Scan(&group.CaseName, &group.Statuses, &group.MaxRetry, &seenToday, &failedBefore); err != nil {
			t.Fatal(err)
		}
		groups++
		if seenToday != 0 {
			accumulator.AddCaseGroup(group)
		}
		if failedBefore != 0 {
			history[group.CaseName] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	metrics := accumulator.Finish(history)
	if len(metrics) != 1 {
		t.Fatalf("case aggregate produced %d records", len(metrics))
	}
	return metrics[0], groups
}

// seedAncientHistoryFixture seeds one default-branch repo with every shape a
// bounded case read could get wrong (see the test below) and returns it with its
// window. Merges are stopped so the part layout, and with it read_rows, is fixed.
func seedAncientHistoryFixture(ctx context.Context, t *testing.T, conn driver.Conn) (defaultBranchRepo, defaultBranchWindow) {
	t.Helper()
	for _, table := range []string{"ci_pipeline_runs", "test_suite_results", "test_case_results", "repos"} {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatal(err)
		}
	}
	w := newDefaultBranchWindow()
	repo := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000d7"),
		settings: `{"default_branch":"main"}`, prBranch: "main", recordedInSQL: true}
	seedDefaultBranchRepo(ctx, t, conn, w, repo)
	synced := w.day.Add(20 * time.Hour)
	nextDay := w.day.AddDate(0, 0, 1).Add(time.Hour)
	ancient := w.day.AddDate(0, 0, -60)

	execAll(ctx, t, conn, `INSERT INTO test_suite_results
(repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count,
 quarantined_count, duration_seconds, started_at, finished_at, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [][]any{
		{repo.id, "r-main", "s-next-day", "suite", uint32(1), uint32(1), uint32(0), uint32(0), uint32(0), uint32(0), 1.0, nextDay, nextDay, defaultBranchTestOrg, synced},
		{repo.id, "r-ancient-suite", "s-ancient", "suite", uint32(1), uint32(0), uint32(1), uint32(0), uint32(0), uint32(0), 1.0, ancient, ancient, defaultBranchTestOrg, synced},
	})
	execAll(ctx, t, conn, `INSERT INTO test_case_results
(repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, [][]any{
		{repo.id, "r-main", "s-next-day", "c-next", "R", "passed", uint32(0), defaultBranchTestOrg, synced},
		{repo.id, "r-no-suite", "s-none", "c-none", "R", "passed", uint32(0), defaultBranchTestOrg, synced},
		{repo.id, "r-ancient-suite", "s-ancient", "c-ancient", "R", "failed", uint32(0), defaultBranchTestOrg, synced},
	})
	if err := conn.Exec(ctx, `INSERT INTO test_case_results
(repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, org_id, last_synced)
SELECT ?, concat('a-', toString(number)), 's', 'c', concat('ancient-', toString(number % 50)), 'failed', 0, ?, ?
FROM numbers(30000)`, repo.id, defaultBranchTestOrg, synced); err != nil {
		t.Fatalf("seed ancient case rows: %v", err)
	}

	return repo, w
}

// The case read with the default-branch exclusion must equal the read without
// it when the repo has no run known to be off the default branch, and must not
// grow with the repo's case history. The seed holds shapes that could break
// either: a run with cases but no
// suite at all, a suite older than the history window, a failure inside the
// history window, a suite of the next day sharing today's run id, and 30,000
// ancient case rows that only an unbounded scan reads.
func TestTestopsCaseAggregateEqualsLegacyReadAndDoesNotScanHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := startDefaultBranchClickHouse(ctx, t)
	repo, w := seedAncientHistoryFixture(ctx, t, conn)

	var legacy testops.TestMetric
	legacyReads := measureCaseResultsReads(ctx, t, conn, func() { legacy, _ = legacyCaseAggregate(ctx, t, conn, repo.id, w) })
	var bounded testops.TestMetric
	boundedReads := measureCaseResultsReads(ctx, t, conn, func() { bounded = computeDefaultBranchTestMetric(ctx, t, conn, w, repo.id) })

	if !reflect.DeepEqual(legacy, bounded) {
		t.Fatalf("the bounded read moved the record:\nlegacy  %+v\nbounded %+v", legacy, bounded)
	}
	if legacy.TotalCases == 0 || legacy.DistinctCases == 0 || legacy.FailureRecurrence == 0 {
		t.Fatalf("the fixture does not exercise the read (a vacuous equality): %+v", legacy)
	}
	t.Logf("test_case_results rows read: legacy %d, now %d", legacyReads.readRows, boundedReads.readRows)
	// 30,000 ancient rows sit in the table; a read that scans the repo's
	// history reads them all (one granule is 8,192 rows).
	if boundedReads.readRows >= 1000 {
		t.Errorf("the case read touched %d rows with 30,000 ancient rows in the table: it scans history", boundedReads.readRows)
	}
}

// Moving the default-branch exclusion from the case rows into the suite sets
// must not move a result, and the read must not touch the excluded runs' case
// rows. The second half is a pin, not a red cell: on a fixture where the pull
// request run is one contiguous block of rows, the previous form (a NOT IN on
// the case rows) is pruned by the key as well and reads the same few rows. The
// fixture does not interleave run ids the way a production table's granules
// can.
func TestTestopsCaseAggregateExclusionInSuiteSetsEqualsPreviousFormAndSkipsPullRequestRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := startDefaultBranchClickHouse(ctx, t)
	for _, table := range []string{"ci_pipeline_runs", "test_suite_results", "test_case_results", "repos"} {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatal(err)
		}
	}
	w := newDefaultBranchWindow()
	repos := []defaultBranchRepo{
		{id: uuid.MustParse("00000000-0000-4000-8000-0000000000f1"), settings: `{"default_branch":"main"}`, prBranch: "feature/x", recordedInSQL: true},
		{id: uuid.MustParse("00000000-0000-4000-8000-0000000000f2"), settings: `{"default_branch":"main"}`, prBranch: "main", recordedInSQL: true},
		{id: uuid.MustParse("00000000-0000-4000-8000-0000000000f3"), settings: `{"default_branch":"main"}`, prBranch: "", recordedInSQL: true},
		{id: uuid.MustParse("00000000-0000-4000-8000-0000000000f4"), settings: `{"default_branch":"main"}`, prBranch: "feature/x", recordedInSQL: true, prCasesOnly: true},
	}
	for _, repo := range repos {
		seedDefaultBranchRepo(ctx, t, conn, w, repo)
	}
	for _, repo := range repos {
		previous, _ := previousExclusionCaseAggregate(ctx, t, conn, repo.id, w, "main")
		current := computeDefaultBranchTestMetric(ctx, t, conn, w, repo.id)
		current.FlakeUnknown = false // the previous form had no such mark
		if !reflect.DeepEqual(previous, current) {
			t.Errorf("repo %s: the exclusion inside the suite sets moved the record:\nprevious %+v\ncurrent  %+v", repo.id, previous, current)
		}
	}

	// A pull-request run with 30,000 case rows today: the previous form reads
	// them all, the new form skips the run.
	bulk := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000f5"), settings: `{"default_branch":"main"}`, prBranch: "feature/x", recordedInSQL: true}
	seedDefaultBranchRepo(ctx, t, conn, w, bulk)
	synced := w.day.Add(20 * time.Hour)
	started := w.day.Add(12 * time.Hour)
	execAll(ctx, t, conn, `INSERT INTO ci_pipeline_runs
(repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, retry_count, org_id, branch)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [][]any{{bulk.id, "r-pr-bulk", "success", started, started, started.Add(time.Minute), synced, uint32(0), defaultBranchTestOrg, "feature/y"}})
	execAll(ctx, t, conn, `INSERT INTO test_suite_results
(repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count,
 quarantined_count, duration_seconds, started_at, finished_at, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [][]any{{bulk.id, "r-pr-bulk", "s-bulk", "suite", uint32(30000), uint32(30000), uint32(0), uint32(0), uint32(0), uint32(0), 1.0, started, started.Add(time.Minute), defaultBranchTestOrg, synced}})
	if err := conn.Exec(ctx, `INSERT INTO test_case_results
(repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, org_id, last_synced)
SELECT ?, 'r-pr-bulk', 's-bulk', toString(number), concat('bulk-', toString(number)), 'passed', 0, ?, ?
FROM numbers(30000)`, bulk.id, defaultBranchTestOrg, synced); err != nil {
		t.Fatalf("seed pull-request case rows: %v", err)
	}
	var previous, current testops.TestMetric
	previousReads := measureCaseResultsReads(ctx, t, conn, func() {
		previous, _ = previousExclusionCaseAggregate(ctx, t, conn, bulk.id, w, "main")
	})
	currentReads := measureCaseResultsReads(ctx, t, conn, func() { current = computeDefaultBranchTestMetric(ctx, t, conn, w, bulk.id) })
	current.FlakeUnknown = false
	if !reflect.DeepEqual(previous, current) {
		t.Fatalf("the bulk fixture moved the record:\nprevious %+v\ncurrent  %+v", previous, current)
	}
	t.Logf("test_case_results rows read: previous form %d, now %d", previousReads.readRows, currentReads.readRows)
	if currentReads.readRows >= 1000 {
		t.Errorf("the case read touched %d rows with a 30,000-row pull-request run in the window: it reads the excluded run", currentReads.readRows)
	}
}

// A run with suite rows but no ci_pipeline_runs row cannot be shown to be on the
// default branch, so the default-branch measures leave it out (prod had 6 such
// runs in 1,206 when this was decided); the pass and failure counts, which come
// from suite rows, still count it. Coverage already needs the pipeline row (its
// join), so its snapshot drops out the same way.
func TestTestopsRunWithoutPipelineRowIsLeftOutOfDefaultBranchMeasures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startDefaultBranchClickHouse(ctx, t)
	w := newDefaultBranchWindow()

	// r-pr has suite, case and coverage rows but no pipeline row.
	orphan := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000e5"),
		settings: `{"default_branch":"main"}`, prBranch: "feature/x", recordedInSQL: true, noPipelineRow: "r-pr"}
	// The same rows with the pipeline row present on the default branch: C flakes.
	control := defaultBranchRepo{id: uuid.MustParse("00000000-0000-4000-8000-0000000000e6"),
		settings: `{"default_branch":"main"}`, prBranch: "main", recordedInSQL: true}
	seedDefaultBranchRepo(ctx, t, conn, w, orphan)
	seedDefaultBranchRepo(ctx, t, conn, w, control)

	if got := computeDefaultBranchTestMetric(ctx, t, conn, w, control.id); got.FlakeCases != 1 {
		t.Fatalf("control flake_cases=%d, want 1", got.FlakeCases)
	}
	got := computeDefaultBranchTestMetric(ctx, t, conn, w, orphan.id)
	if got.FlakeCases != 0 || got.RetryDependencyRate != 0 || got.DistinctCases != 3 {
		t.Errorf("a run with no pipeline row fed the default-branch measures: flake_cases=%d retry_dependency_rate=%v distinct_cases=%d, want 0 / 0 / 3",
			got.FlakeCases, got.RetryDependencyRate, got.DistinctCases)
	}
	if got.TotalCases != 5 {
		t.Errorf("total_cases=%d, want 5: suite rows of a run with no pipeline row still count in the pass and failure counts", got.TotalCases)
	}
	coverage, err := loadNativeTestopsLatestCoverage(ctx, conn, defaultBranchTestOrg, orphan.id, w.start, w.end)
	if err != nil || len(coverage) != 1 || coverage[0].RunID != "r-main" {
		t.Errorf("coverage=%+v err=%v, want r-main", coverage, err)
	}
}
