package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// -----------------------------------------------------------------------
// CHAOS-4284: ClickHouse readers and writers for the NATIVE
// testops_pipeline / testops_test / testops_coverage families.
//
// # Why these are not the loaders next door
//
// testops_risk_native_clickhouse.go has loadTestopsPipelineRuns /
// loadTestopsSuiteAndCaseRows / loadTestopsCoverageSnapshots, which
// materialise every source row into a slice. They are kept there purely as
// the raw, row-at-a-time reference reader the pushdown-vs-raw differential
// test (testops_native_integration_test.go) compares the readers below
// against -- neither TestopsRiskExecutor nor the testops_pipeline /
// testops_test / testops_coverage families read through them any more. On
// repo 920f9442 a Python loader with this same row-materialising shape hit
// its 200k-row cap on test_case_results and raised TestopsRowCapExceeded (a
// MemoryError subclass), which worker_metrics_runner classified as
// resource_exhausted, freezing the allocation chain downstream -- the
// original motivation for reducing test_case_results inside ClickHouse
// instead of reading it row by row, in both the pushdown and the raw
// readers.
//
// The readers below have NO cap because they never need one:
//
//   - test_case_results, the table that actually choked, is REDUCED INSIDE
//     ClickHouse to one row per case_name (loadNativeTestopsCaseAggregate). The
//     compute only ever asks three questions of a case -- which normalised
//     statuses did it have today, did any attempt today exceed 0, and did it
//     fail in the history window -- so a status set, a max and a flag are
//     lossless. Result cardinality becomes the distinct case count, which is
//     the irreducible size of Python's own case_statuses dict: never worse
//     than Python, with the row explosion gone.
//   - ci_pipeline_runs and test_suite_results STREAM into an accumulator
//     (testops.PipelineAccumulator / testops.TestAccumulator) instead of a
//     slice, so peak memory is O(groups) + two float64 per run, not
//     O(rows x columns). Their cardinality is CI-run scale, not case scale.
//   - coverage_snapshots is reduced to the single latest snapshot per repo
//     in ClickHouse (ORDER BY ... LIMIT 1).
//
// # Dedup: FINAL, read-for-read with Python
//
// Every source table is ReplacingMergeTree(last_synced), so a row is only
// authoritative after dedup. These readers use FINAL, matching Python's own
// reads line for line (loaders/clickhouse.py:1307, 1390, 1442, 1444, 1460,
// 1581, 1594, 1640).
//
// An earlier revision of this file used `argMax(tuple(...), last_synced)
// GROUP BY <sorting key>` instead, on the reasoning that it is
// merge-independent and composes with aggregation. That was REVERTED
// (2026-09-04, fleet ruling measured on a real ClickHouse):
//
//   - argMax is NONDETERMINISTIC on a version-column TIE. Given two rows with
//     the same sorting key and the same last_synced, it may return either.
//   - FINAL is deterministic on the same input: last-inserted wins.
//
// And last_synced CAN tie here. The testops writer batches per projection
// (internal/providersync/github_tests_effects_clickhouse.go:62,91,120,152,
// 185,218) and stamps every row in a pass from ONE normalizedAt, so
// last_synced is BATCH-CONSTANT, not per-row. CHAOS-5045's duplicates come
// from re-projection ACROSS passes and do carry distinct timestamps -- but two
// rows sharing a sorting key WITHIN one pass (a JUnit report containing the
// same case twice, a merged report) share last_synced exactly. Uniqueness is
// not provable from the source, and the rule is that argMax requires provable
// per-key uniqueness. It does not have it, so FINAL it is.
//
// The 200k cap removal does NOT depend on argMax and is unaffected: what
// removes the materialisation is the PUSHDOWN in loadNativeTestopsCaseGroups
// (GROUP BY case_name, returning one row per case name instead of one per
// case ROW), and that reduction works identically on top of FINAL.
//
// # The day-window filter
//
// With FINAL the window predicate sits in the same WHERE clause, exactly as
// Python writes it: FINAL resolves the row first, so the filter always sees
// the winning version. The outside-the-dedup placement the argMax form needed
// is gone with it.
//
// # Ordering
//
// Every query carries an explicit ORDER BY. Python's own loader
// (loaders/clickhouse.py:1286) has none, which makes its avg_queue_seconds
// summation order -- and its `first_suite` team/service pick -- dependent on
// whatever order ClickHouse happened to return. That is a pre-existing Python
// nondeterminism, recorded rather than fixed; a deterministic order here is
// what makes the differential test's bit-exact comparison meaningful instead
// of accidentally passing.
// -----------------------------------------------------------------------

// testopsNativeConn is the narrow read capability these loaders need.
// Deliberately the same shape as testopsRiskConn so a caller can pass one
// driver.Conn to both.
type testopsNativeConn interface {
	Query(context.Context, string, ...any) (driver.Rows, error)
}

// testopsNativeBatchConn is the narrow write capability the three writers
// need, mirroring testopsRiskBatchConn.
type testopsNativeBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// -----------------------------------------------------------------------
// Readers
// -----------------------------------------------------------------------

// testopsPipelineRunsSource and testopsSuiteResultsSource are the testops
// families' source reads: the current version (FINAL) of each pipeline run
// started in [start, end), and of each test suite whose start (or, without
// one, finish) falls in it. The zero-row source check reads the same text.
const (
	testopsPipelineRunsSource = `
FROM ci_pipeline_runs FINAL
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
  AND started_at >= {start:DateTime64(3, 'UTC')} AND started_at < {end:DateTime64(3, 'UTC')}
  AND lowerUTF8(trimBoth(ifNull(status, ''))) != '` + testops.StatusDeleted + `'`
)

// testopsSuiteResultsSource is the suite rows' source read, rendered by testopspooled (one text, also the window bound of the
// newest-attempt fragment).
var testopsSuiteResultsSource = testopspooled.SuiteWindowSource("repo_ids", "start", "end")

// testopsKeptSuitesToday is the day's kept suite rows of the repository list: the window source and the newest-attempt fragment,
// the suite read's own rule (one text). The case reads take a run as "in today" only when it has such a row (CHAOS-13926): a run
// whose counted suite moved to a later day by a re-run then leaves no case on this day, so the day gets no row instead of 0.
var testopsKeptSuitesToday = testopsSuiteResultsSource + "\n  AND " + testopspooled.SuiteRowsNewestAttempt("", testopsRuleScope)

// testopsCaseSuitesNotRetired and testopsHistorySuitesNotRetired keep the case reads' own-suite sets off a retired report slot
// and its retraction rows (CHAOS-14259), at every attempt: a slot whose rows a later sync retracted holds no case of the run. The
// today set is bound like the kept suites (a case counts only in a run of today's kept suites); the history set by its own
// window [history_start, start), the window of the suites it tests.
var (
	testopsCaseSuitesNotRetired    = testopspooled.SuiteRowsNotRetired("", testopsRuleScope)
	testopsHistorySuitesNotRetired = testopspooled.SuiteRowsNotRetired("", testopspooled.Scope{RepoIDsParam: "repo_ids", WindowStartParam: "history_start", WindowEndParam: "start"})
)

// testopsFailureStatuses is FAILURE_STATUSES (compute_testops.py:54), bound as {failure_statuses:Array(String)}.
var testopsFailureStatuses = []string{"failure", "failed", "error", "errors", "timeout", "timed_out"}

// testopsCaseArgs binds the case reads: the source parameters of one repository, the history start and, when the off-default
// set is used, the default branch.
func testopsCaseArgs(orgID string, repoID uuid.UUID, defaultBranch string, historyStart, start, end time.Time) []any {
	args := append(testopsSourceArgs(orgID, []uuid.UUID{repoID}, start, end),
		clickhouse.Named("history_start", remaining.DateTime64Argument(historyStart, remaining.DateTime64MillisecondPrecision)))
	if defaultBranch != "" {
		args = append(args, clickhouse.Named("default_branch", defaultBranch))
	}
	return args
}

// testopsHistoryNameArgs binds the reference history read (loadHistoricalFailedCaseNames): the source parameters of its own
// window [start, end), the end of the current day and the failure vocabulary.
func testopsHistoryNameArgs(orgID string, repoID uuid.UUID, start, end, currentDayEnd time.Time) []any {
	return append(testopsSourceArgs(orgID, []uuid.UUID{repoID}, start, end),
		clickhouse.Named("current_day_end", remaining.DateTime64Argument(currentDayEnd, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("failure_statuses", testopsFailureStatuses))
}

func testopsSourceArgs(orgID string, repoIDs []uuid.UUID, start, end time.Time) []any {
	return []any{
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("start", remaining.DateTime64Argument(start, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("end", remaining.DateTime64Argument(end, remaining.DateTime64MillisecondPrecision)),
	}
}

// loadNativeTestopsPipelineRuns streams the deduped ci_pipeline_runs rows for
// one (org, repo, day) straight into a testops.PipelineAccumulator. It never
// builds a slice of rows, and it has no row cap.
//
// Ports load_testops_pipeline_data (loaders/clickhouse.py:1286) minus the
// ci_job_runs half: compute_pipeline_metrics_daily takes job_runs only to
// `del` it on its first line (compute_testops.py:123), so loading that table
// would be pure cost.
func loadNativeTestopsPipelineRuns(
	ctx context.Context,
	conn testopsNativeConn,
	accumulator *testops.PipelineAccumulator,
	orgID string,
	repoID uuid.UUID,
	start, end time.Time,
) error {
	rows, err := conn.Query(ctx, `
SELECT status, queued_at, started_at, finished_at, duration_seconds, queue_seconds,
       retry_count, team_id, service_id, org_id, run_id, last_synced`+testopsPipelineRunsSource+`
ORDER BY run_id`,
		testopsSourceArgs(orgID, []uuid.UUID{repoID}, start, end)...)
	if err != nil {
		return fmt.Errorf("load native testops pipeline runs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		row := testops.PipelineRunRow{RepoID: repoID}
		var runID string
		if err := rows.Scan(
			&row.Status, &row.QueuedAt, &row.StartedAt, &row.FinishedAt,
			&row.DurationSeconds, &row.QueueSeconds, &row.RetryCount,
			&row.TeamID, &row.ServiceID, &row.OrgID, &runID, &row.LastSynced,
		); err != nil {
			return fmt.Errorf("scan native testops pipeline run: %w", err)
		}
		accumulator.Add(row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate native testops pipeline runs: %w", err)
	}
	return nil
}

// loadNativeTestopsSuites streams the deduped test_suite_results rows for one
// (org, repo, day) into a testops.TestAccumulator, ordered by (run_id,
// suite_id).
//
// The order is load-bearing beyond determinism-for-its-own-sake: Python takes
// team_id/service_id/org_id from `repo_suites[0]` (compute_testops.py:327,
// `first_suite`), so which suite lands first decides those three output
// columns whenever a repo's suites disagree. Python leaves that to an
// unordered query; this reader pins it.
// testopsRuleScope bounds the inner selects of the newest-attempt fragments (testopspooled) to the readers' repository list and
// window; the parameters are the ones testopsSourceArgs binds.
var testopsRuleScope = testopspooled.Scope{RepoIDsParam: "repo_ids", WindowStartParam: "start", WindowEndParam: "end"}

func loadNativeTestopsSuites(
	ctx context.Context,
	conn testopsNativeConn,
	accumulator *testops.TestAccumulator,
	orgID string,
	repoID uuid.UUID,
	start, end time.Time,
) error {
	rows, err := conn.Query(ctx, `
SELECT run_id, suite_id, total_count, passed_count, failed_count, skipped_count,
       error_count, quarantined_count, duration_seconds, started_at, finished_at,
       team_id, service_id, org_id`+testopsKeptSuitesToday+`
ORDER BY run_id, suite_id`,
		testopsSourceArgs(orgID, []uuid.UUID{repoID}, start, end)...)
	if err != nil {
		return fmt.Errorf("load native testops suite results: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		row := testops.SuiteRow{RepoID: repoID}
		if err := rows.Scan(
			&row.RunID, &row.SuiteID, &row.TotalCount, &row.PassedCount,
			&row.FailedCount, &row.SkippedCount, &row.ErrorCount, &row.QuarantinedCount,
			&row.DurationSeconds, &row.StartedAt, &row.FinishedAt,
			&row.TeamID, &row.ServiceID, &row.OrgID,
		); err != nil {
			return fmt.Errorf("scan native testops suite result: %w", err)
		}
		accumulator.AddSuite(row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate native testops suite results: %w", err)
	}
	return nil
}

// loadNativeTestopsCaseAggregate is the one test_case_results read behind a
// test metric record, reduced inside ClickHouse to one row per case_name. The
// table's sorting key has no time column, but the semi-joins below are on
// run_id, the third key column after org_id and repo_id, so ClickHouse reads
// only the runs they name (the read does not grow with the repo's case
// history); both questions the record asks of it are answered in the same scan. It folds today's cases into accumulator and
// returns the names that failed in [historyStart, start).
//
// Today's half reproduces load_testops_test_data's two-part semi-join
// (loaders/clickhouse.py:1344): a case counts when (a) its run has SOME suite
// in [start, end) and (b) its OWN suite starts before `end` -- (b) is the
// day-boundary guard that stops a suite from a later day being folded into
// today just because it shares a run_id. Status strings come back RAW.
// Normalising them here would mean writing Python's `str.strip().lower()` in
// SQL, and the two do not agree: str.strip() removes unicode whitespace,
// ClickHouse's trim and RE2's \s do not. normalizeTestStatus in the testops
// package does it instead, on exactly the strings Python would have seen.
// coalesce(status, ”) keeps the NULL case explicit: Python maps a NULL status
// to None to "" through _normalize_test_status, and groupUniqArray would
// otherwise drop NULLs from the array entirely.
//
// The history half ports load_testops_historical_failed_case_names
// (loaders/clickhouse.py:1498): distinct names that failed in [historyStart,
// start), EXCLUDING any run that also has a suite in [start, end) so a run
// straddling the day boundary is not counted as both today and history. That
// query joined each case to its suite on the suite key and kept DISTINCT
// names, so the join only ever tested that a matching suite existed; the
// (run_id, suite_id) semi-join below tests the same thing. lower(trim(status))
// IS evaluated in SQL there, unlike today's half, because Python's own query
// normalises that way, so matching it is fidelity rather than divergence.
// FAILURE_STATUSES (compute_testops.py:54) is the vocabulary, passed as
// parameters rather than interpolated.
//
// A name seen only in history comes back with seen_today = 0 and never
// reaches the accumulator: counting it would make a repo with no suites and
// no cases today emit a record.
func loadNativeTestopsCaseAggregate(
	ctx context.Context,
	conn testopsNativeConn,
	accumulator *testops.TestAccumulator,
	orgID string,
	repoID uuid.UUID,
	historyStart, start, end time.Time,
) (map[string]struct{}, error) {
	defaultBranch, err := loadNativeTestopsDefaultBranch(ctx, conn, orgID, repoID)
	if err != nil {
		return nil, err
	}
	// Runs not shown to sit on the repository default branch are left out of
	// this read: flake, retry-dependency and failure-recurrence compare a
	// case name's statuses across runs, and a pull-request branch that fails,
	// is fixed and then passes on a new commit would read as a flake. Pass and
	// failure counts come from suite rows (loadNativeTestopsSuites) and are not
	// narrowed here.
	//
	// The exclusion sits INSIDE the two suite sets that name the runs a case row
	// can belong to (today's runs, history runs), not on the case rows: a case
	// row of an excluded run is then never in either set, and the run_id IN
	// semi-joins keep ClickHouse from reading it at all, instead of reading it
	// and dropping it. (The third set, suites before `end`, is only ever ANDed
	// with the today set, so it needs no exclusion of its own.)
	//
	// A run with suite rows but no ci_pipeline_runs row is left out too: its
	// branch cannot be shown to be the default. A run whose recorded branch is
	// NULL or empty stays in, and so does every run when the repository's
	// default branch is not recorded. The off-default set is limited to the
	// runs with a suite in [historyStart, end), the only runs the sets can name.
	//
	// Today's run set takes the kept suites (testopsKeptSuitesToday), and so does the history arm through NOT run_in_today. The
	// own-suite tests (in_today, and the history arm's suite set) keep the suite rows of every attempt, except a retired slot's
	// (testopsCaseSuitesNotRetired, CHAOS-14259): a re-run's attempt-1 case rows still join
	// on the newest attempt's day, so a test that failed on attempt 1 and passed on attempt 2 stays a flake there, and an
	// attempt-1 failure stays history for a later day.
	offDefaultCTE := ""
	offDefaultExclusion := ""
	if defaultBranch != "" {
		offDefaultCTE = offDefaultRunsCTE
		offDefaultExclusion = "\n        AND run_id NOT IN (SELECT run_id FROM off_default)"
	}
	args := append(testopsCaseArgs(orgID, repoID, defaultBranch, historyStart, start, end),
		clickhouse.Named("failure_statuses", testopsFailureStatuses))
	rows, err := conn.Query(ctx, offDefaultCTE+`
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
      SELECT run_id`+testopsKeptSuitesToday+offDefaultExclusion+`
    ) AS run_in_today,
    run_in_today AND (run_id, suite_id) IN (
      SELECT run_id, suite_id FROM test_suite_results FINAL
      WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
        AND coalesce(started_at, finished_at) < {end:DateTime64(3, 'UTC')}
        AND `+testopsCaseSuitesNotRetired+`
    ) AS in_today,
    NOT run_in_today
      AND lower(trim(status)) IN {failure_statuses:Array(String)}
      AND (run_id, suite_id) IN (
        SELECT run_id, suite_id FROM test_suite_results FINAL
        WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
          AND coalesce(started_at, finished_at) >= {history_start:DateTime64(3, 'UTC')}
          AND coalesce(started_at, finished_at) < {start:DateTime64(3, 'UTC')}`+offDefaultExclusion+`
          AND `+testopsHistorySuitesNotRetired+`
      ) AS failed_in_history
  FROM test_case_results FINAL
  WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND case_name != ''
)
WHERE in_today OR failed_in_history
GROUP BY case_name
ORDER BY case_name`, args...)
	if err != nil {
		return nil, fmt.Errorf("load native testops case aggregate: %w", err)
	}
	defer rows.Close()

	historicalFailedNames := make(map[string]struct{})
	sawCaseToday := false
	for rows.Next() {
		var group testops.CaseGroup
		var seenToday, failedBefore uint8
		if err := rows.Scan(&group.CaseName, &group.Statuses, &group.MaxRetry, &seenToday, &failedBefore); err != nil {
			return nil, fmt.Errorf("scan native testops case aggregate: %w", err)
		}
		if seenToday != 0 {
			accumulator.AddCaseGroup(group)
			sawCaseToday = true
		}
		if failedBefore != 0 {
			historicalFailedNames[group.CaseName] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate native testops case aggregate: %w", err)
	}
	if !sawCaseToday && defaultBranch != "" {
		// No default-branch case row today. When the day does hold case rows of
		// runs left out above, the flake measures are not "0 flakes" but unknown:
		// say so, instead of letting a repo-day that only ran pull requests read
		// as healthy.
		offDefaultCases, err := hasNativeTestopsOffDefaultCaseToday(
			ctx, conn, orgID, repoID, defaultBranch, historyStart, start, end)
		if err != nil {
			return nil, err
		}
		if offDefaultCases {
			accumulator.MarkCasesOffDefaultBranchOnly()
		}
	}
	return historicalFailedNames, nil
}

// offDefaultRunsCTE names, as off_default, the runs with a suite in
// [historyStart, end) that are not shown to be on the repository default
// branch: a run counts as on it only when ci_pipeline_runs holds a row for it
// whose branch is the default, NULL or empty. A run with suite rows but NO
// pipeline row is therefore off_default: its branch cannot be proven, and once
// pull-request reports sync it would leak into the default-branch measures.
// A NULL or empty branch is a recorded absence and stays on the default side.
const offDefaultRunsCTE = `
WITH window_runs AS (
  SELECT DISTINCT run_id FROM test_suite_results FINAL
  WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
    AND coalesce(started_at, finished_at) >= {history_start:DateTime64(3, 'UTC')}
    AND coalesce(started_at, finished_at) < {end:DateTime64(3, 'UTC')}
), off_default AS (
  SELECT run_id FROM window_runs
  WHERE run_id NOT IN (
    SELECT run_id FROM ci_pipeline_runs FINAL
    WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
      AND run_id IN (SELECT run_id FROM window_runs)
      AND (branch IS NULL OR branch = '' OR branch = {default_branch:String})
  )
)`

// hasNativeTestopsOffDefaultCaseToday reports whether today holds a case row of
// a run not shown to be on the default branch, by the same day rule the case
// aggregate uses for today's rows.
func hasNativeTestopsOffDefaultCaseToday(
	ctx context.Context, conn testopsNativeConn, orgID string, repoID uuid.UUID,
	defaultBranch string, historyStart, start, end time.Time,
) (bool, error) {
	rows, err := conn.Query(ctx, offDefaultRunsCTE+`
SELECT count() FROM (
  SELECT 1 FROM test_case_results FINAL
  WHERE case_name != ''
    AND run_id IN (SELECT run_id FROM off_default)
    AND run_id IN (
      SELECT run_id`+testopsKeptSuitesToday+`
    )
    AND (run_id, suite_id) IN (
      SELECT run_id, suite_id FROM test_suite_results FINAL
      WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
        AND coalesce(started_at, finished_at) < {end:DateTime64(3, 'UTC')}
        AND `+testopsCaseSuitesNotRetired+`
    )
    AND org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
  LIMIT 1
)`, testopsCaseArgs(orgID, repoID, defaultBranch, historyStart, start, end)...)
	if err != nil {
		return false, fmt.Errorf("load native testops off-default case rows: %w", err)
	}
	defer rows.Close()
	var count uint64
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return false, fmt.Errorf("scan native testops off-default case rows: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate native testops off-default case rows: %w", err)
	}
	return count > 0, nil
}

// loadNativeTestopsLatestCoverage returns AT MOST ONE snapshot: the latest by
// lexical (run_id, snapshot_id) among coverage snapshots whose pipeline run
// started in [start, end). Ports load_testops_coverage_data
// (loaders/clickhouse.py:1665) plus compute_coverage_metrics_daily's own
// `_latest_snapshot_key` reduction (compute_testops.py:95), which Python does
// in Python over every loaded row.
//
// ORDER BY ... DESC LIMIT 1 rather than argMax: the payload here is mostly
// Nullable, and while the tuple trick above defuses argMax's NULL-skipping,
// picking the single winning ROW is simpler to read and obviously correct at
// this cardinality (coverage snapshots per repo/day are a handful).
//
// Python's tie-break is a strict `>`, i.e. first-seen wins on an exact key
// tie. No tie is reachable: (org_id, repo_id, run_id, snapshot_id) is
// coverage_snapshots' own ORDER BY key (migration 042), so after dedup exactly
// one row carries any given (run_id, snapshot_id).
func loadNativeTestopsLatestCoverage(
	ctx context.Context, conn testopsNativeConn, orgID string, repoID uuid.UUID, start, end time.Time,
) ([]testops.CoverageSnapshotRow, error) {
	defaultBranch, err := loadNativeTestopsDefaultBranch(ctx, conn, orgID, repoID)
	if err != nil {
		return nil, err
	}
	// A pull-request run's coverage must never become the repository's latest
	// snapshot: the ORDER BY below knows no branch, so a PR run whose id sorts
	// after the default branch's would win. Only runs whose ci_pipeline_runs row
	// records the default branch, or no branch, are kept; the inner join already
	// drops a run with no pipeline row (see loadNativeTestopsCaseAggregate for
	// the other unknown cases). ci_pipeline_runs carries every branch's runs, so
	// the filter does not depend on which runs the test sync fetches reports for.
	onDefaultBranch := ""
	args := testopsSourceArgs(orgID, []uuid.UUID{repoID}, start, end)
	if defaultBranch != "" {
		onDefaultBranch = "\n  AND (p.branch IS NULL OR p.branch = '' OR p.branch = {default_branch:String})"
		args = append(args, clickhouse.Named("default_branch", defaultBranch))
	}
	rows, err := conn.Query(ctx, `
SELECT c.run_id, c.snapshot_id, c.lines_total, c.lines_covered,
       c.line_coverage_pct, c.branch_coverage_pct, c.branches_total, c.branches_covered, c.team_id, c.service_id, c.org_id
FROM coverage_snapshots AS c FINAL
INNER JOIN ci_pipeline_runs AS p FINAL
  ON (p.repo_id = c.repo_id) AND (p.run_id = c.run_id) AND (p.org_id = c.org_id)
WHERE p.started_at >= {start:DateTime64(3, 'UTC')} AND p.started_at < {end:DateTime64(3, 'UTC')}
  AND p.repo_id IN {repo_ids:Array(UUID)} AND p.org_id = {org_id:String}
  AND `+testopspooled.CoverageRowsNewestAttempt("c", testopsRuleScope)+onDefaultBranch+`
ORDER BY c.run_id DESC, c.snapshot_id DESC
LIMIT 1`, args...)
	if err != nil {
		return nil, fmt.Errorf("load native testops coverage snapshot: %w", err)
	}
	defer rows.Close()

	var result []testops.CoverageSnapshotRow
	for rows.Next() {
		row := testops.CoverageSnapshotRow{RepoID: repoID}
		if err := rows.Scan(
			&row.RunID, &row.SnapshotID, &row.LinesTotal, &row.LinesCovered,
			&row.LineCoveragePct, &row.BranchCoveragePct, &row.BranchesTotal, &row.BranchesCovered, &row.TeamID, &row.ServiceID, &row.OrgID,
		); err != nil {
			return nil, fmt.Errorf("scan native testops coverage snapshot: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate native testops coverage snapshot: %w", err)
	}
	return result, nil
}

// -----------------------------------------------------------------------
// Writers
//
// All three target tables are ReplacingMergeTree(computed_at) ORDER BY
// (org_id, repo_id, day) (migration 096). A merge keeps only the newest
// computed_at per key, but two writes from the SAME run share that computed_at
// and one of them survives arbitrarily, so each partition must still write
// each row exactly once.
//
// Column lists mirror sinks/clickhouse/ci.py's _insert_rows calls exactly,
// including org_id being supplied by the caller's run scope rather than the
// record, matching writeTestopsRisk.
// -----------------------------------------------------------------------

func writeTestopsPipelineMetrics(
	ctx context.Context, conn testopsNativeBatchConn, organizationID string,
	day time.Time, computedAt time.Time, metrics []testops.PipelineMetric,
) (int, error) {
	if len(metrics) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO testops_pipeline_metrics_daily (
		repo_id, day, pipelines_count, success_count, failure_count, cancelled_count,
		success_rate, failure_rate, cancel_rate, rerun_rate,
		median_duration_seconds, p95_duration_seconds, avg_queue_seconds, p95_queue_seconds,
		team_id, service_id, org_id, computed_at,
		skipped_count, unfinished_count, other_count, unfinished_stale_count,
		pooled_service_ids, pooled_durations, pooled_queues, pooled_samples_stored)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_pipeline_metrics_daily batch: %w", err)
	}
	for _, metric := range metrics {
		samples := metric.PooledSamples
		fit := pooledSamplesFit(ctx, "testops_pipeline", organizationID, metric.RepoID.String(), day, len(samples))
		if !fit {
			samples = nil
		}
		services, durations, queues := pooledSampleColumns(samples)
		if err := batch.Append(
			metric.RepoID, chDate(day),
			// pipelines_count/success_count/failure_count/cancelled_count
			// are per-(repo, team, service, day) tallies incremented once
			// per pipeline run with no LIMIT -- bounded by a day's pipeline
			// volume, never negative -- so they keep a direct conversion.
			uint32(metric.PipelinesCount), uint32(metric.SuccessCount),
			uint32(metric.FailureCount), uint32(metric.CancelledCount),
			metric.SuccessRate, metric.FailureRate, metric.CancelRate, metric.RerunRate,
			metric.MedianDurationSeconds, metric.P95DurationSeconds,
			metric.AvgQueueSeconds, metric.P95QueueSeconds,
			metric.TeamID, metric.ServiceID, organizationID, computedAt.UTC(),
			uint32PtrOf(metric.SkippedCount), uint32PtrOf(metric.UnfinishedCount),
			uint32PtrOf(metric.OtherCount), uint32PtrOf(metric.UnfinishedStaleCount),
			services, durations, queues, pooledMarker(fit),
		); err != nil {
			return 0, fmt.Errorf("append testops_pipeline_metrics_daily row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(metrics), fmt.Errorf("send testops_pipeline_metrics_daily batch: %w", err)
	}
	return len(metrics), nil
}

func uint32PtrOf(n int) *uint32 {
	v := uint32(n)
	return &v
}

// measuredCaseRates returns flake_rate, retry_dependency_rate and
// failure_recurrence_score as written: NULL (nil) for a day that measured no
// case (FlakeUnknown, or no case row at all), because a rate over 0 cases is
// unknown, not 0. A day that measured cases writes its rates, a 0 included.
func measuredCaseRates(metric testops.TestMetric) (flakeRate, retryDependencyRate, failureRecurrence *float64) {
	if metric.DistinctCases == 0 {
		return nil, nil, nil
	}
	return &metric.FlakeRate, &metric.RetryDependencyRate, &metric.FailureRecurrence
}

func writeTestopsTestMetrics(
	ctx context.Context, conn testopsNativeBatchConn, organizationID string,
	day time.Time, computedAt time.Time, metrics []testops.TestMetric,
) (int, error) {
	if len(metrics) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO testops_test_metrics_daily (
		repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count,
		pass_rate, failure_rate, flake_rate, retry_dependency_rate, total_suites,
		suite_duration_p50_seconds, suite_duration_p95_seconds, failure_recurrence_score,
		team_id, service_id, org_id, computed_at, flake_cases, distinct_cases,
		pooled_service_ids, pooled_durations, pooled_samples_stored,
		report_runs, partial_report_runs, unmarked_report_runs,
		recurrent_failed_names, distinct_failed_names)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_test_metrics_daily batch: %w", err)
	}
	for _, metric := range metrics {
		// A flake-unknown day carries 0 flake cases of 0 distinct cases: it adds
		// nothing to the org series' sums (queryapi/analytics/timeseries.go), so the
		// org value is the other repos' and is unavailable only when every repo's
		// day is unknown. A NULL pair would blank the whole org day instead.
		flakeCases, distinctCases := uint32PtrOf(metric.FlakeCases), uint32PtrOf(metric.DistinctCases)
		flakeRate, retryDependencyRate, failureRecurrence := measuredCaseRates(metric)
		samples := metric.PooledSamples
		fit := pooledSamplesFit(ctx, "testops_test", organizationID, metric.RepoID.String(), day, len(samples))
		if !fit {
			samples = nil
		}
		services, durations, _ := pooledSampleColumns(samples)
		var reportRuns, partialReportRuns, unmarkedReportRuns *uint32
		if counts := metric.ReportRuns; counts != nil {
			reportRuns, partialReportRuns, unmarkedReportRuns = uint32PtrOf(counts.Runs), uint32PtrOf(counts.Partial), uint32PtrOf(counts.Unmarked)
		}
		if err := batch.Append(
			metric.RepoID, chDate(day),
			// total_cases/passed_count/failed_count/skipped_count/
			// quarantined_count/total_suites are per-(repo, team, service,
			// day) tallies incremented once per test case/suite run with no
			// LIMIT -- bounded by a day's test volume, never negative -- so
			// they keep a direct conversion.
			uint32(metric.TotalCases), uint32(metric.PassedCount), uint32(metric.FailedCount),
			uint32(metric.SkippedCount), uint32(metric.QuarantinedCount),
			metric.PassRate, metric.FailureRate, flakeRate, retryDependencyRate,
			uint32(metric.TotalSuites),
			metric.SuiteDurationP50Seconds, metric.SuiteDurationP95Seconds, failureRecurrence,
			metric.TeamID, metric.ServiceID, organizationID, computedAt.UTC(),
			// flake_cases/distinct_cases are Nullable(UInt32): a row written before
			// the columns existed reads NULL, a row written now carries both counts
			// (CHAOS-9737), so the org series can sum them.
			flakeCases, distinctCases,
			services, durations, pooledMarker(fit),
			// report_runs/partial_report_runs/unmarked_report_runs: NULL only for a record whose completeness was not read.
			reportRuns, partialReportRuns, unmarkedReportRuns,
			// The two counts of failure_recurrence_score, on every row (0/0 on a day with no failed name): a report
			// window pools them. A row written before the columns existed reads NULL and is in neither sum.
			uint32PtrOf(metric.RecurrentFailedNames), uint32PtrOf(metric.DistinctFailedNames),
		); err != nil {
			return 0, fmt.Errorf("append testops_test_metrics_daily row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(metrics), fmt.Errorf("send testops_test_metrics_daily batch: %w", err)
	}
	return len(metrics), nil
}

func writeTestopsCoverageMetrics(
	ctx context.Context, conn testopsNativeBatchConn, organizationID string,
	day time.Time, computedAt time.Time, metrics []testops.CoverageMetric,
) (int, error) {
	if len(metrics) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO testops_coverage_metrics_daily (
		repo_id, day, line_coverage_pct, branch_coverage_pct, lines_total, lines_covered,
		coverage_delta_pct, uncovered_files_count, coverage_regression_count,
		team_id, service_id, org_id, computed_at, branches_total, branches_covered)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_coverage_metrics_daily batch: %w", err)
	}
	for _, metric := range metrics {
		if err := batch.Append(
			metric.RepoID, chDate(day),
			metric.LineCoveragePct, metric.BranchCoveragePct,
			metric.LinesTotal, metric.LinesCovered, metric.CoverageDeltaPct,
			// uncovered_files_count/coverage_regression_count are always 0
			// (CoverageMetric's own doc comment: v1 has no per-file coverage
			// input) -- a constant conversion, never negative or unbounded.
			uint32(metric.UncoveredFilesCount), uint32(metric.CoverageRegressionCount),
			metric.TeamID, metric.ServiceID, organizationID, computedAt.UTC(),
			metric.BranchesTotal, metric.BranchesCovered,
		); err != nil {
			return 0, fmt.Errorf("append testops_coverage_metrics_daily row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(metrics), fmt.Errorf("send testops_coverage_metrics_daily batch: %w", err)
	}
	return len(metrics), nil
}

// loadNativeTestopsDefaultBranch returns the repository's default branch as the
// repository sync recorded it in repos.settings (GitHub and GitLab both write
// the key default_branch, and both coerce a missing value to "main" before
// writing). "" means no repos row or no recorded value: the caller then cannot
// tell a default-branch run from another one and must not drop any run.
func loadNativeTestopsDefaultBranch(
	ctx context.Context, conn testopsNativeConn, orgID string, repoID uuid.UUID,
) (string, error) {
	rows, err := conn.Query(ctx, `
SELECT coalesce(JSONExtractString(argMax(settings, last_synced), 'default_branch'), '')
FROM repos
WHERE org_id = ? AND id = ?`, orgID, repoID)
	if err != nil {
		return "", fmt.Errorf("load native testops default branch: %w", err)
	}
	defer rows.Close()
	var branch string
	for rows.Next() {
		if err := rows.Scan(&branch); err != nil {
			return "", fmt.Errorf("scan native testops default branch: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate native testops default branch: %w", err)
	}
	return branch, nil
}
