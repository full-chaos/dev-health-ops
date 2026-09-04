package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// -----------------------------------------------------------------------
// CHAOS-4284: ClickHouse readers and writers for the NATIVE
// testops_pipeline / testops_test / testops_coverage families.
//
// # Why these are not the loaders next door
//
// testops_risk_native_clickhouse.go already has loadTestopsPipelineRuns /
// loadTestopsSuiteAndCaseRows / loadTestopsCoverageSnapshots, and CHAOS-4294
// wrote them for its own in-process recompute. They are deliberately NOT
// reused here, for one reason: they materialise every source row and bound
// that materialisation with a 200k hard cap
// (DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS, errTestopsRowCapExceeded), which is a
// faithful port of the Python loader -- including the exact failure this
// ticket exists to remove. On repo 920f9442 the Python side hit that cap on
// test_case_results and raised TestopsRowCapExceeded (a MemoryError
// subclass), which worker_metrics_runner classifies as resource_exhausted,
// freezing the allocation chain downstream.
//
// The readers below have NO cap because they never need one:
//
//   - test_case_results, the table that actually choked, is REDUCED INSIDE
//     ClickHouse to one row per case_name (loadNativeTestopsCaseGroups). The
//     compute only ever asks two questions of a case -- which normalised
//     statuses did it have, and did any attempt exceed 0 -- so a status set
//     plus a max is lossless. Result cardinality becomes the distinct case
//     count, which is the irreducible size of Python's own case_statuses
//     dict: never worse than Python, with the row explosion gone.
//   - ci_pipeline_runs and test_suite_results STREAM into an accumulator
//     (testops.PipelineAccumulator / testops.TestAccumulator) instead of a
//     slice, so peak memory is O(groups) + two float64 per run, not
//     O(rows x columns). Their cardinality is CI-run scale, not case scale.
//   - coverage_snapshots is reduced to the single latest snapshot per repo
//     in ClickHouse (ORDER BY ... LIMIT 1).
//
// # Dedup: argMax over a TUPLE, never FINAL under aggregation
//
// Every source table is ReplacingMergeTree(last_synced), so a row is only
// authoritative after dedup. These readers dedup with
// `argMax((cols...), last_synced) GROUP BY <the table's exact ORDER BY key>`
// rather than FINAL, for two reasons:
//
//  1. It is merge-independent. CHAOS-5045 found GitHub's TestOps ARTIFACTS
//     phase re-projecting the same test_case_results rows on every hourly
//     unit (raw/uniq 1.86 for repo 920f9442, and up to 5x). Those duplicates
//     are only collapsed by FINAL at query time or by a background merge
//     that may not have run; an argMax over the sorting key is correct
//     either way, and correct DURING the window where lane-5045's fix has
//     not yet been deployed.
//  2. It composes with aggregation. FINAL inside a subquery that is then
//     grouped is both slower and easy to get subtly wrong.
//
// The argMax argument is a TUPLE, not a bare column, on purpose: ClickHouse's
// argMax SKIPS rows whose arg is NULL, so `argMax(status, last_synced)` over
// a Nullable(String) can return an OLDER non-null status when the newest row
// legitimately has status NULL -- silently un-doing the dedup. A tuple value
// is never itself NULL (only its elements are), so every row participates and
// the winner is genuinely the max-last_synced row. tupleElement then unpacks
// it.
//
// # The day-window filter sits OUTSIDE the dedup
//
// Python filters the FINAL (i.e. already-deduped) row's started_at. Applying
// the window before the GROUP BY would let a stale duplicate's timestamp
// decide whether the run is in scope. Every reader here therefore filters on
// the primary-key prefix (org_id, repo_id) inside, and applies the time
// window to the deduped result outside.
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
SELECT
  tupleElement(winner, 1) AS status,
  tupleElement(winner, 2) AS queued_at,
  tupleElement(winner, 3) AS started_at,
  tupleElement(winner, 4) AS finished_at,
  tupleElement(winner, 5) AS duration_seconds,
  tupleElement(winner, 6) AS queue_seconds,
  tupleElement(winner, 7) AS retry_count,
  tupleElement(winner, 8) AS team_id,
  tupleElement(winner, 9) AS service_id,
  org_id,
  run_id
FROM (
  SELECT
    org_id,
    run_id,
    argMax(
      (status, queued_at, started_at, finished_at, duration_seconds,
       queue_seconds, retry_count, team_id, service_id),
      last_synced
    ) AS winner
  FROM ci_pipeline_runs
  WHERE org_id = ? AND repo_id = ?
  GROUP BY org_id, repo_id, run_id
)
WHERE tupleElement(winner, 3) >= ? AND tupleElement(winner, 3) < ?
ORDER BY run_id`,
		orgID, repoID, start.UTC(), end.UTC())
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
			&row.TeamID, &row.ServiceID, &row.OrgID, &runID,
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
func loadNativeTestopsSuites(
	ctx context.Context,
	conn testopsNativeConn,
	accumulator *testops.TestAccumulator,
	orgID string,
	repoID uuid.UUID,
	start, end time.Time,
) error {
	rows, err := conn.Query(ctx, `
SELECT
  run_id,
  suite_id,
  tupleElement(winner, 1) AS total_count,
  tupleElement(winner, 2) AS passed_count,
  tupleElement(winner, 3) AS failed_count,
  tupleElement(winner, 4) AS skipped_count,
  tupleElement(winner, 5) AS error_count,
  tupleElement(winner, 6) AS quarantined_count,
  tupleElement(winner, 7) AS duration_seconds,
  tupleElement(winner, 8) AS started_at,
  tupleElement(winner, 9) AS finished_at,
  tupleElement(winner, 10) AS team_id,
  tupleElement(winner, 11) AS service_id,
  org_id
FROM (
  SELECT
    org_id,
    run_id,
    suite_id,
    argMax(
      (total_count, passed_count, failed_count, skipped_count, error_count,
       quarantined_count, duration_seconds, started_at, finished_at,
       team_id, service_id),
      last_synced
    ) AS winner
  FROM test_suite_results
  WHERE org_id = ? AND repo_id = ?
  GROUP BY org_id, repo_id, run_id, suite_id
)
WHERE coalesce(tupleElement(winner, 8), tupleElement(winner, 9)) >= ?
  AND coalesce(tupleElement(winner, 8), tupleElement(winner, 9)) < ?
ORDER BY run_id, suite_id`,
		orgID, repoID, start.UTC(), end.UTC())
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

// loadNativeTestopsCaseGroups is the cap-removing read: test_case_results
// reduced to ONE row per case_name inside ClickHouse.
//
// It reproduces load_testops_test_data's two-part semi-join exactly
// (loaders/clickhouse.py:1344): a case counts when (a) its run has SOME suite
// in [start, end) and (b) its OWN suite starts before `end` -- (b) is the
// day-boundary guard that stops a suite from a later day being folded into
// today just because it shares a run_id.
//
// Status strings come back RAW. Normalising them here would mean writing
// Python's `str.strip().lower()` in SQL, and the two do not agree: str.strip()
// removes unicode whitespace, ClickHouse's trim and RE2's \s do not.
// normalizeTestStatus in the testops package does it instead, on exactly the
// strings Python would have seen. (Contrast loadNativeHistoricalFailedCaseNames
// below, where the SQL-side lower(trim(...)) is CORRECT -- because Python's own
// query already does it that way, so copying it is fidelity, not divergence.)
//
// coalesce(status, ”) keeps the NULL case explicit: Python maps a NULL status
// to None to "" through _normalize_test_status, and groupUniqArray would
// otherwise drop NULLs from the array entirely.
func loadNativeTestopsCaseGroups(
	ctx context.Context,
	conn testopsNativeConn,
	accumulator *testops.TestAccumulator,
	orgID string,
	repoID uuid.UUID,
	start, end time.Time,
) error {
	rows, err := conn.Query(ctx, `
SELECT
  case_name,
  groupUniqArray(status) AS statuses,
  max(retry_attempt) AS max_retry
FROM (
  SELECT
    run_id,
    suite_id,
    tupleElement(winner, 1) AS case_name,
    tupleElement(winner, 2) AS status,
    tupleElement(winner, 3) AS retry_attempt
  FROM (
    SELECT
      org_id,
      run_id,
      suite_id,
      argMax((case_name, ifNull(toString(status), ''), retry_attempt), last_synced) AS winner
    FROM test_case_results
    WHERE org_id = ? AND repo_id = ?
    GROUP BY org_id, repo_id, run_id, suite_id, case_id
  )
)
WHERE case_name != ''
  AND run_id IN (
    SELECT run_id FROM (
      SELECT run_id, argMax((started_at, finished_at), last_synced) AS w
      FROM test_suite_results
      WHERE org_id = ? AND repo_id = ?
      GROUP BY org_id, repo_id, run_id, suite_id
    )
    WHERE coalesce(tupleElement(w, 1), tupleElement(w, 2)) >= ?
      AND coalesce(tupleElement(w, 1), tupleElement(w, 2)) < ?
  )
  AND (run_id, suite_id) IN (
    SELECT run_id, suite_id FROM (
      SELECT run_id, suite_id, argMax((started_at, finished_at), last_synced) AS w
      FROM test_suite_results
      WHERE org_id = ? AND repo_id = ?
      GROUP BY org_id, repo_id, run_id, suite_id
    )
    WHERE coalesce(tupleElement(w, 1), tupleElement(w, 2)) < ?
  )
GROUP BY case_name
ORDER BY case_name`,
		orgID, repoID,
		orgID, repoID, start.UTC(), end.UTC(),
		orgID, repoID, end.UTC())
	if err != nil {
		return fmt.Errorf("load native testops case groups: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var group testops.CaseGroup
		if err := rows.Scan(&group.CaseName, &group.Statuses, &group.MaxRetry); err != nil {
			return fmt.Errorf("scan native testops case group: %w", err)
		}
		accumulator.AddCaseGroup(group)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate native testops case groups: %w", err)
	}
	return nil
}

// loadNativeHistoricalFailedCaseNames ports
// load_testops_historical_failed_case_names (loaders/clickhouse.py:1498):
// distinct case names that failed in [start, end), EXCLUDING any run that also
// has a suite in [end, currentDayEnd) so a run straddling the day boundary is
// not counted as both "today" and "historical".
//
// The result is already a reduction (distinct names over a 29-day window), so
// there is nothing left to push down -- the cap is simply dropped, and the two
// source reads gain argMax dedup.
//
// lower(trim(status)) IS evaluated in SQL here, unlike the case-group reader.
// That is deliberate fidelity: Python's own version of this query does the
// normalisation in SQL with the same expression, so matching it is what keeps
// the two sides identical. FAILURE_STATUSES (compute_testops.py:54) is the
// vocabulary, passed as parameters rather than interpolated.
func loadNativeHistoricalFailedCaseNames(
	ctx context.Context,
	conn testopsNativeConn,
	orgID string,
	repoID uuid.UUID,
	start, end, currentDayEnd time.Time,
) (map[string]struct{}, error) {
	rows, err := conn.Query(ctx, `
SELECT DISTINCT c.case_name AS case_name
FROM (
  SELECT
    run_id,
    suite_id,
    tupleElement(winner, 1) AS case_name,
    tupleElement(winner, 2) AS status
  FROM (
    SELECT
      org_id, run_id, suite_id,
      argMax((case_name, ifNull(toString(status), '')), last_synced) AS winner
    FROM test_case_results
    WHERE org_id = ? AND repo_id = ?
    GROUP BY org_id, repo_id, run_id, suite_id, case_id
  )
) AS c
INNER JOIN (
  SELECT
    run_id,
    suite_id,
    tupleElement(w, 1) AS started_at,
    tupleElement(w, 2) AS finished_at
  FROM (
    SELECT org_id, run_id, suite_id, argMax((started_at, finished_at), last_synced) AS w
    FROM test_suite_results
    WHERE org_id = ? AND repo_id = ?
    GROUP BY org_id, repo_id, run_id, suite_id
  )
) AS s ON (s.run_id = c.run_id) AND (s.suite_id = c.suite_id)
WHERE coalesce(s.started_at, s.finished_at) >= ?
  AND coalesce(s.started_at, s.finished_at) < ?
  AND lower(trim(c.status)) IN (?, ?, ?, ?, ?, ?)
  AND s.run_id NOT IN (
    SELECT run_id FROM (
      SELECT run_id, argMax((started_at, finished_at), last_synced) AS w2
      FROM test_suite_results
      WHERE org_id = ? AND repo_id = ?
      GROUP BY org_id, repo_id, run_id, suite_id
    )
    WHERE coalesce(tupleElement(w2, 1), tupleElement(w2, 2)) >= ?
      AND coalesce(tupleElement(w2, 1), tupleElement(w2, 2)) < ?
  )
ORDER BY case_name`,
		orgID, repoID,
		orgID, repoID,
		start.UTC(), end.UTC(),
		"failure", "failed", "error", "errors", "timeout", "timed_out",
		orgID, repoID, end.UTC(), currentDayEnd.UTC())
	if err != nil {
		return nil, fmt.Errorf("load native testops historical failed case names: %w", err)
	}
	defer rows.Close()

	result := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan native testops historical failed case name: %w", err)
		}
		if name != "" {
			result[name] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate native testops historical failed case names: %w", err)
	}
	return result, nil
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
	rows, err := conn.Query(ctx, `
SELECT
  c.run_id AS run_id,
  c.snapshot_id AS snapshot_id,
  c.lines_total AS lines_total,
  c.lines_covered AS lines_covered,
  c.line_coverage_pct AS line_coverage_pct,
  c.branch_coverage_pct AS branch_coverage_pct,
  c.team_id AS team_id,
  c.service_id AS service_id,
  c.org_id AS org_id
FROM (
  SELECT
    org_id,
    run_id,
    snapshot_id,
    tupleElement(winner, 1) AS lines_total,
    tupleElement(winner, 2) AS lines_covered,
    tupleElement(winner, 3) AS line_coverage_pct,
    tupleElement(winner, 4) AS branch_coverage_pct,
    tupleElement(winner, 5) AS team_id,
    tupleElement(winner, 6) AS service_id
  FROM (
    SELECT
      org_id, run_id, snapshot_id,
      argMax(
        (lines_total, lines_covered, line_coverage_pct, branch_coverage_pct, team_id, service_id),
        last_synced
      ) AS winner
    FROM coverage_snapshots
    WHERE org_id = ? AND repo_id = ?
    GROUP BY org_id, repo_id, run_id, snapshot_id
  )
) AS c
INNER JOIN (
  -- ci_pipeline_runs.started_at is NOT Nullable, so a bare argMax is safe
  -- here: the NULL-skipping the tuple form defuses elsewhere cannot apply.
  SELECT run_id, argMax(started_at, last_synced) AS started_at
  FROM ci_pipeline_runs
  WHERE org_id = ? AND repo_id = ?
  GROUP BY org_id, repo_id, run_id
) AS p ON (p.run_id = c.run_id)
WHERE p.started_at >= ? AND p.started_at < ?
ORDER BY c.run_id DESC, c.snapshot_id DESC
LIMIT 1`,
		orgID, repoID, orgID, repoID, start.UTC(), end.UTC())
	if err != nil {
		return nil, fmt.Errorf("load native testops coverage snapshot: %w", err)
	}
	defer rows.Close()

	var result []testops.CoverageSnapshotRow
	for rows.Next() {
		row := testops.CoverageSnapshotRow{RepoID: repoID}
		if err := rows.Scan(
			&row.RunID, &row.SnapshotID, &row.LinesTotal, &row.LinesCovered,
			&row.LineCoveragePct, &row.BranchCoveragePct, &row.TeamID, &row.ServiceID, &row.OrgID,
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
// All three target tables are plain MergeTree ORDER BY (repo_id, day)
// (029_testops_tables.sql:106,130,155) -- NO ReplacingMergeTree, so nothing
// deduplicates a second write of the same (repo_id, day). That is why
// job_daily.py's three s.write_testops_* calls must be skip-gated in the same
// change that registers these executors: without the gate every partition
// would write each row twice, and no engine would collapse them.
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
		team_id, service_id, org_id, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_pipeline_metrics_daily batch: %w", err)
	}
	for _, metric := range metrics {
		if err := batch.Append(
			metric.RepoID, chDate(day),
			uint32(metric.PipelinesCount), uint32(metric.SuccessCount),
			uint32(metric.FailureCount), uint32(metric.CancelledCount),
			metric.SuccessRate, metric.FailureRate, metric.CancelRate, metric.RerunRate,
			metric.MedianDurationSeconds, metric.P95DurationSeconds,
			metric.AvgQueueSeconds, metric.P95QueueSeconds,
			metric.TeamID, metric.ServiceID, organizationID, computedAt.UTC(),
		); err != nil {
			return 0, fmt.Errorf("append testops_pipeline_metrics_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send testops_pipeline_metrics_daily batch: %w", err)
	}
	return len(metrics), nil
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
		team_id, service_id, org_id, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_test_metrics_daily batch: %w", err)
	}
	for _, metric := range metrics {
		if err := batch.Append(
			metric.RepoID, chDate(day),
			uint32(metric.TotalCases), uint32(metric.PassedCount), uint32(metric.FailedCount),
			uint32(metric.SkippedCount), uint32(metric.QuarantinedCount),
			metric.PassRate, metric.FailureRate, metric.FlakeRate, metric.RetryDependencyRate,
			uint32(metric.TotalSuites),
			metric.SuiteDurationP50Seconds, metric.SuiteDurationP95Seconds, metric.FailureRecurrence,
			metric.TeamID, metric.ServiceID, organizationID, computedAt.UTC(),
		); err != nil {
			return 0, fmt.Errorf("append testops_test_metrics_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send testops_test_metrics_daily batch: %w", err)
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
		team_id, service_id, org_id, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_coverage_metrics_daily batch: %w", err)
	}
	for _, metric := range metrics {
		if err := batch.Append(
			metric.RepoID, chDate(day),
			metric.LineCoveragePct, metric.BranchCoveragePct,
			metric.LinesTotal, metric.LinesCovered, metric.CoverageDeltaPct,
			uint32(metric.UncoveredFilesCount), uint32(metric.CoverageRegressionCount),
			metric.TeamID, metric.ServiceID, organizationID, computedAt.UTC(),
		); err != nil {
			return 0, fmt.Errorf("append testops_coverage_metrics_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send testops_coverage_metrics_daily batch: %w", err)
	}
	return len(metrics), nil
}
