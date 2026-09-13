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
       retry_count, team_id, service_id, org_id, run_id
FROM ci_pipeline_runs FINAL
WHERE org_id = ? AND repo_id = ?
  AND started_at >= ? AND started_at < ?
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
SELECT run_id, suite_id, total_count, passed_count, failed_count, skipped_count,
       error_count, quarantined_count, duration_seconds, started_at, finished_at,
       team_id, service_id, org_id
FROM test_suite_results FINAL
WHERE org_id = ? AND repo_id = ?
  AND coalesce(started_at, finished_at) >= ? AND coalesce(started_at, finished_at) < ?
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

// loadNativeTestopsCaseAggregate is the one test_case_results read behind a
// test metric record, reduced inside ClickHouse to one row per case_name. The
// table's sorting key has no time column, so any read of it scans the repo's
// whole case history; both questions the record asks of it are therefore
// answered in the same scan. It folds today's cases into accumulator and
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
	rows, err := conn.Query(ctx, `
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
ORDER BY case_name`,
		orgID, repoID, start.UTC(), end.UTC(),
		orgID, repoID, end.UTC(),
		"failure", "failed", "error", "errors", "timeout", "timed_out",
		orgID, repoID, historyStart.UTC(), start.UTC(),
		orgID, repoID)
	if err != nil {
		return nil, fmt.Errorf("load native testops case aggregate: %w", err)
	}
	defer rows.Close()

	historicalFailedNames := make(map[string]struct{})
	for rows.Next() {
		var group testops.CaseGroup
		var seenToday, failedBefore uint8
		if err := rows.Scan(&group.CaseName, &group.Statuses, &group.MaxRetry, &seenToday, &failedBefore); err != nil {
			return nil, fmt.Errorf("scan native testops case aggregate: %w", err)
		}
		if seenToday != 0 {
			accumulator.AddCaseGroup(group)
		}
		if failedBefore != 0 {
			historicalFailedNames[group.CaseName] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate native testops case aggregate: %w", err)
	}
	return historicalFailedNames, nil
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
SELECT c.run_id, c.snapshot_id, c.lines_total, c.lines_covered,
       c.line_coverage_pct, c.branch_coverage_pct, c.team_id, c.service_id, c.org_id
FROM coverage_snapshots AS c FINAL
INNER JOIN ci_pipeline_runs AS p FINAL
  ON (p.repo_id = c.repo_id) AND (p.run_id = c.run_id) AND (p.org_id = c.org_id)
WHERE p.started_at >= ? AND p.started_at < ? AND p.repo_id = ? AND p.org_id = ?
ORDER BY c.run_id DESC, c.snapshot_id DESC
LIMIT 1`,
		start.UTC(), end.UTC(), repoID, orgID)
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
