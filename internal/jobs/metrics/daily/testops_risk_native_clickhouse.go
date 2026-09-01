package daily

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// -----------------------------------------------------------------------
// Fidelity notes (CHAOS-4294)
//
// The Python authority is:
//   - src/dev_health_ops/metrics/compute_testops.py
//     (compute_pipeline_metrics_daily, compute_test_metrics_daily,
//     compute_coverage_metrics_daily -- these three families,
//     testops_pipeline/testops_test/testops_coverage, families.json,
//     CHAOS-4284 -- stay "pending"/bridge; they are ported here ONLY as
//     unexported, in-memory helpers so this executor can reproduce the
//     SAME-RUN inputs testops_risk's three risk-model functions read.)
//   - src/dev_health_ops/metrics/compute_testops_risk.py
//     (compute_release_confidence, compute_quality_drag,
//     compute_pipeline_stability -- the family this executor actually
//     ports and writes.)
//   - src/dev_health_ops/metrics/loaders/clickhouse.py
//     (load_testops_pipeline_data, load_testops_test_data,
//     load_testops_historical_failed_case_names, load_testops_coverage_data)
//
// WHY the pending families are re-derived here instead of read back from
// ClickHouse: job_daily.py computes testops_pipeline_metrics/
// testops_test_metrics/testops_coverage_metrics as LOCAL, IN-PROCESS values
// (job_daily.py:1602-1626) and feeds them directly into
// compute_release_confidence/compute_quality_drag/compute_pipeline_stability
// a few lines later (job_daily.py:1904-1927) -- it never re-reads
// testops_{pipeline,test,coverage}_metrics_daily from ClickHouse. Go's
// PartitionHandler.Work runs every native family BEFORE the one combined
// Python compatibility-bridge call (daily.go: computeNativeFamilies() then
// compatibility.ComputePartition()), so on the day this partition is
// computing, those bridge-written tables do not have TODAY's rows yet --
// only a prior day's, if any. Reading them back from ClickHouse here would
// silently score testops_risk against stale or missing inputs. Recomputing
// the same aggregation Python performs in-process, from the same raw
// tables, is what keeps this native path row-identical.
//
// SCOPE PER CALL: the Python bridge invokes run_daily_metrics_job once PER
// repo_id (worker_metrics.py:1729, CHAOS-4264) with backfill_days=1, so
// `days = [target_day]` and every local variable -- including the
// "pipeline_metrics_buffer" pipeline_stability reads -- is fresh per call.
// In production this buffer therefore holds AT MOST the current day's own
// row for that one repo: pipeline_stability's "7-day rolling window" is a
// real capability of the underlying function, but the live per-repo/per-day
// partition call site never gives it more than one day of history. This
// executor mirrors that exact scope: one repo, one day, no synthetic
// history. See TestopsRiskExecutor's doc comment for the loop structure.
// -----------------------------------------------------------------------

// testopsRiskConn is the narrow ClickHouse capability this file needs.
type testopsRiskConn interface {
	Query(context.Context, string, ...any) (driver.Rows, error)
}

// testopsRiskBatchConn is the narrow write capability writeTestopsRisk needs.
type testopsRiskBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

func derefStr(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// testopsLoaderMaxRows ports _testops_loader_max_rows (loaders/clickhouse.py:99):
// same env var, same default, same "non-positive or unparseable falls back
// to the default" behavior.
func testopsLoaderMaxRows() int {
	const defaultMaxRows = 200_000
	raw, err := envInt("DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS", defaultMaxRows)
	if err != nil || raw <= 0 {
		return defaultMaxRows
	}
	return raw
}

// errTestopsRowCapExceeded ports TestopsRowCapExceeded (loaders/clickhouse.py:110):
// a bounded, classified refusal rather than an unbounded read. ComputeFamily
// returning this error is not a partition failure -- PartitionHandler's
// fail-open native-family policy (daily.go computeNativeFamilies) simply
// leaves testops_risk off the skip list for this partition, so the Python
// compatibility bridge computes and writes it exactly as it would have
// before this executor existed (the bridge enforces the identical cap on
// its own read).
type errTestopsRowCapExceeded struct {
	table   string
	maxRows int
	fetched int
}

func (err *errTestopsRowCapExceeded) Error() string {
	return fmt.Sprintf(
		"testops_row_cap_exceeded: table=%q max_rows=%d fetched>=%d -- refusing to compute testops metrics on a partial/truncated result",
		err.table, err.maxRows, err.fetched,
	)
}

// -----------------------------------------------------------------------
// Raw row shapes (ports of the TypedDicts in testops_schemas.py, narrowed to
// the columns compute_testops.py actually reads).
// -----------------------------------------------------------------------

type testopsPipelineRunRow struct {
	RepoID       uuid.UUID
	Status       *string
	QueuedAt     *time.Time
	StartedAt    time.Time
	FinishedAt   *time.Time
	DurationSecs *float64
	QueueSecs    *float64
	RetryCount   uint32
	TeamID       *string
	ServiceID    *string
	OrgID        string
}

type testopsSuiteRow struct {
	RepoID           uuid.UUID
	RunID            string
	SuiteID          string
	TotalCount       uint32
	PassedCount      uint32
	FailedCount      uint32
	SkippedCount     uint32
	ErrorCount       uint32
	QuarantinedCount uint32
	DurationSecs     *float64
	StartedAt        *time.Time
	FinishedAt       *time.Time
	TeamID           *string
	ServiceID        *string
	OrgID            string
}

type testopsCaseRow struct {
	RepoID       uuid.UUID
	RunID        string
	SuiteID      string
	CaseName     string
	Status       *string
	RetryAttempt uint32
}

type testopsCoverageSnapshotRow struct {
	RepoID            uuid.UUID
	RunID             string
	SnapshotID        string
	LinesTotal        *uint32
	LinesCovered      *uint32
	LineCoveragePct   *float64
	BranchCoveragePct *float64
	TeamID            *string
	ServiceID         *string
	OrgID             string
}

// -----------------------------------------------------------------------
// Loaders -- ports of ClickHouseDataLoader.load_testops_* (loaders/clickhouse.py).
// Every loader here is scoped to exactly ONE repo (this executor's own
// per-repo loop, mirroring the Python bridge's per-repo_id call) and one
// organization, unlike the Python methods' optional org-wide mode -- a
// native executor is always constructed for one run.OrganizationID, so the
// "repo_id is None" / "self.org_id is empty" branches those methods carry
// for admin/backfill tooling are dropped here as genuinely unreachable from
// this call site.
//
// job_runs (ci_job_runs) is deliberately NOT loaded: compute_pipeline_metrics_daily
// receives job_runs only to `del` it immediately (compute_testops.py:123) --
// it is unused by the family this executor ports.
// -----------------------------------------------------------------------

func loadTestopsPipelineRuns(
	ctx context.Context, conn testopsRiskConn, orgID string, repoID uuid.UUID, start, end time.Time,
) ([]testopsPipelineRunRow, error) {
	rows, err := conn.Query(ctx, `
SELECT status, queued_at, started_at, finished_at, duration_seconds, queue_seconds,
       retry_count, team_id, service_id, org_id
FROM ci_pipeline_runs FINAL
WHERE started_at >= ? AND started_at < ? AND repo_id = ? AND org_id = ?`,
		start.UTC(), end.UTC(), repoID, orgID)
	if err != nil {
		return nil, fmt.Errorf("load testops pipeline runs: %w", err)
	}
	defer rows.Close()

	var result []testopsPipelineRunRow
	for rows.Next() {
		row := testopsPipelineRunRow{RepoID: repoID}
		if err := rows.Scan(
			&row.Status, &row.QueuedAt, &row.StartedAt, &row.FinishedAt,
			&row.DurationSecs, &row.QueueSecs, &row.RetryCount,
			&row.TeamID, &row.ServiceID, &row.OrgID,
		); err != nil {
			return nil, fmt.Errorf("scan testops pipeline run: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate testops pipeline runs: %w", err)
	}
	return result, nil
}

// loadTestopsSuiteAndCaseRows ports load_testops_test_data
// (loaders/clickhouse.py:1344), including its two-query semi-join shape:
// suites in [start,end) for this repo, then cases whose run has SOME suite
// in [start,end) for this repo AND whose OWN suite starts before `end`
// (the day-boundary guard load_testops_test_data:1459 documents). Row-cap
// enforced on both, suites first (mirrors the Python ordering rationale).
func loadTestopsSuiteAndCaseRows(
	ctx context.Context, conn testopsRiskConn, orgID string, repoID uuid.UUID, start, end time.Time,
) ([]testopsSuiteRow, []testopsCaseRow, error) {
	maxRows := testopsLoaderMaxRows()
	limit := maxRows + 1

	suiteRows, err := conn.Query(ctx, `
SELECT repo_id, run_id, suite_id, total_count, passed_count, failed_count, skipped_count,
       error_count, quarantined_count, duration_seconds, started_at, finished_at,
       team_id, service_id, org_id
FROM test_suite_results FINAL
WHERE coalesce(started_at, finished_at) >= ? AND coalesce(started_at, finished_at) < ?
  AND repo_id = ? AND org_id = ?
LIMIT ?`,
		start.UTC(), end.UTC(), repoID, orgID, uint64(limit))
	if err != nil {
		return nil, nil, fmt.Errorf("load testops suite results: %w", err)
	}
	var suites []testopsSuiteRow
	for suiteRows.Next() {
		var row testopsSuiteRow
		if err := suiteRows.Scan(
			&row.RepoID, &row.RunID, &row.SuiteID, &row.TotalCount, &row.PassedCount,
			&row.FailedCount, &row.SkippedCount, &row.ErrorCount, &row.QuarantinedCount,
			&row.DurationSecs, &row.StartedAt, &row.FinishedAt, &row.TeamID, &row.ServiceID, &row.OrgID,
		); err != nil {
			suiteRows.Close()
			return nil, nil, fmt.Errorf("scan testops suite result: %w", err)
		}
		suites = append(suites, row)
	}
	suiteErr := suiteRows.Err()
	suiteRows.Close()
	if suiteErr != nil {
		return nil, nil, fmt.Errorf("iterate testops suite results: %w", suiteErr)
	}
	if len(suites) > maxRows {
		return nil, nil, &errTestopsRowCapExceeded{table: "test_suite_results", maxRows: maxRows, fetched: len(suites)}
	}

	caseRows, err := conn.Query(ctx, `
SELECT c.repo_id, c.run_id, c.suite_id, c.case_name, c.status, c.retry_attempt
FROM test_case_results AS c FINAL
WHERE (c.repo_id, c.run_id) IN (
  SELECT repo_id, run_id FROM test_suite_results FINAL
  WHERE coalesce(started_at, finished_at) >= ? AND coalesce(started_at, finished_at) < ?
    AND repo_id = ? AND org_id = ?
)
AND (c.repo_id, c.run_id, c.suite_id) IN (
  SELECT repo_id, run_id, suite_id FROM test_suite_results FINAL
  WHERE coalesce(started_at, finished_at) < ?
    AND repo_id = ? AND org_id = ?
)
AND c.repo_id = ? AND c.org_id = ?
LIMIT ?`,
		start.UTC(), end.UTC(), repoID, orgID,
		end.UTC(), repoID, orgID,
		repoID, orgID, uint64(limit))
	if err != nil {
		return nil, nil, fmt.Errorf("load testops case results: %w", err)
	}
	var cases []testopsCaseRow
	for caseRows.Next() {
		var row testopsCaseRow
		if err := caseRows.Scan(&row.RepoID, &row.RunID, &row.SuiteID, &row.CaseName, &row.Status, &row.RetryAttempt); err != nil {
			caseRows.Close()
			return nil, nil, fmt.Errorf("scan testops case result: %w", err)
		}
		cases = append(cases, row)
	}
	caseErr := caseRows.Err()
	caseRows.Close()
	if caseErr != nil {
		return nil, nil, fmt.Errorf("iterate testops case results: %w", caseErr)
	}
	if len(cases) > maxRows {
		return nil, nil, &errTestopsRowCapExceeded{table: "test_case_results", maxRows: maxRows, fetched: len(cases)}
	}
	return suites, cases, nil
}

// loadHistoricalFailedCaseNames ports load_testops_historical_failed_case_names
// (loaders/clickhouse.py:1498), narrowed to one repo: distinct case names
// that failed in [start,end) EXCLUDING any run_id that also has a suite in
// [end,currentDayEnd) -- the same day-boundary run_id exclusion that
// prevents a straddling run from being double counted as both "today" and
// "historical".
func loadHistoricalFailedCaseNames(
	ctx context.Context, conn testopsRiskConn, orgID string, repoID uuid.UUID, start, end, currentDayEnd time.Time,
) (map[string]struct{}, error) {
	maxRows := testopsLoaderMaxRows()
	limit := maxRows + 1

	rows, err := conn.Query(ctx, `
SELECT DISTINCT c.case_name AS case_name
FROM test_case_results AS c FINAL
INNER JOIN test_suite_results AS s FINAL
  ON (s.repo_id = c.repo_id) AND (s.run_id = c.run_id) AND (s.suite_id = c.suite_id) AND (s.org_id = c.org_id)
WHERE coalesce(s.started_at, s.finished_at) >= ? AND coalesce(s.started_at, s.finished_at) < ?
  AND lower(trim(c.status)) IN (?, ?, ?, ?, ?, ?)
  AND (s.repo_id, s.run_id) NOT IN (
    SELECT repo_id, run_id FROM test_suite_results FINAL
    WHERE coalesce(started_at, finished_at) >= ? AND coalesce(started_at, finished_at) < ?
      AND repo_id = ? AND org_id = ?
  )
  AND s.repo_id = ? AND s.org_id = ?
LIMIT ?`,
		start.UTC(), end.UTC(),
		"failure", "failed", "error", "errors", "timeout", "timed_out",
		end.UTC(), currentDayEnd.UTC(), repoID, orgID,
		repoID, orgID, uint64(limit))
	if err != nil {
		return nil, fmt.Errorf("load testops historical failed case names: %w", err)
	}
	defer rows.Close()

	result := make(map[string]struct{})
	count := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan testops historical failed case name: %w", err)
		}
		count++
		if name != "" {
			result[name] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate testops historical failed case names: %w", err)
	}
	if count > maxRows {
		return nil, &errTestopsRowCapExceeded{table: "test_case_results:historical_names", maxRows: maxRows, fetched: count}
	}
	return result, nil
}

// loadTestopsCoverageSnapshots ports load_testops_coverage_data
// (loaders/clickhouse.py:1665): coverage snapshots joined to a pipeline run
// starting in [start,end), for one repo.
func loadTestopsCoverageSnapshots(
	ctx context.Context, conn testopsRiskConn, orgID string, repoID uuid.UUID, start, end time.Time,
) ([]testopsCoverageSnapshotRow, error) {
	rows, err := conn.Query(ctx, `
SELECT c.repo_id, c.run_id, c.snapshot_id, c.lines_total, c.lines_covered,
       c.line_coverage_pct, c.branch_coverage_pct, c.team_id, c.service_id, c.org_id
FROM coverage_snapshots AS c FINAL
INNER JOIN ci_pipeline_runs AS p FINAL
  ON (p.repo_id = c.repo_id) AND (p.run_id = c.run_id) AND (p.org_id = c.org_id)
WHERE p.started_at >= ? AND p.started_at < ? AND p.repo_id = ? AND p.org_id = ?`,
		start.UTC(), end.UTC(), repoID, orgID)
	if err != nil {
		return nil, fmt.Errorf("load testops coverage snapshots: %w", err)
	}
	defer rows.Close()

	var result []testopsCoverageSnapshotRow
	for rows.Next() {
		var row testopsCoverageSnapshotRow
		if err := rows.Scan(
			&row.RepoID, &row.RunID, &row.SnapshotID, &row.LinesTotal, &row.LinesCovered,
			&row.LineCoveragePct, &row.BranchCoveragePct, &row.TeamID, &row.ServiceID, &row.OrgID,
		); err != nil {
			return nil, fmt.Errorf("scan testops coverage snapshot: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate testops coverage snapshots: %w", err)
	}
	return result, nil
}

// -----------------------------------------------------------------------
// Compute -- ports of compute_testops.py's three family functions, narrowed
// to what testops_risk's own functions read.
// -----------------------------------------------------------------------

type testopsPipelineMetric struct {
	RepoID                uuid.UUID
	SuccessRate           float64
	FailureCount          int
	PipelinesCount        int
	MedianDurationSeconds *float64
	AvgQueueSeconds       *float64
	RerunRate             float64
	TeamID                *string
	ServiceID             *string
	OrgID                 string
}

type testopsTestMetric struct {
	RepoID            uuid.UUID
	PassRate          float64
	FlakeRate         float64
	FailureRecurrence float64
	TotalCases        int
	TeamID            *string
	ServiceID         *string
	OrgID             string
}

type testopsCoverageMetric struct {
	RepoID           uuid.UUID
	LineCoveragePct  *float64
	CoverageDeltaPct *float64
}

func normalizePipelineStatus(status *string) string {
	normalized := trimLower(derefStr(status))
	switch normalized {
	case "success", "succeeded", "passed":
		return "success"
	case "failure", "failed", "error", "errors", "timeout", "timed_out":
		return "failure"
	case "cancelled", "canceled", "cancel":
		return "cancelled"
	default:
		return normalized
	}
}

var testopsFailureStatuses = map[string]struct{}{
	"failure": {}, "failed": {}, "error": {}, "errors": {}, "timeout": {}, "timed_out": {},
}

func normalizeTestStatus(status *string) string {
	normalized := trimLower(derefStr(status))
	switch normalized {
	case "success", "succeeded", "passed":
		return "passed"
	default:
		if _, ok := testopsFailureStatuses[normalized]; ok {
			return "failed"
		}
		switch normalized {
		case "quarantined", "quarantine":
			return "quarantined"
		case "skipped", "skip":
			return "skipped"
		default:
			return normalized
		}
	}
}

// computeTestopsPipelineMetric ports compute_pipeline_metrics_daily
// (compute_testops.py:114), narrowed to ONE repo (this executor's per-repo
// loop already applied the (repo_id, day) scope the SQL WHERE clause
// enforces in Python). Unlike Python, which groups by (repo_id, team_id,
// service_id) and can emit several rows for one repo when rows disagree on
// team/service, this returns one row PER (team_id, service_id) group within
// the repo -- same grouping key, same shape, just pre-scoped to one repo_id.
func computeTestopsPipelineMetrics(repoID uuid.UUID, rows []testopsPipelineRunRow) []testopsPipelineMetric {
	type bucket struct {
		pipelines, success, failure, cancelled, reruns int
		durations, queues                              []float64
		orgID                                          string
	}
	type key struct{ teamID, serviceID string }
	byGroup := make(map[key]*bucket)
	var order []key
	for _, row := range rows {
		k := key{teamID: derefStr(row.TeamID), serviceID: derefStr(row.ServiceID)}
		b, ok := byGroup[k]
		if !ok {
			b = &bucket{orgID: row.OrgID}
			byGroup[k] = b
			order = append(order, k)
		}
		b.pipelines++
		switch normalizePipelineStatus(row.Status) {
		case "success":
			b.success++
		case "failure":
			b.failure++
		case "cancelled":
			b.cancelled++
		}
		if row.RetryCount > 0 {
			b.reruns++
		}
		if d := safeDurationSeconds(row.QueuedAt, row.StartedAt, row.FinishedAt, row.DurationSecs, false); d != nil {
			b.durations = append(b.durations, *d)
		}
		if q := safeDurationSeconds(row.QueuedAt, row.StartedAt, row.FinishedAt, row.QueueSecs, true); q != nil {
			b.queues = append(b.queues, *q)
		}
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].teamID != order[j].teamID {
			return order[i].teamID < order[j].teamID
		}
		return order[i].serviceID < order[j].serviceID
	})
	result := make([]testopsPipelineMetric, 0, len(order))
	for _, k := range order {
		b := byGroup[k]
		metric := testopsPipelineMetric{RepoID: repoID, OrgID: b.orgID, PipelinesCount: b.pipelines, FailureCount: b.failure}
		if b.pipelines > 0 {
			metric.SuccessRate = float64(b.success) / float64(b.pipelines)
			metric.RerunRate = float64(b.reruns) / float64(b.pipelines)
		}
		if len(b.durations) > 0 {
			v := median(b.durations)
			metric.MedianDurationSeconds = &v
		}
		if len(b.queues) > 0 {
			v := mean(b.queues)
			metric.AvgQueueSeconds = &v
		}
		if k.teamID != "" {
			metric.TeamID = strPtr(k.teamID)
		}
		if k.serviceID != "" {
			metric.ServiceID = strPtr(k.serviceID)
		}
		result = append(result, metric)
	}
	return result
}

// safeDurationSeconds ports _safe_duration_seconds/_safe_queue_seconds
// (compute_testops.py:72,85) -- both share this shape: prefer a
// non-negative explicit value, else derive from a start/end pair, never
// negative. isQueue picks the (queuedAt, startedAt) pair instead of
// (startedAt, finishedAt).
func safeDurationSeconds(queuedAt *time.Time, startedAt time.Time, finishedAt *time.Time, explicit *float64, isQueue bool) *float64 {
	if explicit != nil && *explicit >= 0 {
		v := *explicit
		return &v
	}
	if isQueue {
		if queuedAt == nil {
			return nil
		}
		d := startedAt.Sub(*queuedAt).Seconds()
		if d < 0 {
			return nil
		}
		return &d
	}
	if finishedAt == nil {
		return nil
	}
	d := finishedAt.Sub(startedAt).Seconds()
	if d < 0 {
		return nil
	}
	return &d
}

// computeTestopsTestMetrics ports compute_test_metrics_daily
// (compute_testops.py:216), narrowed to one repo. suites/cases are already
// scoped to this repo and day by the loader's semi-join; a repo with
// neither produces no rows here, matching Python's `if not repo_suites and
// not repo_cases: continue`.
func computeTestopsTestMetrics(
	repoID uuid.UUID, suites []testopsSuiteRow, cases []testopsCaseRow, historicalFailedNames map[string]struct{},
) []testopsTestMetric {
	if len(suites) == 0 && len(cases) == 0 {
		return nil
	}
	var totalCases, passedCount, failedCount int
	for _, s := range suites {
		totalCases += int(s.TotalCount)
		passedCount += int(s.PassedCount)
		failedCount += int(s.FailedCount) + int(s.ErrorCount)
	}

	caseStatuses := make(map[string]map[string]struct{})
	retryAttemptsByCase := make(map[string]map[uint32]struct{})
	currentFailedNames := make(map[string]struct{})
	for _, c := range cases {
		if c.CaseName == "" {
			continue
		}
		normalized := normalizeTestStatus(c.Status)
		if caseStatuses[c.CaseName] == nil {
			caseStatuses[c.CaseName] = make(map[string]struct{})
		}
		caseStatuses[c.CaseName][normalized] = struct{}{}
		if retryAttemptsByCase[c.CaseName] == nil {
			retryAttemptsByCase[c.CaseName] = make(map[uint32]struct{})
		}
		retryAttemptsByCase[c.CaseName][c.RetryAttempt] = struct{}{}
		if normalized == "failed" {
			currentFailedNames[c.CaseName] = struct{}{}
		}
	}
	distinctCases := len(caseStatuses)
	var flakeCases, retryDependentCases int
	for name, statuses := range caseStatuses {
		_, hasPassed := statuses["passed"]
		_, hasFailed := statuses["failed"]
		if hasPassed && hasFailed {
			flakeCases++
		}
		if hasPassed {
			for attempt := range retryAttemptsByCase[name] {
				if attempt > 0 {
					retryDependentCases++
					break
				}
			}
		}
	}
	recurrentFailures := 0
	for name := range currentFailedNames {
		if _, ok := historicalFailedNames[name]; ok {
			recurrentFailures++
		}
	}

	var first *testopsSuiteRow
	if len(suites) > 0 {
		first = &suites[0]
	}
	metric := testopsTestMetric{RepoID: repoID, TotalCases: totalCases}
	if totalCases > 0 {
		metric.PassRate = float64(passedCount) / float64(totalCases)
	}
	if distinctCases > 0 {
		metric.FlakeRate = float64(flakeCases) / float64(distinctCases)
	}
	if len(currentFailedNames) > 0 {
		metric.FailureRecurrence = float64(recurrentFailures) / float64(len(currentFailedNames))
	}
	if first != nil {
		metric.TeamID = first.TeamID
		metric.ServiceID = first.ServiceID
		metric.OrgID = first.OrgID
	}
	return []testopsTestMetric{metric}
}

// computeTestopsCoverageMetric ports compute_coverage_metrics_daily
// (compute_testops.py:371), narrowed to one repo: the latest (by
// (run_id, snapshot_id) lexical order, matching Python's tuple-comparison
// tie-break) current-window snapshot and, if present, the latest
// prior-window snapshot for the coverage delta.
func computeTestopsCoverageMetric(repoID uuid.UUID, current, prior []testopsCoverageSnapshotRow) *testopsCoverageMetric {
	latest := latestSnapshot(current)
	if latest == nil {
		return nil
	}
	priorLatest := latestSnapshot(prior)
	metric := &testopsCoverageMetric{RepoID: repoID, LineCoveragePct: latest.LineCoveragePct}
	if latest.LineCoveragePct != nil && priorLatest != nil && priorLatest.LineCoveragePct != nil {
		delta := *latest.LineCoveragePct - *priorLatest.LineCoveragePct
		metric.CoverageDeltaPct = &delta
	}
	return metric
}

func latestSnapshot(rows []testopsCoverageSnapshotRow) *testopsCoverageSnapshotRow {
	var latest *testopsCoverageSnapshotRow
	for index := range rows {
		row := &rows[index]
		if latest == nil || snapshotKeyLess(*latest, *row) {
			latest = row
		}
	}
	return latest
}

func snapshotKeyLess(a, b testopsCoverageSnapshotRow) bool {
	if a.RunID != b.RunID {
		return a.RunID < b.RunID
	}
	return a.SnapshotID < b.SnapshotID
}

// median/mean mirror compute.py's module-level _median/_mean
// (src/dev_health_ops/metrics/compute.py:43,53) -- only these two are
// needed here since testops_risk's own functions never read
// p95_duration_seconds/p95_queue_seconds (compute_testops_risk.py reads
// only success_rate, median_duration_seconds, avg_queue_seconds off a
// PipelineMetricsDailyRecord), so no percentile port is required for
// row-identity on the family this executor actually writes.
func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func trimLower(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func strPtr(value string) *string { return &value }

// pyRound ports Python's round(x, ndigits): correctly-rounded decimal at
// ndigits, ties-to-even, based on the double's EXACT binary value -- not
// "multiply, round half away from zero, divide", which disagrees with
// Python at exact .5 boundaries. strconv.FormatFloat('f', ndigits, 64) is
// Go's own correctly-rounded (round-to-even on exact ties) decimal
// conversion of the same IEEE754 double, so re-parsing it reproduces
// CPython's round() for every finite value tested against it in this
// package's tests. Matches Python's OWN round() (not builtins.round on a
// Decimal), which is what every _clamp/round(..., 4)/round(..., 2) call in
// compute_testops_risk.py and compute_testops.py uses.
func pyRound(value float64, ndigits int) float64 {
	if value != value || value > 1e300 || value < -1e300 { // NaN/huge guard, never expected here
		return value
	}
	formatted := strconv.FormatFloat(value, 'f', ndigits, 64)
	rounded, err := strconv.ParseFloat(formatted, 64)
	if err != nil {
		return value
	}
	return rounded
}

func clampUnit(value float64) float64 {
	if value < 0.0 {
		return 0.0
	}
	if value > 1.0 {
		return 1.0
	}
	return value
}

// factorsJSONField is one key/value pair of a factors_json payload, kept as
// an ordered slice (not a map) because Python's json.dumps on a dict
// preserves INSERTION order while Go's encoding/json on a map sorts keys
// alphabetically -- a real divergence risk against the byte-identical
// row this ticket must prove. isFloat/precision select formatting: Python's
// json.dumps prints a Python int with no decimal point and a Python float
// (even a whole number like 5.0) WITH one, mirrored here by isFloat.
type factorsJSONField struct {
	key      string
	floatVal float64
	intVal   int
	isFloat  bool
}

func ff(key string, value float64) factorsJSONField {
	return factorsJSONField{key: key, floatVal: value, isFloat: true}
}

func fi(key string, value int) factorsJSONField {
	return factorsJSONField{key: key, intVal: value, isFloat: false}
}

// factorsJSON ports json.dumps(factors) as compute_testops_risk.py's two
// factors dicts build it: default separators (", " and ": "), no
// sort_keys, insertion order preserved.
func factorsJSON(fields []factorsJSONField) string {
	var b strings.Builder
	b.WriteByte('{')
	for index, field := range fields {
		if index > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('"')
		b.WriteString(field.key)
		b.WriteString("\": ")
		if field.isFloat {
			b.WriteString(pythonFloatJSON(field.floatVal))
		} else {
			b.WriteString(strconv.Itoa(field.intVal))
		}
	}
	b.WriteByte('}')
	return b.String()
}

// pythonFloatJSON mirrors how Python's json module serializes a float:
// float.__repr__'s shortest round-trip decimal (David Gay's algorithm),
// which is the same well-defined function of a double's bit pattern that
// Go's strconv.FormatFloat(-1 precision, 'g') computes -- both are
// "shortest decimal string that round-trips to this exact double", so they
// agree for every value exercised by this package's tests. The one
// necessary reshaping: Python always keeps a decimal point on a float
// (json.dumps(5.0) -> "5.0", never "5"), and never emits Go's "e+" /
// uppercase exponent form for the small magnitudes these factors take, so a
// bare integer-looking mantissa gets ".0" appended.
func pythonFloatJSON(value float64) string {
	formatted := strconv.FormatFloat(value, 'g', -1, 64)
	if !strings.ContainsAny(formatted, ".eE") {
		formatted += ".0"
	}
	return formatted
}

// -----------------------------------------------------------------------
// Output rows -- ports of ReleaseConfidenceRecord/QualityDragRecord/
// PipelineStabilityRecord (testops_schemas.py:421,440,458).
// -----------------------------------------------------------------------

type testopsReleaseConfidenceRow struct {
	RepoID                uuid.UUID
	Day                   time.Time
	ConfidenceScore       float64
	PipelineSuccessFactor float64
	TestPassFactor        float64
	CoverageFactor        float64
	FlakePenalty          float64
	RegressionPenalty     float64
	FactorsJSON           string
	TeamID                *string
	ServiceID             *string
	OrgID                 string
	ComputedAt            time.Time
}

type testopsQualityDragRow struct {
	RepoID                  uuid.UUID
	Day                     time.Time
	DragHours               float64
	FailureReworkHours      float64
	FlakeInvestigationHours float64
	QueueWaitHours          float64
	RetryOverheadHours      float64
	FactorsJSON             string
	TeamID                  *string
	ServiceID               *string
	OrgID                   string
	ComputedAt              time.Time
}

type testopsPipelineStabilityRow struct {
	RepoID                    uuid.UUID
	Day                       time.Time
	StabilityIndex            float64
	SuccessRate7d             float64
	SuccessRateTrend          float64
	FailureClusteringScore    float64
	MedianRecoveryTimeSeconds *float64
	TeamID                    *string
	ServiceID                 *string
	OrgID                     string
	ComputedAt                time.Time
}

// computeReleaseConfidence ports compute_release_confidence
// (compute_testops_risk.py:24), narrowed to one repo: pipe/test/cov are
// this repo's single representative rows (or nil), matching
// pipe_by_repo.get(repo_id) after Python's dict-overwrite collapse -- see
// this file's package doc comment on WHY a multi-team/service pipeline
// list collapses to its LAST entry. Returns nil when none of pipe/test/cov
// is present, matching Python's repo_ids union check.
func computeReleaseConfidence(
	repoID uuid.UUID, day time.Time, pipe *testopsPipelineMetric, test *testopsTestMetric, cov *testopsCoverageMetric, computedAt time.Time,
) *testopsReleaseConfidenceRow {
	if pipe == nil && test == nil && cov == nil {
		return nil
	}
	var successRate, passRate, flakeRate, failureRecurrence float64
	var coveragePct, coverageDelta float64
	if pipe != nil {
		successRate = pipe.SuccessRate
	}
	if test != nil {
		passRate = test.PassRate
		flakeRate = test.FlakeRate
		failureRecurrence = test.FailureRecurrence
	}
	if cov != nil && cov.LineCoveragePct != nil {
		coveragePct = *cov.LineCoveragePct
	}
	if cov != nil && cov.CoverageDeltaPct != nil {
		coverageDelta = *cov.CoverageDeltaPct
	}

	pipelineFactor := 0.4 * successRate
	testFactor := 0.3 * passRate
	covFactor := 0.2 * clampUnit(coveragePct/100.0)
	flakeFactor := 0.1 * (1.0 - flakeRate)
	baseScore := pipelineFactor + testFactor + covFactor + flakeFactor

	flakePenalty := 0.0
	if flakeRate > 0.05 {
		flakePenalty = 0.1
	}
	regressionPenalty := 0.0
	if coverageDelta < -2.0 {
		regressionPenalty += 0.05
	}
	if failureRecurrence > 0.3 {
		regressionPenalty += 0.1
	}
	score := clampUnit(baseScore - flakePenalty - regressionPenalty)

	factors := factorsJSON([]factorsJSONField{
		ff("pipeline_success_rate", pyRound(successRate, 4)),
		ff("test_pass_rate", pyRound(passRate, 4)),
		ff("coverage_pct", pyRound(coveragePct, 2)),
		ff("flake_rate", pyRound(flakeRate, 4)),
		ff("failure_recurrence", pyRound(failureRecurrence, 4)),
		ff("coverage_delta_pct", pyRound(coverageDelta, 2)),
		ff("base_score", pyRound(baseScore, 4)),
		ff("flake_penalty", pyRound(flakePenalty, 4)),
		ff("regression_penalty", pyRound(regressionPenalty, 4)),
	})

	row := &testopsReleaseConfidenceRow{
		RepoID: repoID, Day: day,
		ConfidenceScore:       pyRound(score, 4),
		PipelineSuccessFactor: pyRound(pipelineFactor, 4),
		TestPassFactor:        pyRound(testFactor, 4),
		CoverageFactor:        pyRound(covFactor, 4),
		FlakePenalty:          pyRound(flakePenalty, 4),
		RegressionPenalty:     pyRound(regressionPenalty, 4),
		FactorsJSON:           factors,
		ComputedAt:            computedAt,
	}
	if pipe != nil {
		row.TeamID, row.ServiceID, row.OrgID = pipe.TeamID, pipe.ServiceID, pipe.OrgID
	} else if test != nil {
		row.TeamID, row.ServiceID, row.OrgID = test.TeamID, test.ServiceID, test.OrgID
	}
	return row
}

// computeQualityDrag ports compute_quality_drag (compute_testops_risk.py:110).
// Unlike release confidence, coverage is never an input and the repo-id
// eligibility set is pipe/test only (compute_testops_risk.py:117-126).
func computeQualityDrag(
	repoID uuid.UUID, day time.Time, pipe *testopsPipelineMetric, test *testopsTestMetric, computedAt time.Time,
) *testopsQualityDragRow {
	if pipe == nil && test == nil {
		return nil
	}
	var medianDur float64
	var failureCount, pipelinesCount int
	var avgQueue, rerunRate float64
	if pipe != nil {
		if pipe.MedianDurationSeconds != nil {
			medianDur = *pipe.MedianDurationSeconds
		}
		failureCount = pipe.FailureCount
		pipelinesCount = pipe.PipelinesCount
		if pipe.AvgQueueSeconds != nil {
			avgQueue = *pipe.AvgQueueSeconds
		}
		rerunRate = pipe.RerunRate
	}
	var flakeRate float64
	var totalCases int
	if test != nil {
		flakeRate = test.FlakeRate
		totalCases = test.TotalCases
	}

	failureReworkHours := float64(failureCount) * medianDur / 3600.0
	flakeInvestigationHours := flakeRate * float64(totalCases) * 0.25
	queueWaitHours := float64(pipelinesCount) * avgQueue / 3600.0
	retryOverheadHours := rerunRate * float64(pipelinesCount) * medianDur / 3600.0
	dragHours := failureReworkHours + flakeInvestigationHours + queueWaitHours + retryOverheadHours

	factors := factorsJSON([]factorsJSONField{
		fi("failure_count", failureCount),
		ff("median_duration_seconds", pyRound(medianDur, 2)),
		fi("pipelines_count", pipelinesCount),
		ff("avg_queue_seconds", pyRound(avgQueue, 2)),
		ff("rerun_rate", pyRound(rerunRate, 4)),
		ff("flake_rate", pyRound(flakeRate, 4)),
		fi("total_cases", totalCases),
	})

	row := &testopsQualityDragRow{
		RepoID: repoID, Day: day,
		DragHours:               pyRound(dragHours, 4),
		FailureReworkHours:      pyRound(failureReworkHours, 4),
		FlakeInvestigationHours: pyRound(flakeInvestigationHours, 4),
		QueueWaitHours:          pyRound(queueWaitHours, 4),
		RetryOverheadHours:      pyRound(retryOverheadHours, 4),
		FactorsJSON:             factors,
		ComputedAt:              computedAt,
	}
	if pipe != nil {
		row.TeamID, row.ServiceID, row.OrgID = pipe.TeamID, pipe.ServiceID, pipe.OrgID
	} else if test != nil {
		row.TeamID, row.ServiceID, row.OrgID = test.TeamID, test.ServiceID, test.OrgID
	}
	return row
}

// computePipelineStability ports compute_pipeline_stability
// (compute_testops_risk.py:185), scoped to ONE repo: dayEntries is every
// pipeline-metric row this repo produced for the partition's day (there can
// be more than one -- see computeTestopsPipelineMetrics's doc comment on
// (team_id, service_id) grouping), in the SAME order
// computeTestopsPipelineMetrics returned them (mirrors Python's stable sort
// by `.day`, which is a no-op tie for same-day rows and so preserves
// pipeline_metrics_buffer's own insertion order). Returns nil when
// dayEntries is empty, matching `if n == 0: continue`.
func computePipelineStability(repoID uuid.UUID, day time.Time, dayEntries []testopsPipelineMetric, computedAt time.Time) *testopsPipelineStabilityRow {
	n := len(dayEntries)
	if n == 0 {
		return nil
	}
	weights := make([]float64, n)
	totalWeight := 0.0
	for i := range dayEntries {
		weights[i] = 1.0 + float64(i)*0.5
		totalWeight += weights[i]
	}
	successRate7d := 0.0
	for i, m := range dayEntries {
		successRate7d += m.SuccessRate * weights[i]
	}
	successRate7d /= totalWeight

	successRateTrend := 0.0
	if n >= 2 {
		xMean := float64(n-1) / 2.0
		ySum := 0.0
		for _, m := range dayEntries {
			ySum += m.SuccessRate
		}
		yMean := ySum / float64(n)
		num := 0.0
		den := 0.0
		for i, m := range dayEntries {
			num += (float64(i) - xMean) * (m.SuccessRate - yMean)
			den += (float64(i) - xMean) * (float64(i) - xMean)
		}
		if den > 0 {
			successRateTrend = num / den
		}
	}

	consecutiveFailures := 0
	totalFailures := 0
	for i, m := range dayEntries {
		if m.FailureCount > 0 {
			totalFailures++
			if i > 0 && dayEntries[i-1].FailureCount > 0 {
				consecutiveFailures++
			}
		}
	}
	failureClustering := 0.0
	if totalFailures > 0 {
		denom := totalFailures
		if denom < 1 {
			denom = 1
		}
		failureClustering = float64(consecutiveFailures) / float64(denom)
	}

	var durations []float64
	for _, m := range dayEntries {
		if m.MedianDurationSeconds != nil && m.FailureCount > 0 {
			durations = append(durations, *m.MedianDurationSeconds)
		}
	}
	var medianRecovery *float64
	if len(durations) > 0 {
		v := median(durations)
		medianRecovery = &v
	}

	trendComponent := successRateTrend
	if trendComponent > 0.1 {
		trendComponent = 0.1
	}
	stability := clampUnit(successRate7d * (1.0 - failureClustering) * (1.0 + trendComponent))

	latest := dayEntries[n-1]
	row := &testopsPipelineStabilityRow{
		RepoID: repoID, Day: day,
		StabilityIndex:         pyRound(stability, 4),
		SuccessRate7d:          pyRound(successRate7d, 4),
		SuccessRateTrend:       pyRound(successRateTrend, 4),
		FailureClusteringScore: pyRound(failureClustering, 4),
		TeamID:                 latest.TeamID,
		ServiceID:              latest.ServiceID,
		OrgID:                  latest.OrgID,
		ComputedAt:             computedAt,
	}
	if medianRecovery != nil {
		v := pyRound(*medianRecovery, 2)
		row.MedianRecoveryTimeSeconds = &v
	}
	return row
}

// -----------------------------------------------------------------------
// Write -- ports write_release_confidence/write_quality_drag/
// write_pipeline_stability (sinks/clickhouse/ci.py:190, wellbeing.py:84,106).
// One PrepareBatch/Send per table, exactly the column order those methods
// insert -- these are plain (non-Replacing) MergeTree tables, so this is a
// pure append, matching the append-only + reader-argMax-dedup contract the
// rest of metrics.daily's native families already use.
// -----------------------------------------------------------------------

func writeTestopsRisk(
	ctx context.Context, conn testopsRiskBatchConn, organizationID string,
	releaseConfidence []testopsReleaseConfidenceRow,
	qualityDrag []testopsQualityDragRow,
	pipelineStability []testopsPipelineStabilityRow,
) (int, error) {
	written := 0
	if len(releaseConfidence) > 0 {
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO testops_release_confidence (
			repo_id, day, confidence_score, pipeline_success_factor, test_pass_factor,
			coverage_factor, flake_penalty, regression_penalty, factors_json,
			team_id, service_id, org_id, computed_at)`)
		if err != nil {
			return written, fmt.Errorf("prepare testops_release_confidence batch: %w", err)
		}
		for _, row := range releaseConfidence {
			if err := batch.Append(
				row.RepoID, chDate(row.Day), row.ConfidenceScore, row.PipelineSuccessFactor,
				row.TestPassFactor, row.CoverageFactor, row.FlakePenalty, row.RegressionPenalty,
				row.FactorsJSON, row.TeamID, row.ServiceID, organizationID, row.ComputedAt.UTC(),
			); err != nil {
				return written, fmt.Errorf("append testops_release_confidence row: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return written, fmt.Errorf("send testops_release_confidence batch: %w", err)
		}
		written += len(releaseConfidence)
	}

	if len(qualityDrag) > 0 {
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO testops_quality_drag (
			repo_id, day, drag_hours, failure_rework_hours, flake_investigation_hours,
			queue_wait_hours, retry_overhead_hours, factors_json,
			team_id, service_id, org_id, computed_at)`)
		if err != nil {
			return written, fmt.Errorf("prepare testops_quality_drag batch: %w", err)
		}
		for _, row := range qualityDrag {
			if err := batch.Append(
				row.RepoID, chDate(row.Day), row.DragHours, row.FailureReworkHours,
				row.FlakeInvestigationHours, row.QueueWaitHours, row.RetryOverheadHours,
				row.FactorsJSON, row.TeamID, row.ServiceID, organizationID, row.ComputedAt.UTC(),
			); err != nil {
				return written, fmt.Errorf("append testops_quality_drag row: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return written, fmt.Errorf("send testops_quality_drag batch: %w", err)
		}
		written += len(qualityDrag)
	}

	if len(pipelineStability) > 0 {
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO testops_pipeline_stability (
			repo_id, day, stability_index, success_rate_7d, success_rate_trend,
			failure_clustering_score, median_recovery_time_seconds,
			team_id, service_id, org_id, computed_at)`)
		if err != nil {
			return written, fmt.Errorf("prepare testops_pipeline_stability batch: %w", err)
		}
		for _, row := range pipelineStability {
			if err := batch.Append(
				row.RepoID, chDate(row.Day), row.StabilityIndex, row.SuccessRate7d,
				row.SuccessRateTrend, row.FailureClusteringScore, row.MedianRecoveryTimeSeconds,
				row.TeamID, row.ServiceID, organizationID, row.ComputedAt.UTC(),
			); err != nil {
				return written, fmt.Errorf("append testops_pipeline_stability row: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return written, fmt.Errorf("send testops_pipeline_stability batch: %w", err)
		}
		written += len(pipelineStability)
	}

	return written, nil
}

func chDate(day time.Time) time.Time {
	return time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
}
