// Package testops ports the three metrics.daily families
// `testops_pipeline`/`testops_test`/`testops_coverage` compute
// (`src/dev_health_ops/metrics/compute_testops.py`) into pure, exported Go
// functions with no ClickHouse dependency -- callers own loading raw rows
// and writing output (day/computed_at are therefore NOT part of the output
// structs here, matching internal/jobs/metrics/numerical's convention: a
// writer stamps those at persist time, same as
// numerical.TeamWellbeingMetric).
//
// # Provenance and current callers (CHAOS-4294)
//
// This package exists first as CHAOS-4294's (testops_risk) own input
// recompute: TestopsRiskExecutor (internal/jobs/metrics/daily) needs the
// SAME pipeline/test/coverage aggregation Python's job_daily.py computes
// in-process a few lines before feeding testops_risk's own three
// risk-model functions, because Go's native families run BEFORE the
// Python compatibility bridge each partition -- see
// internal/jobs/metrics/daily/testops_risk_native_clickhouse.go's package
// doc comment for the full "why". CHAOS-4294 does NOT register these
// functions as native families and does NOT write
// testops_{pipeline,test,coverage}_metrics_daily -- those tables and their
// own families.json entries stay Python-bridge/"pending" (CHAOS-4284).
//
// team-lead's ruling (2026-09-01): these functions are exported here, and
// return the FULL Python record shape (not just the subset testops_risk
// itself reads), specifically so CHAOS-4284's eventual native cutover of
// testops_pipeline/testops_test/testops_coverage can import and reuse them
// verbatim -- write a ClickHouse loader + writer around them -- instead of
// re-porting the same Python source a second time. If you are picking up
// CHAOS-4284: the compute already exists here; do not re-derive it.
//
// # Fidelity
//
// The Python authority is src/dev_health_ops/metrics/compute_testops.py's
// compute_pipeline_metrics_daily / compute_test_metrics_daily /
// compute_coverage_metrics_daily. job_runs is accepted by the Python
// pipeline function only to be discarded immediately
// (compute_testops.py:123) and is therefore not a parameter here at all.
// See each function's own doc comment for further fidelity notes
// (grouping keys, tie-break order, etc).
package testops

import (
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// -----------------------------------------------------------------------
// Raw row shapes (ports of the TypedDicts in testops_schemas.py, narrowed
// to the columns compute_testops.py actually reads).
// -----------------------------------------------------------------------

// PipelineRunRow ports PipelineRunExtendedRow (testops_schemas.py:64).
type PipelineRunRow struct {
	RepoID          uuid.UUID
	Status          *string
	QueuedAt        *time.Time
	StartedAt       time.Time
	FinishedAt      *time.Time
	DurationSeconds *float64
	QueueSeconds    *float64
	RetryCount      uint32
	TeamID          *string
	ServiceID       *string
	OrgID           string
}

// SuiteRow ports TestSuiteResultRow (testops_schemas.py:145).
type SuiteRow struct {
	RepoID           uuid.UUID
	RunID            string
	SuiteID          string
	TotalCount       uint32
	PassedCount      uint32
	FailedCount      uint32
	SkippedCount     uint32
	ErrorCount       uint32
	QuarantinedCount uint32
	DurationSeconds  *float64
	StartedAt        *time.Time
	FinishedAt       *time.Time
	TeamID           *string
	ServiceID        *string
	OrgID            string
}

// CaseRow ports TestCaseResultRow (testops_schemas.py:174), narrowed to
// the fields compute_test_metrics_daily reads (CHAOS-4350 already
// projected the Python loader down to this same narrow shape).
type CaseRow struct {
	RepoID       uuid.UUID
	RunID        string
	SuiteID      string
	CaseName     string
	Status       *string
	RetryAttempt uint32
}

// CoverageSnapshotRow ports CoverageSnapshotRow (testops_schemas.py:206),
// narrowed to the columns compute_coverage_metrics_daily reads.
type CoverageSnapshotRow struct {
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
// Output shapes -- full ports of PipelineMetricsDailyRecord/
// TestMetricsDailyRecord/CoverageMetricsDailyRecord (testops_schemas.py),
// minus day/computed_at (caller concern, see package doc comment).
// -----------------------------------------------------------------------

// PipelineMetric ports PipelineMetricsDailyRecord (testops_schemas.py:339).
type PipelineMetric struct {
	RepoID                uuid.UUID
	PipelinesCount        int
	SuccessCount          int
	FailureCount          int
	CancelledCount        int
	SuccessRate           float64
	FailureRate           float64
	CancelRate            float64
	RerunRate             float64
	MedianDurationSeconds *float64
	P95DurationSeconds    *float64
	AvgQueueSeconds       *float64
	P95QueueSeconds       *float64
	TeamID                *string
	ServiceID             *string
	OrgID                 string
}

// TestMetric ports TestMetricsDailyRecord (testops_schemas.py:366).
type TestMetric struct {
	RepoID                  uuid.UUID
	TotalCases              int
	PassedCount             int
	FailedCount             int
	SkippedCount            int
	QuarantinedCount        int
	PassRate                float64
	FailureRate             float64
	FlakeRate               float64
	RetryDependencyRate     float64
	TotalSuites             int
	SuiteDurationP50Seconds *float64
	SuiteDurationP95Seconds *float64
	FailureRecurrence       float64
	TeamID                  *string
	ServiceID               *string
	OrgID                   string
}

// CoverageMetric ports CoverageMetricsDailyRecord (testops_schemas.py:394).
// UncoveredFilesCount/CoverageRegressionCount are always 0 -- Python's own
// compute_coverage_metrics_daily hardcodes both to 0 (v1 has no per-file
// coverage input, see testops_schemas.py's module doc comment: "v1:
// aggregate ... coverage. Changed-code coverage deferred to v2"), so this
// is not a Go-side gap.
type CoverageMetric struct {
	RepoID                  uuid.UUID
	LineCoveragePct         *float64
	BranchCoveragePct       *float64
	LinesTotal              *uint32
	LinesCovered            *uint32
	CoverageDeltaPct        *float64
	UncoveredFilesCount     int
	CoverageRegressionCount int
	TeamID                  *string
	ServiceID               *string
	OrgID                   string
}

func derefStr(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func strPtr(value string) *string { return &value }

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

// percentile mirrors compute.py's module-level _percentile: linear
// interpolation between closest ranks. Returns 0 for an empty slice.
func percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	switch {
	case pct <= 0:
		return minFloat(values)
	case pct >= 100:
		return maxFloat(values)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	// float64(...) on rank is load-bearing (CHAOS-4818): sort.Float64s above
	// can push the compiler to rematerialize rank after the call instead of
	// reusing the already-rounded value, fusing that recomputation with the
	// next statement's subtraction (frac := rank - float64(lo)) into one
	// FNMSUBD on arm64 -- fusion "across statements", not just within one.
	rank := float64(float64(len(sorted)-1) * (pct / 100.0))
	lo := int(rank)
	hi := lo + 1
	if hi > len(sorted)-1 {
		hi = len(sorted) - 1
	}
	frac := rank - float64(lo)
	// float64(...) around each product prevents Go from fusing this into one
	// FMA on arm64, which would round differently than CPython's
	// compute._percentile (CHAOS-4818).
	return float64(sorted[lo]*(1-frac)) + float64(sorted[hi]*frac)
}

func minFloat(values []float64) float64 {
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}

func maxFloat(values []float64) float64 {
	result := values[0]
	for _, value := range values[1:] {
		if value > result {
			result = value
		}
	}
	return result
}

func trimLower(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
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

var failureStatuses = map[string]struct{}{
	"failure": {}, "failed": {}, "error": {}, "errors": {}, "timeout": {}, "timed_out": {},
}

func normalizeTestStatus(status *string) string {
	normalized := trimLower(derefStr(status))
	switch normalized {
	case "success", "succeeded", "passed":
		return "passed"
	default:
		if _, ok := failureStatuses[normalized]; ok {
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

// RepoTeamResolver resolves a team from a repo's full name -- the same
// shape internal/jobs/metrics/numerical.RepoTeamResolver already declares
// (and internal/jobs/metrics/daily's repoPatternResolver already
// implements, via NewRepoPatternResolver), redeclared locally so this
// package stays free of a hard dependency on either -- Go's structural
// typing means any caller's existing resolver satisfies this without an
// adapter. An empty repoName, a nil resolver, or no match must all resolve
// to ("", "").
type RepoTeamResolver interface {
	ResolveRepo(repoName string) (teamID, teamName string)
}

// resolveRepoTeam ports _resolve_repo_team (compute_testops.py:24) exactly,
// including its truthy check: a FALSY raw team_id -- Python's `if
// row_team_id:`, true for both None and "" -- falls back to repo-pattern
// resolution, not just a nil one. This is the reason ComputePipelineMetrics
// never needs to distinguish a raw team_id of nil from "" in its grouping
// key (both take the identical fallback path and land on the identical
// resolved value), while ComputeServiceID-equivalent handling for
// service_id -- which Python never resolves this way -- still must (see
// ComputePipelineMetrics's own doc comment). Returns nil (Python's None)
// when there is nothing to resolve to, never a pointer to "".
func resolveRepoTeam(rawTeamID *string, repoName string, resolver RepoTeamResolver) *string {
	if rawTeamID != nil && *rawTeamID != "" {
		return rawTeamID
	}
	if resolver == nil {
		return nil
	}
	teamID, _ := resolver.ResolveRepo(repoName)
	if teamID == "" {
		return nil
	}
	return &teamID
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

// ComputePipelineMetrics ports compute_pipeline_metrics_daily
// (compute_testops.py:114), narrowed to ONE repo -- callers scope `rows`
// to one (org, repo, day) themselves (this package has no ClickHouse
// dependency and does no filtering). repoName/resolver are exactly what
// _resolve_repo_team needs (job_daily.py passes its own repo_team_resolver
// and repo_names_by_id straight through) -- pass "", nil when there is no
// resolver available; that degrades to "never resolve", matching Python's
// own `repo_team_resolver is None` branch.
//
// Groups by (resolved team_id, RAW service_id) within that repo, exactly
// like Python: a single repo/day CAN return more than one row when its
// rows disagree on team/service. Two subtleties, both confirmed against
// compute_testops.py and both caught by codex adversarial review round 1
// (CHAOS-4294) against an earlier revision that got both wrong:
//
//  1. team_id in the grouping key is the RESOLVED value
//     (`_resolve_repo_team(repo_id, row.get("team_id"), ...)`,
//     compute_testops.py:135), not the raw row value -- a raw team_id of
//     None and "" are NOT distinct groups, because _resolve_repo_team's own
//     truthy check (`if row_team_id:`) treats both as "absent" and applies
//     the identical repo-pattern fallback to both.
//  2. service_id in the grouping key is the RAW row value
//     (`service_id = row.get("service_id")`, compute_testops.py:138) with
//     NO resolution step at all -- unlike team_id, a raw service_id of
//     None and "" ARE genuinely distinct Python dict keys and must produce
//     two separate output rows, not one merged row. serviceSet below
//     carries that distinction (a plain string key would silently collapse
//     a nil ServiceID and a non-nil empty one into the same group).
//
// Returned in sorted (team_id, service_id) order -- both `or ""` for the
// sort comparison only, matching Python's
// `sorted(by_group.items(), key=lambda item: (str(item[0][0]), item[0][1] or "", item[0][2] or ""))`
// -- with a STABLE sort so two groups whose sort keys tie (e.g. a nil vs a
// "" service_id, both sorting as "") keep Python's dict's own
// insertion-order tie-break instead of an arbitrary one.
func ComputePipelineMetrics(repoID uuid.UUID, rows []PipelineRunRow, repoName string, resolver RepoTeamResolver) []PipelineMetric {
	type bucket struct {
		pipelines, success, failure, cancelled, reruns int
		durations, queues                              []float64
		orgID                                          string
	}
	type key struct {
		teamID       string // "" means unresolved (Python None) -- see doc comment: never ambiguous with a real value post-resolution.
		serviceSet   bool
		serviceValue string
	}
	byGroup := make(map[key]*bucket)
	var order []key
	for _, row := range rows {
		resolvedTeam := resolveRepoTeam(row.TeamID, repoName, resolver)
		k := key{teamID: derefStr(resolvedTeam)}
		if row.ServiceID != nil {
			k.serviceSet = true
			k.serviceValue = *row.ServiceID
		}
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
		if d := safeDurationSeconds(row.QueuedAt, row.StartedAt, row.FinishedAt, row.DurationSeconds, false); d != nil {
			b.durations = append(b.durations, *d)
		}
		if q := safeDurationSeconds(row.QueuedAt, row.StartedAt, row.FinishedAt, row.QueueSeconds, true); q != nil {
			b.queues = append(b.queues, *q)
		}
	}
	sort.SliceStable(order, func(i, j int) bool {
		if order[i].teamID != order[j].teamID {
			return order[i].teamID < order[j].teamID
		}
		return order[i].serviceValue < order[j].serviceValue
	})
	result := make([]PipelineMetric, 0, len(order))
	for _, k := range order {
		b := byGroup[k]
		metric := PipelineMetric{
			RepoID: repoID, OrgID: b.orgID,
			PipelinesCount: b.pipelines, SuccessCount: b.success,
			FailureCount: b.failure, CancelledCount: b.cancelled,
		}
		if b.pipelines > 0 {
			n := float64(b.pipelines)
			metric.SuccessRate = float64(b.success) / n
			metric.FailureRate = float64(b.failure) / n
			metric.CancelRate = float64(b.cancelled) / n
			metric.RerunRate = float64(b.reruns) / n
		}
		if len(b.durations) > 0 {
			m := median(b.durations)
			p95 := percentile(b.durations, 95.0)
			metric.MedianDurationSeconds = &m
			metric.P95DurationSeconds = &p95
		}
		if len(b.queues) > 0 {
			avg := mean(b.queues)
			p95 := percentile(b.queues, 95.0)
			metric.AvgQueueSeconds = &avg
			metric.P95QueueSeconds = &p95
		}
		if k.teamID != "" {
			metric.TeamID = strPtr(k.teamID)
		}
		if k.serviceSet {
			metric.ServiceID = strPtr(k.serviceValue)
		}
		result = append(result, metric)
	}
	return result
}

// ComputeTestMetrics ports compute_test_metrics_daily
// (compute_testops.py:216), narrowed to one repo: suites/cases are
// expected pre-scoped to one (org, repo, day) by the caller (mirrors
// Python's own contract post-CHAOS-4350 -- see that function's docstring).
// historicalFailedNames is the CHAOS-4350-PR2 SQL-aggregate input
// (load_testops_historical_failed_case_names), a set of case names that
// failed OUTSIDE today's runs -- callers own deriving it. A repo with
// neither suites nor cases returns nil, matching Python's
// `if not repo_suites and not repo_cases: continue`. repoName/resolver
// resolve the OUTPUT team_id via _resolve_repo_team
// (compute_testops.py:357-362) -- same fallback ComputePipelineMetrics
// applies to its grouping key, here applied only to the single
// representative row's team_id, never as a grouping key (this function
// does not group by team at all).
func ComputeTestMetrics(
	repoID uuid.UUID, suites []SuiteRow, cases []CaseRow, historicalFailedNames map[string]struct{},
	repoName string, resolver RepoTeamResolver,
) []TestMetric {
	if len(suites) == 0 && len(cases) == 0 {
		return nil
	}
	var totalCases, passedCount, failedCount, skippedCount, quarantinedCount int
	var suiteDurations []float64
	for _, s := range suites {
		totalCases += int(s.TotalCount)
		passedCount += int(s.PassedCount)
		failedCount += int(s.FailedCount) + int(s.ErrorCount)
		skippedCount += int(s.SkippedCount)
		quarantinedCount += int(s.QuarantinedCount)
		if s.DurationSeconds != nil && *s.DurationSeconds >= 0 {
			suiteDurations = append(suiteDurations, *s.DurationSeconds)
		}
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

	var first *SuiteRow
	if len(suites) > 0 {
		first = &suites[0]
	}
	metric := TestMetric{
		RepoID: repoID, TotalCases: totalCases, PassedCount: passedCount,
		FailedCount: failedCount, SkippedCount: skippedCount, QuarantinedCount: quarantinedCount,
		TotalSuites: len(suites),
	}
	if totalCases > 0 {
		metric.PassRate = float64(passedCount) / float64(totalCases)
		metric.FailureRate = float64(failedCount) / float64(totalCases)
	}
	if distinctCases > 0 {
		metric.FlakeRate = float64(flakeCases) / float64(distinctCases)
		metric.RetryDependencyRate = float64(retryDependentCases) / float64(distinctCases)
	}
	if len(suiteDurations) > 0 {
		p50 := median(suiteDurations)
		p95 := percentile(suiteDurations, 95.0)
		metric.SuiteDurationP50Seconds = &p50
		metric.SuiteDurationP95Seconds = &p95
	}
	if len(currentFailedNames) > 0 {
		metric.FailureRecurrence = float64(recurrentFailures) / float64(len(currentFailedNames))
	}
	if first != nil {
		metric.TeamID = resolveRepoTeam(first.TeamID, repoName, resolver)
		metric.ServiceID = first.ServiceID
		metric.OrgID = first.OrgID
	}
	return []TestMetric{metric}
}

// ComputeCoverageMetric ports compute_coverage_metrics_daily
// (compute_testops.py:371), narrowed to one repo: the latest (by
// (run_id, snapshot_id) lexical order, matching Python's tuple-comparison
// tie-break) current-window snapshot and, if present, the latest
// prior-window snapshot for the coverage delta. Returns nil when `current`
// has no rows for this repo, matching Python's `latest_current_by_repo`
// lookup miss. repoName/resolver resolve the OUTPUT team_id via
// _resolve_repo_team (compute_testops.py:438-443), same as
// ComputeTestMetrics.
func ComputeCoverageMetric(repoID uuid.UUID, current, prior []CoverageSnapshotRow, repoName string, resolver RepoTeamResolver) *CoverageMetric {
	latest := latestSnapshot(current)
	if latest == nil {
		return nil
	}
	priorLatest := latestSnapshot(prior)
	metric := &CoverageMetric{
		RepoID: repoID, LineCoveragePct: latest.LineCoveragePct, BranchCoveragePct: latest.BranchCoveragePct,
		LinesTotal: latest.LinesTotal, LinesCovered: latest.LinesCovered,
		TeamID: resolveRepoTeam(latest.TeamID, repoName, resolver), ServiceID: latest.ServiceID, OrgID: latest.OrgID,
	}
	if latest.LineCoveragePct != nil && priorLatest != nil && priorLatest.LineCoveragePct != nil {
		delta := *latest.LineCoveragePct - *priorLatest.LineCoveragePct
		metric.CoverageDeltaPct = &delta
	}
	return metric
}

func latestSnapshot(rows []CoverageSnapshotRow) *CoverageSnapshotRow {
	var latest *CoverageSnapshotRow
	for index := range rows {
		row := &rows[index]
		if latest == nil || snapshotKeyLess(*latest, *row) {
			latest = row
		}
	}
	return latest
}

func snapshotKeyLess(a, b CoverageSnapshotRow) bool {
	if a.RunID != b.RunID {
		return a.RunID < b.RunID
	}
	return a.SnapshotID < b.SnapshotID
}
