// Package testopsrisk serves the testopsRisk GraphQL field: release
// confidence, quality drag and pipeline stability over a date range, built
// from the latest computed value per repository and day of three test-ops
// tables, plus a per-repository quadrant of the latest pipeline success and
// test pass rates.
//
// The org is always the caller's own org.
package testopsrisk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/scopelabel"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/latestrow"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// QueryClient is the narrow ClickHouse boundary this package needs.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

const (
	dateLayout    = "2006-01-02"
	quadrantLimit = 50
)

// dailyRow is one day of the three org-wide daily aggregates; a metric a
// table has no row for that day is nil.
type dailyRow struct {
	day                     time.Time
	releaseConfidence       *float64
	qualityDragHours        *float64
	failureReworkHours      *float64
	flakeInvestigationHours *float64
	queueWaitHours          *float64
	retryOverheadHours      *float64
	partialRows             *uint64
	pipelineStability       *float64
}

// stabilityLiveRows is the one read of stability values. The statement builder names it through this variable:
// the org-scope census follows a package-level name to its literals, and the shared statement is read only by reference.
var stabilityLiveRows = testops.StabilityLiveRowsSQL

// dailyQuery reads, per day, the mean over repositories of the latest
// release confidence and pipeline stability and the sums of the latest
// quality-drag hours, outer-joined on the day. Stability is read only through
// testops.StabilityLiveRowsSQL: a repo-day with no completed run has none.
// The quality-drag table is read FINAL: one whole row per repo-day, the one a
// merge keeps (the last inserted of equal computed_at), so a NULL part of that
// row stays NULL and a merge never changes the answer. A repo-day whose newest
// row of a table retires the day (package liverow) is left out of that table's
// aggregate: the day reads as absent, never 0.
func dailyStatement(scope scopeFilter) string {
	return `SELECT
    day AS day,
    release_confidence,
    quality_drag_hours,
    failure_rework_hours,
    flake_investigation_hours,
    queue_wait_hours,
    retry_overhead_hours,
    partial_rows,
    pipeline_stability
FROM (
    SELECT
        day,
        avg(confidence_score) AS release_confidence
    FROM (
        SELECT
            day,
            repo_id,
            argMax(confidence_score, computed_at) AS confidence_score
        FROM testops_release_confidence
        WHERE org_id = {org_id:String}
          AND day >= {start:Date}
          AND day <= {end:Date}` + scope.and("          ") + `
        GROUP BY day, repo_id
        HAVING ` + liverow.NewestPredicate("testops_release_confidence", "") + `
    )
    GROUP BY day
) AS release_daily
FULL OUTER JOIN (
    SELECT
        day,
        sum(drag_hours) AS quality_drag_hours,
        sum(failure_rework_hours) AS failure_rework_hours,
        sum(flake_investigation_hours) AS flake_investigation_hours,
        sum(queue_wait_hours) AS queue_wait_hours,
        sum(retry_overhead_hours) AS retry_overhead_hours,
        toUInt64(countIf(testops_quality_drag.failure_rework_hours IS NULL
            OR testops_quality_drag.flake_investigation_hours IS NULL
            OR testops_quality_drag.queue_wait_hours IS NULL
            OR testops_quality_drag.retry_overhead_hours IS NULL)) AS partial_rows
    -- the columns of countIf are table-qualified: an output alias of a sum above would otherwise be read inside it (code 184)
    FROM testops_quality_drag FINAL
    WHERE org_id = {org_id:String}
      AND day >= {start:Date}
      AND day <= {end:Date}` + scope.and("      ") + `
      AND ` + liverow.Predicate("testops_quality_drag", "") + `
    GROUP BY day
) AS drag_daily USING (day)
FULL OUTER JOIN (
    SELECT
        day,
        avg(stability_index) AS pipeline_stability
    FROM (
` + stabilityLiveRows + `
    )` + scope.where("    ") + `
    GROUP BY day
) AS stability_daily USING (day)
ORDER BY day ASC
SETTINGS join_use_nulls = 1`
}

var dailyQuery = dailyStatement(scopeFilter{})

// quadrantQuery reads, per repository, the pipeline success rate and test
// pass rate of its latest release-confidence row, least confident first: the
// newest row of each repo-day, without the repo-days whose newest row retires
// the day (package liverow), then the latest day of each repository. A
// factor missing from that row's factors_json (absent key or JSON null) is
// NULL, not 0. The Nullable value is wrapped in a tuple because argMax skips
// NULL values, which would let an older row's factor stand in for the latest
// row's missing one.
func quadrantStatement(scope scopeFilter) string {
	return fmt.Sprintf(`SELECT
    coalesce(nullIf(repos.repo, ''), toString(latest.repo_id)) AS repo_label,
    nullIf(repos.repo, '') AS repo_name,
    latest.pipeline_success_rate,
    latest.test_pass_rate
FROM (
    SELECT
        repo_id,
        tupleElement(argMax(tuple(day_pipeline_success_rate), day), 1) AS pipeline_success_rate,
        tupleElement(argMax(tuple(day_test_pass_rate), day), 1) AS test_pass_rate,
        argMax(day_confidence_score, day) AS confidence_score
    FROM (
        SELECT
            repo_id,
            day,
            tupleElement(argMax(
                tuple(JSONExtract(factors_json, 'pipeline_success_rate', 'Nullable(Float64)')),
                computed_at
            ), 1) AS day_pipeline_success_rate,
            tupleElement(argMax(
                tuple(JSONExtract(factors_json, 'test_pass_rate', 'Nullable(Float64)')),
                computed_at
            ), 1) AS day_test_pass_rate,
            argMax(confidence_score, computed_at) AS day_confidence_score
        FROM testops_release_confidence
        WHERE org_id = {org_id:String}
          AND day >= {start:Date}
          AND day <= {end:Date}%s
        GROUP BY repo_id, day
        HAVING %s
    )
    GROUP BY repo_id
) AS latest
LEFT JOIN (
    SELECT id, %s AS repo
    FROM repos
    WHERE org_id = {org_id:String}
    GROUP BY id
) AS repos
  ON repos.id = latest.repo_id
ORDER BY latest.confidence_score ASC
LIMIT 50`, scope.and("          "), liverow.NewestPredicate("testops_release_confidence", ""), latestrow.ArgMaxKeepNullBy("repo", "last_synced"))
}

var quadrantQuery = quadrantStatement(scopeFilter{})

// scopeFilter is the repository scope of a request: the AND-joined conditions on a repo_id column and their
// bindings. The zero value is no scope, and renders nothing, so a statement with no scope is the text it was
// before the scope existed.
type scopeFilter struct {
	conditions []string
	bindings   []clickhouse.Binding
}

// newScopeFilter builds the scope of repoIds (ids or full names). An id selects the risk rows that carry it, whether or
// not the catalog holds the repository (each read scopes org_id itself, and the quadrant names such a repository by its
// id); a full name selects the repository that holds it in the latest stored version of its catalog row (FINAL), and a
// blank name is none, so a list of blank repository ids matches no repository. teamIds are the repositories these teams
// own (teamscope.RepoCondition). A nil or empty list is no scope; a blank team id is dropped by teamscope, so a list
// of blank team ids is no team scope.
func newScopeFilter(orgID string, repoIDs, teamIDs []string, asOf time.Time) scopeFilter {
	var f scopeFilter
	if len(repoIDs) > 0 {
		f.conditions = append(f.conditions, `(toString(repo_id) IN {repo_ids:Array(String)}
              OR repo_id IN (
                  SELECT id FROM repos FINAL
                  WHERE org_id = {org_id:String}
                    AND repo != ''
                    AND repo IN {repo_ids:Array(String)}
              ))`)
		f.bindings = append(f.bindings, clickhouse.Binding{Name: "repo_ids", Value: repoIDs})
	}
	if condition, bindings := teamscope.RepoCondition(orgID, "toString(repo_id)", teamIDs, asOf); condition != "" {
		f.conditions = append(f.conditions, condition)
		f.bindings = append(f.bindings, bindings...)
	}
	return f
}

// and renders the conditions as AND clauses, each on a line of its own.
func (f scopeFilter) and(indent string) string {
	var b strings.Builder
	for _, c := range f.conditions {
		b.WriteString("\n" + indent + "AND " + c)
	}
	return b.String()
}

// where renders the conditions as one WHERE clause.
func (f scopeFilter) where(indent string) string {
	if len(f.conditions) == 0 {
		return ""
	}
	return "\n" + indent + "WHERE " + strings.Join(f.conditions, "\n"+indent+"  AND ")
}

func readDaily(ctx context.Context, client QueryClient, scope scopeFilter, bindings []clickhouse.Binding) ([]dailyRow, error) {
	statement := dailyQuery
	if len(scope.conditions) > 0 {
		statement = dailyStatement(scope)
		bindings = append(bindings[:len(bindings):len(bindings)], scope.bindings...)
	}
	rs, err := client.Query(ctx, statement, bindings)
	if err != nil {
		return nil, fmt.Errorf("testopsrisk: daily query: %w", err)
	}
	defer rs.Close()
	var out []dailyRow
	for rs.Next() {
		var r dailyRow
		var day *time.Time
		if err := rs.Scan(&day, &r.releaseConfidence, &r.qualityDragHours, &r.failureReworkHours,
			&r.flakeInvestigationHours, &r.queueWaitHours, &r.retryOverheadHours, &r.partialRows, &r.pipelineStability); err != nil {
			return nil, fmt.Errorf("testopsrisk: daily scan: %w", err)
		}
		if day == nil {
			continue
		}
		r.day = *day
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("testopsrisk: daily rows: %w", err)
	}
	return out, nil
}

func readQuadrant(ctx context.Context, client QueryClient, scope scopeFilter, bindings []clickhouse.Binding) ([]model.TestOpsRiskQuadrantPoint, error) {
	statement := quadrantQuery
	if len(scope.conditions) > 0 {
		statement = quadrantStatement(scope)
		bindings = append(bindings[:len(bindings):len(bindings)], scope.bindings...)
	}
	rs, err := client.Query(ctx, statement, bindings)
	if err != nil {
		return nil, fmt.Errorf("testopsrisk: quadrant query: %w", err)
	}
	defer rs.Close()
	out := []model.TestOpsRiskQuadrantPoint{}
	for rs.Next() {
		var label string
		var rawName *string
		var success, pass *float64
		if err := rs.Scan(&label, &rawName, &success, &pass); err != nil {
			return nil, fmt.Errorf("testopsrisk: quadrant scan: %w", err)
		}
		var name *string
		if rawName != nil {
			if clean, ok := scopelabel.CleanName(*rawName); ok {
				name = &clean
			}
		}
		out = append(out, model.TestOpsRiskQuadrantPoint{ID: label, Name: name, PipelineSuccessRate: success, TestPassRate: pass})
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("testopsrisk: quadrant rows: %w", err)
	}
	return out, nil
}

// clamp01 clamps to [0, 1] with min/max comparisons that keep the first
// argument unless the second is strictly better, so a NaN input reads as 1.
func clamp01(v float64) float64 {
	hi := 1.0
	if v < hi {
		hi = v
	}
	lo := 0.0
	if hi > lo {
		lo = hi
	}
	return lo
}

// delta is the change from the first to the last point as a percentage of
// the first point's magnitude; absent with fewer than two points or a zero
// first value.
func delta(points []model.TestOpsRiskSparkPoint) *float64 {
	if len(points) < 2 {
		return nil
	}
	previous, current := points[0].Value, points[len(points)-1].Value
	if previous == 0 {
		return nil
	}
	abs := previous
	if abs < 0 {
		abs = -abs
	}
	d := (current - previous) / abs * 100
	return &d
}

// latestWith is the newest row for which pick yields a value.
func latestWith(rows []dailyRow, pick func(dailyRow) *float64) *dailyRow {
	for i := len(rows) - 1; i >= 0; i-- {
		if pick(rows[i]) != nil {
			return &rows[i]
		}
	}
	return nil
}

type breakdownPart struct {
	category string
	hours    *float64
}

// breakdownItems lists the parts that have a sum; a part whose sum is absent
// (every summed row NULL) is omitted, never shown as 0.
func breakdownItems(parts ...breakdownPart) []model.TestOpsRiskBreakdownItem {
	items := []model.TestOpsRiskBreakdownItem{}
	for _, p := range parts {
		if p.hours != nil {
			items = append(items, model.TestOpsRiskBreakdownItem{Category: p.category, Hours: *p.hours})
		}
	}
	return items
}

// Resolve answers testopsRisk for the org over the inclusive date range, narrowed to the repositories of
// input.RepoIds and the repositories input.TeamIds own (as of now). A scope never widens: the org_id predicate of
// every read stays.
func Resolve(ctx context.Context, client QueryClient, orgID string, input model.TestOpsRiskInput) (*model.TestOpsRiskResult, error) {
	if client == nil {
		return nil, errors.New("testopsrisk: clickhouse client is required")
	}
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start", Value: input.StartDate.Time().Format(dateLayout)},
		{Name: "end", Value: input.EndDate.Time().Format(dateLayout)},
	}
	scope := newScopeFilter(orgID, input.RepoIds, input.TeamIds, time.Now().UTC())
	rows, err := readDaily(ctx, client, scope, bindings)
	if err != nil {
		return nil, err
	}
	quadrant, err := readQuadrant(ctx, client, scope, bindings)
	if err != nil {
		return nil, err
	}

	confidence := func(r dailyRow) *float64 { return r.releaseConfidence }
	drag := func(r dailyRow) *float64 { return r.qualityDragHours }
	stability := func(r dailyRow) *float64 { return r.pipelineStability }

	timeseries := []model.TestOpsRiskTrendPoint{}
	confidenceSpark := []model.TestOpsRiskSparkPoint{}
	dragSpark := []model.TestOpsRiskSparkPoint{}
	stabilitySpark := []model.TestOpsRiskSparkPoint{}
	for _, r := range rows {
		if r.releaseConfidence != nil {
			risk := clamp01(1.0 - *r.releaseConfidence)
			timeseries = append(timeseries, model.TestOpsRiskTrendPoint{Date: graphqldate.New(r.day), RiskScore: risk})
			confidenceSpark = append(confidenceSpark, model.TestOpsRiskSparkPoint{Ts: graphqldate.New(r.day), Value: (1.0 - risk) * 100.0})
		}
		if r.qualityDragHours != nil {
			dragSpark = append(dragSpark, model.TestOpsRiskSparkPoint{Ts: graphqldate.New(r.day), Value: *r.qualityDragHours})
		}
		if r.pipelineStability != nil {
			stabilitySpark = append(stabilitySpark, model.TestOpsRiskSparkPoint{Ts: graphqldate.New(r.day), Value: *r.pipelineStability * 100.0})
		}
	}

	out := &model.TestOpsRiskResult{
		OrgID:                orgID,
		Timeseries:           timeseries,
		QualityDragBreakdown: []model.TestOpsRiskBreakdownItem{},
		QuadrantData:         quadrant,
		ConfidenceSpark:      confidenceSpark,
		ConfidenceDelta:      delta(confidenceSpark),
		DragSpark:            dragSpark,
		DragDelta:            delta(dragSpark),
		StabilitySpark:       stabilitySpark,
		StabilityDelta:       delta(stabilitySpark),
	}
	if r := latestWith(rows, confidence); r != nil {
		out.ReleaseConfidence = r.releaseConfidence
	}
	if r := latestWith(rows, stability); r != nil {
		out.PipelineStability = r.pipelineStability
	}
	if r := latestWith(rows, drag); r != nil {
		out.QualityDragHours = r.qualityDragHours
		out.QualityDragPartial = r.partialRows != nil && *r.partialRows > 0
		out.QualityDragBreakdown = breakdownItems(
			breakdownPart{"Failure Rework", r.failureReworkHours},
			breakdownPart{"Flake Investigation", r.flakeInvestigationHours},
			breakdownPart{"Queue Wait", r.queueWaitHours},
			breakdownPart{"Retry Overhead", r.retryOverheadHours},
		)
	}
	return out, nil
}
