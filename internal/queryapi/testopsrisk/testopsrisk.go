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
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
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
	pipelineStability       *float64
}

// dailyQuery reads, per day, the mean over repositories of the latest
// release confidence and pipeline stability and the sums of the latest
// quality-drag hours, outer-joined on the day.
const dailyQuery = `SELECT
    day AS day,
    release_confidence,
    quality_drag_hours,
    failure_rework_hours,
    flake_investigation_hours,
    queue_wait_hours,
    retry_overhead_hours,
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
          AND day <= {end:Date}
        GROUP BY day, repo_id
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
        sum(retry_overhead_hours) AS retry_overhead_hours
    FROM (
        SELECT
            day,
            repo_id,
            argMax(drag_hours, computed_at) AS drag_hours,
            argMax(failure_rework_hours, computed_at) AS failure_rework_hours,
            argMax(flake_investigation_hours, computed_at) AS flake_investigation_hours,
            argMax(queue_wait_hours, computed_at) AS queue_wait_hours,
            argMax(retry_overhead_hours, computed_at) AS retry_overhead_hours
        FROM testops_quality_drag
        WHERE org_id = {org_id:String}
          AND day >= {start:Date}
          AND day <= {end:Date}
        GROUP BY day, repo_id
    )
    GROUP BY day
) AS drag_daily USING (day)
FULL OUTER JOIN (
    SELECT
        day,
        avg(stability_index) AS pipeline_stability
    FROM (
        SELECT
            day,
            repo_id,
            argMax(stability_index, computed_at) AS stability_index
        FROM testops_pipeline_stability
        WHERE org_id = {org_id:String}
          AND day >= {start:Date}
          AND day <= {end:Date}
        GROUP BY day, repo_id
    )
    GROUP BY day
) AS stability_daily USING (day)
ORDER BY day ASC
SETTINGS join_use_nulls = 1`

// quadrantQuery reads, per repository, the pipeline success rate and test
// pass rate of its latest release-confidence row, least confident first. A
// factor missing from that row's factors_json (absent key or JSON null) is
// NULL, not 0. The Nullable value is wrapped in a tuple because argMax skips
// NULL values, which would let an older row's factor stand in for the latest
// row's missing one.
const quadrantQuery = `SELECT
    coalesce(nullIf(repos.repo, ''), toString(latest.repo_id)) AS repo_label,
    latest.pipeline_success_rate,
    latest.test_pass_rate
FROM (
    SELECT
        repo_id,
        tupleElement(argMax(
            tuple(JSONExtract(factors_json, 'pipeline_success_rate', 'Nullable(Float64)')),
            (day, computed_at)
        ), 1) AS pipeline_success_rate,
        tupleElement(argMax(
            tuple(JSONExtract(factors_json, 'test_pass_rate', 'Nullable(Float64)')),
            (day, computed_at)
        ), 1) AS test_pass_rate,
        argMax(confidence_score, (day, computed_at)) AS confidence_score
    FROM testops_release_confidence
    WHERE org_id = {org_id:String}
      AND day >= {start:Date}
      AND day <= {end:Date}
    GROUP BY repo_id
) AS latest
LEFT JOIN repos
  ON repos.org_id = {org_id:String}
 AND repos.id = latest.repo_id
ORDER BY latest.confidence_score ASC
LIMIT 50`

func readDaily(ctx context.Context, client QueryClient, bindings []clickhouse.Binding) ([]dailyRow, error) {
	rs, err := client.Query(ctx, dailyQuery, bindings)
	if err != nil {
		return nil, fmt.Errorf("testopsrisk: daily query: %w", err)
	}
	defer rs.Close()
	var out []dailyRow
	for rs.Next() {
		var r dailyRow
		var day *time.Time
		if err := rs.Scan(&day, &r.releaseConfidence, &r.qualityDragHours, &r.failureReworkHours,
			&r.flakeInvestigationHours, &r.queueWaitHours, &r.retryOverheadHours, &r.pipelineStability); err != nil {
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

func readQuadrant(ctx context.Context, client QueryClient, bindings []clickhouse.Binding) ([]model.TestOpsRiskQuadrantPoint, error) {
	rs, err := client.Query(ctx, quadrantQuery, bindings)
	if err != nil {
		return nil, fmt.Errorf("testopsrisk: quadrant query: %w", err)
	}
	defer rs.Close()
	out := []model.TestOpsRiskQuadrantPoint{}
	for rs.Next() {
		var label string
		var success, pass *float64
		if err := rs.Scan(&label, &success, &pass); err != nil {
			return nil, fmt.Errorf("testopsrisk: quadrant scan: %w", err)
		}
		out = append(out, model.TestOpsRiskQuadrantPoint{ID: label, PipelineSuccessRate: success, TestPassRate: pass})
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

func orZero(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

// Resolve answers testopsRisk for the org over the inclusive date range.
func Resolve(ctx context.Context, client QueryClient, orgID string, input model.TestOpsRiskInput) (*model.TestOpsRiskResult, error) {
	if client == nil {
		return nil, errors.New("testopsrisk: clickhouse client is required")
	}
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start", Value: input.StartDate.Time().Format(dateLayout)},
		{Name: "end", Value: input.EndDate.Time().Format(dateLayout)},
	}
	rows, err := readDaily(ctx, client, bindings)
	if err != nil {
		return nil, err
	}
	quadrant, err := readQuadrant(ctx, client, bindings)
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
		out.QualityDragBreakdown = []model.TestOpsRiskBreakdownItem{
			{Category: "Failure Rework", Hours: orZero(r.failureReworkHours)},
			{Category: "Flake Investigation", Hours: orZero(r.flakeInvestigationHours)},
			{Category: "Queue Wait", Hours: orZero(r.queueWaitHours)},
			{Category: "Retry Overhead", Hours: orZero(r.retryOverheadHours)},
		}
	}
	return out, nil
}
