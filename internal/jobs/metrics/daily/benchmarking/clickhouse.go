package benchmarking

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// conn is the narrow ClickHouse capability this package needs, matching
// internal/jobs/metrics/daily/cicd's conn interface shape.
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// metricDefinitions ports _common.py's METRIC_DEFINITIONS (:46-156) verbatim.
// The value column, the grouping columns and the supported scopes are all part
// of the query's meaning, so they are reproduced rather than re-derived.
var metricDefinitions = map[string]MetricDefinition{
	"success_rate":              pipelineMetric("success_rate"),
	"failure_rate":              pipelineMetric("failure_rate"),
	"rerun_rate":                pipelineMetric("rerun_rate"),
	"median_duration_seconds":   pipelineMetric("median_duration_seconds"),
	"p95_duration_seconds":      pipelineMetric("p95_duration_seconds"),
	"avg_queue_seconds":         pipelineMetric("avg_queue_seconds"),
	"p95_queue_seconds":         pipelineMetric("p95_queue_seconds"),
	"pass_rate":                 testMetric("pass_rate"),
	"flake_rate":                testMetric("flake_rate"),
	"retry_dependency_rate":     testMetric("retry_dependency_rate"),
	"failure_recurrence_score":  testMetric("failure_recurrence_score"),
	"line_coverage_pct":         coverageMetric("line_coverage_pct"),
	"branch_coverage_pct":       coverageMetric("branch_coverage_pct"),
	"coverage_delta_pct":        coverageMetric("coverage_delta_pct"),
	"coverage_regression_count": coverageMetric("coverage_regression_count"),
	"cycle_time_hours": {
		Table:             "work_item_metrics_daily",
		ValueColumn:       "cycle_time_p50_hours",
		ScopeSupport:      map[string]bool{ScopeTeam: true, ScopeGlobal: true},
		InnerGroupColumns: []string{"team_id", "work_scope_id", "provider"},
	},
	"defect_intro_rate": {
		Table:             "work_item_metrics_daily",
		ValueColumn:       "defect_intro_rate",
		ScopeSupport:      map[string]bool{ScopeTeam: true, ScopeGlobal: true},
		InnerGroupColumns: []string{"team_id", "work_scope_id", "provider"},
	},
	"deployment_frequency": {
		Table:             "dora_metrics_daily",
		ValueColumn:       "value",
		ScopeSupport:      map[string]bool{ScopeRepo: true, ScopeGlobal: true},
		InnerGroupColumns: []string{"repo_id"},
		// dora_metrics_daily holds many metrics in one table, so this one must
		// select itself by name.
		ExtraFilters: []string{"metric_name = {metric_name:String}"},
	},
}

func testopsScopes() map[string]bool {
	return map[string]bool{ScopeRepo: true, ScopeTeam: true, ScopeGlobal: true}
}

func pipelineMetric(column string) MetricDefinition {
	return MetricDefinition{
		Table: "testops_pipeline_metrics_daily", ValueColumn: column,
		ScopeSupport: testopsScopes(), InnerGroupColumns: []string{"repo_id", "team_id", "service_id"},
	}
}

func testMetric(column string) MetricDefinition {
	return MetricDefinition{
		Table: "testops_test_metrics_daily", ValueColumn: column,
		ScopeSupport: testopsScopes(), InnerGroupColumns: []string{"repo_id", "team_id", "service_id"},
	}
}

func coverageMetric(column string) MetricDefinition {
	return MetricDefinition{
		Table: "testops_coverage_metrics_daily", ValueColumn: column,
		ScopeSupport: testopsScopes(), InnerGroupColumns: []string{"repo_id", "team_id", "service_id"},
	}
}

// clickHouseDateLayout is the wire form for a {name:Date} query parameter.
// See FetchMetricSeriesByScope's binding note for why a time.Time cannot be
// bound directly.
const clickHouseDateLayout = "2006-01-02"

// ClickHouseLoader implements SeriesFetcher against a real ClickHouse.
type ClickHouseLoader struct {
	conn  conn
	orgID string
}

func NewClickHouseLoader(connection conn, orgID string) (*ClickHouseLoader, error) {
	if connection == nil {
		return nil, fmt.Errorf("benchmarking: clickhouse connection is required")
	}
	return &ClickHouseLoader{conn: connection, orgID: orgID}, nil
}

// FetchMetricSeriesByScope ports fetch_metric_series_by_scope
// (_common.py:259-341). The SQL is Python's shape verbatim: an inner
// argMax(value, computed_at) grouped by day plus the metric's own grouping
// columns, then an outer avg() per (scope_key, day) with NULL metric values
// dropped, ordered by scope_key then day.
//
// NOTE ON ORG SCOPING: Python's query carries NO org_id predicate at all --
// the sink it runs against is already org-bound (BaseMetricsSink), so the
// filtering happens a layer up. A Go loader talking straight to the driver has
// no such binding, so an explicit org_id filter is added. That NARROWS what
// already-correct data satisfies the query and cannot widen it -- the same
// reasoning CHAOS-4775 applied to family_readback.
func (loader *ClickHouseLoader) FetchMetricSeriesByScope(
	ctx context.Context, metricName string, startDay, endDay time.Time, scopeType string,
) (map[string][]MetricPoint, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("benchmarking: loader unavailable")
	}
	canonical := CanonicalMetricName(metricName)
	definition, ok := metricDefinitions[canonical]
	if !ok {
		return nil, fmt.Errorf("benchmarking: unsupported metric %q", metricName)
	}
	if !definition.ScopeSupport[scopeType] {
		return nil, fmt.Errorf("benchmarking: metric %s does not support scope_type=%s", canonical, scopeType)
	}

	filters := []string{"day >= {start_day:Date}", "day <= {end_day:Date}", "org_id = {org_id:String}"}
	filters = append(filters, definition.ExtraFilters...)

	var scopeExpression string
	switch scopeType {
	case ScopeRepo:
		scopeExpression = "toString(repo_id)"
	case ScopeTeam:
		scopeExpression = "ifNull(team_id, '')"
		filters = append(filters, "ifNull(team_id, '') != ''")
	default:
		scopeExpression = "'global'"
	}

	// org_id LEADS the group, matching every source table's CURRENT sorting
	// key (migrations 027/042 prepended it). The org filter above is already
	// pre-aggregation so this cannot change a row today -- org_id is constant
	// within the query -- but it makes the tenant boundary STRUCTURAL rather
	// than dependent on the filter staying where it is. Hoisting that filter
	// into an outer query is a plausible refactor, and it is exactly what
	// turns this shape into ai-families' cross-tenant P1.
	innerGroup := append([]string{"org_id", "day"}, definition.InnerGroupColumns...)
	query := fmt.Sprintf(`
SELECT scope_key, day, avg(metric_value) AS value
FROM (
    SELECT
        %s AS scope_key,
        day,
        argMax(%s, computed_at) AS metric_value
    FROM %s
    WHERE %s
    GROUP BY %s
)
WHERE metric_value IS NOT NULL
GROUP BY scope_key, day
ORDER BY scope_key, day`,
		scopeExpression, definition.ValueColumn, definition.Table,
		strings.Join(filters, " AND "), strings.Join(innerGroup, ", "),
	)

	arguments := []any{
		// Date parameters are bound as YYYY-MM-DD STRINGS, not time.Time. The
		// driver serialises a time.Time for a {name:Date} parameter as a full
		// DateTime literal and the server rejects it outright -- "Cannot parse
		// date here: toDateTime('2026-08-24 00:00:00') cannot be parsed as
		// Date". Caught on the sibling compounding_risk loader by CI (#2231's
		// shard leg) and fixed here in the same pass BEFORE this family ever
		// reached CI, since the shape is identical. Same fix and reasoning as
		// RecommendationsLoader.windowArguments (recommendations_loader.go:77-97);
		// the string form is also the closer mirror of what Python's
		// clickhouse-connect puts on the wire for a datetime.date.
		clickhouse.Named("start_day", startDay.Format(clickHouseDateLayout)),
		clickhouse.Named("end_day", endDay.Format(clickHouseDateLayout)),
		clickhouse.Named("org_id", loader.orgID),
	}
	if canonical == "deployment_frequency" {
		arguments = append(arguments, clickhouse.Named("metric_name", "deployment_frequency"))
	}

	rows, err := loader.conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("fetch metric series %s: %w", canonical, err)
	}
	defer rows.Close()

	result := make(map[string][]MetricPoint)
	for rows.Next() {
		var (
			scopeKey string
			day      time.Time
			value    *float64
		)
		if err := rows.Scan(&scopeKey, &day, &value); err != nil {
			return nil, fmt.Errorf("scan metric series row: %w", err)
		}
		// Python drops a blank scope_key and a NULL value
		// (_common.py:333-340); `avg()` over an all-NULL group is NULL, which
		// the outer WHERE cannot catch because it filters the INNER value.
		if scopeKey == "" || value == nil {
			continue
		}
		result[scopeKey] = append(result[scopeKey], MetricPoint{Day: day, Value: *value})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Writer persists all six benchmarking output tables.
type Writer struct {
	conn conn
}

func NewWriter(connection conn) (*Writer, error) {
	if connection == nil {
		return nil, fmt.Errorf("benchmarking: clickhouse connection is required")
	}
	return &Writer{conn: connection}, nil
}

// WriteOutputs writes every non-empty collection and returns the total rows
// written. Column lists and their order are the sink's verbatim
// (sinks/clickhouse/dora.py:56-198).
//
// Fails closed on an empty orgID: org_id is a filter column on all six tables
// and an unscoped row is invisible to every org-bound read.
//
// Mirrors write_benchmarking_outputs' per-collection emptiness checks
// (runner.py:243-256) -- an empty collection is skipped, not written as zero
// rows.
func (writer *Writer) WriteOutputs(ctx context.Context, outputs Outputs, orgID string) (int, error) {
	if writer == nil || writer.conn == nil {
		return 0, fmt.Errorf("benchmarking: writer unavailable")
	}
	if orgID == "" {
		return 0, fmt.Errorf("benchmarking: organization id is required to write benchmarking tables")
	}

	total := 0
	written, err := writer.writeBaselines(ctx, outputs.Baselines, orgID)
	if err != nil {
		return 0, err
	}
	total += written

	written, err = writer.writeMaturityBands(ctx, outputs.MaturityBands, orgID)
	if err != nil {
		return 0, err
	}
	total += written

	written, err = writer.writeAnomalies(ctx, outputs.Anomalies, orgID)
	if err != nil {
		return 0, err
	}
	total += written

	written, err = writer.writePeriodComparisons(ctx, outputs.PeriodComparisons, orgID)
	if err != nil {
		return 0, err
	}
	total += written

	written, err = writer.writeCorrelations(ctx, outputs.Correlations, orgID)
	if err != nil {
		return 0, err
	}
	total += written

	written, err = writer.writeInsights(ctx, outputs.Insights, orgID)
	if err != nil {
		return 0, err
	}
	total += written

	recordRowsWritten(total, orgID != "")
	return total, nil
}

func (writer *Writer) writeBaselines(ctx context.Context, rows []BenchmarkBaselineRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_metric_baselines (
		metric_name, scope_type, scope_key, period_start, period_end,
		rolling_window_days, current_value, baseline_value, percentile_rank,
		p25_value, p50_value, p75_value, p90_value, sample_size, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_metric_baselines batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.MetricName, row.ScopeType, row.ScopeKey, row.PeriodStart, row.PeriodEnd,
			uint16(row.RollingWindowDays), row.CurrentValue, row.BaselineValue, row.PercentileRank,
			row.P25Value, row.P50Value, row.P75Value, row.P90Value, uint32(row.SampleSize),
			orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_metric_baselines row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send testops_metric_baselines batch: %w", err)
	}
	return len(rows), nil
}

func (writer *Writer) writeMaturityBands(ctx context.Context, rows []MaturityBandRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_maturity_bands (
		metric_name, scope_type, scope_key, period_start, period_end,
		value, percentile_rank, maturity_band, confidence, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_maturity_bands batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.MetricName, row.ScopeType, row.ScopeKey, row.PeriodStart, row.PeriodEnd,
			row.Value, row.PercentileRank, row.MaturityBand, row.Confidence, orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_maturity_bands row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send testops_maturity_bands batch: %w", err)
	}
	return len(rows), nil
}

func (writer *Writer) writeAnomalies(ctx context.Context, rows []BenchmarkAnomalyRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_metric_anomalies (
		metric_name, scope_type, scope_key, day, value, baseline_value,
		z_score, anomaly_type, direction, severity, volatility_score, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_metric_anomalies batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.MetricName, row.ScopeType, row.ScopeKey, row.Day, row.Value, row.BaselineValue,
			row.ZScore, row.AnomalyType, row.Direction, row.Severity, row.VolatilityScore,
			orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_metric_anomalies row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send testops_metric_anomalies batch: %w", err)
	}
	return len(rows), nil
}

// writePeriodComparisons writes the table families.json OMITTED from this
// family's `writes` list (CHAOS-4288's surviving ledger defect) -- it is a real
// DDL table that write_benchmarking_outputs has always written.
func (writer *Writer) writePeriodComparisons(ctx context.Context, rows []PeriodComparisonRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_period_comparisons (
		metric_name, scope_type, scope_key, current_period_start, current_period_end,
		comparison_period_start, comparison_period_end, current_value, comparison_value,
		absolute_delta, percentage_change, trend_direction, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_period_comparisons batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.MetricName, row.ScopeType, row.ScopeKey,
			row.CurrentPeriodStart, row.CurrentPeriodEnd,
			row.ComparisonPeriodStart, row.ComparisonPeriodEnd,
			row.CurrentValue, row.ComparisonValue, row.AbsoluteDelta,
			row.PercentageChange, row.TrendDirection, orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_period_comparisons row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send testops_period_comparisons batch: %w", err)
	}
	return len(rows), nil
}

func (writer *Writer) writeCorrelations(ctx context.Context, rows []MetricCorrelationRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_metric_correlations (
		metric_name, paired_metric_name, scope_type, scope_key, period_start, period_end,
		coefficient, p_value, sample_size, is_significant, interpretation, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_metric_correlations batch: %w", err)
	}
	for _, row := range rows {
		significant := uint8(0)
		if row.IsSignificant {
			significant = 1
		}
		if err := batch.Append(
			row.MetricName, row.PairedMetricName, row.ScopeType, row.ScopeKey,
			row.PeriodStart, row.PeriodEnd, row.Coefficient, row.PValue,
			uint32(row.SampleSize), significant, row.Interpretation, orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_metric_correlations row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send testops_metric_correlations batch: %w", err)
	}
	return len(rows), nil
}

func (writer *Writer) writeInsights(ctx context.Context, rows []BenchmarkInsightRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_benchmark_insights (
		insight_id, insight_type, scope_type, scope_key, metric_name, paired_metric_name,
		period_start, period_end, severity, summary, evidence_json, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_benchmark_insights batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.InsightID, row.InsightType, row.ScopeType, row.ScopeKey, row.MetricName,
			row.PairedMetricName, row.PeriodStart, row.PeriodEnd, row.Severity,
			row.Summary, row.EvidenceJSON, orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_benchmark_insights row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send testops_benchmark_insights batch: %w", err)
	}
	return len(rows), nil
}
