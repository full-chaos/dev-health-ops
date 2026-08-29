// Package analytics is the Go port of
// dev_health_ops.api.graphql.resolvers.analytics.resolve_analytics
// (ops/src/dev_health_ops/api/graphql/resolvers/analytics.py), CHAOS-4352
// plan Wave 4's batch `analytics` root -- the highest-fan-out operation in
// the wave (up to 10 counted sub-requests, more real ClickHouse queries
// once sankey/flowMatrix's internal expansion is counted; see
// resolve_analytics's doc comment below).
package analytics

import "fmt"

// ValidationError mirrors Python's api/graphql/errors.py ValidationError --
// a distinguishable error type carrying which input field was rejected, so
// callers/tests can assert on validation vs. a real ClickHouse failure the
// same way sql/validate.py's raises let Python's error handler tell the two
// apart.
type ValidationError struct {
	Message string
	Field   string
	Value   any
}

func (e *ValidationError) Error() string { return e.Message }

func newValidationError(field string, value any, format string, args ...any) error {
	return &ValidationError{Message: fmt.Sprintf(format, args...), Field: field, Value: value}
}

// Dimension is the Go port of sql/validate.py's Dimension enum -- the
// internal representation used for DB column mapping, distinct from the
// wire-level model.DimensionInput (which gqlgen already constrains to the
// schema's allowed enum values at argument-coercion time, so this port
// does not need to re-validate an arbitrary string the way Python's
// validate_dimension does -- Python's own `dimension: DimensionInput!`
// field is likewise already Strawberry-enum-validated before
// resolve_analytics ever sees it. validate_dimension there mainly
// translates the enum's already-valid `.value` into Python's own
// Dimension class for db_column/db_expression lookups; this port's
// dimensionFromInput below is the same translation, not re-validation --
// EXCEPT for the AUTHOR-as-grouping-dimension business rule, which is a
// real rejection this port must reproduce.
type Dimension string

const (
	DimensionTeam        Dimension = "team"
	DimensionRepo        Dimension = "repo"
	DimensionAuthor      Dimension = "author"
	DimensionWorkType    Dimension = "work_type"
	DimensionTheme       Dimension = "theme"
	DimensionSubcategory Dimension = "subcategory"
)

// Measure is the Go port of sql/validate.py's Measure enum.
type Measure string

const (
	MeasureCount                Measure = "count"
	MeasureChurnLOC             Measure = "churn_loc"
	MeasurePRReworkRatio        Measure = "pr_rework_ratio"
	MeasureCycleTimeHours       Measure = "cycle_time_hours"
	MeasureThroughput           Measure = "throughput"
	MeasurePipelineSuccessRate  Measure = "pipeline_success_rate"
	MeasurePipelineFailureRate  Measure = "pipeline_failure_rate"
	MeasurePipelineDurationP95  Measure = "pipeline_duration_p95"
	MeasurePipelineQueueTime    Measure = "pipeline_queue_time"
	MeasurePipelineRerunRate    Measure = "pipeline_rerun_rate"
	MeasureTestPassRate         Measure = "test_pass_rate"
	MeasureTestFailureRate      Measure = "test_failure_rate"
	MeasureTestFlakeRate        Measure = "test_flake_rate"
	MeasureTestSuiteDurationP95 Measure = "test_suite_duration_p95"
	MeasureCoverageLinePct      Measure = "coverage_line_pct"
	MeasureCoverageBranchPct    Measure = "coverage_branch_pct"
	MeasureCoverageDeltaPct     Measure = "coverage_delta_pct"
	MeasureFlagFrictionDelta    Measure = "flag_friction_delta"
	MeasureFlagErrorRateDelta   Measure = "flag_error_rate_delta"
	MeasureFlagCoverageRatio    Measure = "flag_coverage_ratio"
	MeasureFlagActivationRate   Measure = "flag_activation_rate"
)

// BucketInterval is the Go port of sql/validate.py's BucketInterval enum.
type BucketInterval string

const (
	BucketIntervalDay   BucketInterval = "day"
	BucketIntervalWeek  BucketInterval = "week"
	BucketIntervalMonth BucketInterval = "month"
)

// dbColumn ports Dimension.db_column (sql/validate.py:24-69) exactly,
// including the AUTHOR rejection (sql/validate.py:27-47): neither source
// table the compiler ever selects FROM has a scalar per-row author
// identity column, so AUTHOR is not a valid GROUP BY/breakdown dimension
// -- only usable via who.developers / scope.level=developer filtering.
func dbColumn(dim Dimension, useInvestment bool) (string, error) {
	if dim == DimensionAuthor {
		return "", newValidationError("dimension", string(dim),
			"author is not a supported breakdown/grouping dimension; "+
				"filter by who.developers or scope.level=developer instead "+
				"of grouping by author.")
	}
	if useInvestment {
		switch dim {
		case DimensionTeam:
			return "ifNull(nullIf(ut.team_label, ''), 'unassigned')", nil
		case DimensionRepo:
			// Unassigned repo emits '' (NOT the bare 'unassigned'): the web
			// adapter renders an empty repo label as a distinct "Unassigned
			// repo" node. Emitting 'unassigned' collides with the TEAM
			// unassigned label at the ECharts node-name level and folds
			// TEAM->THEME->REPO into a cycle ("Sankey is a DAG" error).
			return "ifNull(nullIf(r.repo, ''), if(repo_id IS NULL, '', toString(repo_id)))", nil
		case DimensionWorkType:
			return "work_unit_type", nil
		case DimensionTheme:
			return "splitByChar('.', subcategory_kv.1)[1]", nil
		case DimensionSubcategory:
			return "subcategory_kv.1", nil
		}
	} else {
		switch dim {
		case DimensionTeam:
			return "team_id", nil
		case DimensionRepo:
			return "repo_id", nil
		case DimensionWorkType:
			return "work_item_type", nil
		case DimensionTheme:
			return "investment_area", nil
		case DimensionSubcategory:
			return "project_stream", nil
		}
	}
	return "", fmt.Errorf("analytics: dbColumn: unhandled dimension %q", dim)
}

// dbExpression ports Measure.db_expression (sql/validate.py:101-171)
// exactly -- three overlapping mappings (investment-path base, the
// testops mapping, the feature-flag mapping) merged the same way Python's
// dict.update chain merges them (testops/ff entries only apply on the
// non-investment path, matching Python: the investment `mapping` dict
// never receives the testops_mapping/ff_mapping updates in the
// use_investment branch).
func dbExpression(measure Measure, useInvestment, useRepoAllocation bool) (string, error) {
	if useInvestment {
		throughputExpr := "SUM(subcategory_kv.2)"
		cycleTimeExpr := "AVG(dateDiff('hour', from_ts, to_ts))"
		if useRepoAllocation {
			throughputExpr = "SUM(subcategory_kv.2 * allocation_weight)"
			cycleTimeExpr = "SUM(dateDiff('hour', from_ts, to_ts) * allocation_weight) / NULLIF(SUM(allocation_weight), 0)"
		}
		switch measure {
		case MeasureCount, MeasureThroughput:
			return throughputExpr, nil
		case MeasureChurnLOC:
			return "SUM(if(effort_metric = 'churn_loc', subcategory_kv.2 * effort_value, 0))", nil
		case MeasureCycleTimeHours:
			return cycleTimeExpr, nil
		default:
			return "", fmt.Errorf("analytics: dbExpression: measure %q has no investment-path mapping", measure)
		}
	}

	switch measure {
	case MeasureCount:
		return "SUM(work_items_completed)", nil
	case MeasureChurnLOC:
		return "SUM(churn_loc)", nil
	case MeasurePRReworkRatio:
		return "SUM(pr_rework_ratio * prs_merged) / NULLIF(SUM(prs_merged), 0)", nil
	case MeasureCycleTimeHours:
		return "AVG(cycle_p50_hours)", nil
	case MeasureThroughput:
		return "SUM(work_items_completed)", nil
	case MeasurePipelineSuccessRate:
		return "AVG(success_rate) * 100", nil
	case MeasurePipelineFailureRate:
		return "AVG(failure_rate) * 100", nil
	case MeasurePipelineDurationP95:
		return "AVG(p95_duration_seconds)", nil
	case MeasurePipelineQueueTime:
		return "AVG(avg_queue_seconds)", nil
	case MeasurePipelineRerunRate:
		return "AVG(rerun_rate) * 100", nil
	case MeasureTestPassRate:
		return "AVG(pass_rate) * 100", nil
	case MeasureTestFailureRate:
		return "AVG(failure_rate) * 100", nil
	case MeasureTestFlakeRate:
		return "AVG(flake_rate) * 100", nil
	case MeasureTestSuiteDurationP95:
		return "AVG(suite_duration_p95_seconds)", nil
	case MeasureCoverageLinePct:
		return "AVG(line_coverage_pct)", nil
	case MeasureCoverageBranchPct:
		return "AVG(branch_coverage_pct)", nil
	case MeasureCoverageDeltaPct:
		return "AVG(coverage_delta_pct)", nil
	case MeasureFlagFrictionDelta:
		return "AVG(release_user_friction_delta) * 100", nil
	case MeasureFlagErrorRateDelta:
		return "AVG(release_error_rate_delta) * 100", nil
	case MeasureFlagCoverageRatio:
		return "AVG(coverage_ratio) * 100", nil
	case MeasureFlagActivationRate:
		return "AVG(flag_activation_rate) * 100", nil
	}
	return "", fmt.Errorf("analytics: dbExpression: unhandled measure %q", measure)
}

// measureSourceTable ports Measure.source_table (sql/validate.py:173-197):
// testops/feature-flag measures read a DIFFERENT daily table than the
// default investment_metrics_daily, applied via dedup_from in the
// compiler. Returns "" (Python: None) for every measure that stays on the
// default source.
func measureSourceTable(measure Measure) string {
	switch measure {
	case MeasurePipelineSuccessRate, MeasurePipelineFailureRate, MeasurePipelineDurationP95,
		MeasurePipelineQueueTime, MeasurePipelineRerunRate:
		return "testops_pipeline_metrics_daily"
	case MeasureTestPassRate, MeasureTestFailureRate, MeasureTestFlakeRate, MeasureTestSuiteDurationP95:
		return "testops_test_metrics_daily"
	case MeasureCoverageLinePct, MeasureCoverageBranchPct, MeasureCoverageDeltaPct:
		return "testops_coverage_metrics_daily"
	case MeasureFlagFrictionDelta, MeasureFlagErrorRateDelta, MeasureFlagCoverageRatio, MeasureFlagActivationRate:
		return "release_impact_daily"
	case MeasurePRReworkRatio:
		return "repo_metrics_daily"
	}
	return ""
}

// dateTruncUnit ports BucketInterval.date_trunc_unit (sql/validate.py:211-214)
// -- an identity mapping (the enum value IS the ClickHouse date_trunc unit
// name), kept as a named function so a caller reads the same shape as the
// Python source rather than using the enum's string value directly.
func dateTruncUnit(interval BucketInterval) string { return string(interval) }
