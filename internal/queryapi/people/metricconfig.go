// Per-metric configuration for GET /api/v1/people/{person_id}/summary and
// GET /api/v1/people/{person_id}/metric -- ports
// _PERSON_METRICS (services/people.py:84-242) verbatim: table/column/
// aggregator/identity_column/extra_where/transform per metric, plus each
// metric's own by_repo/by_work_type/by_stage breakdown config.
//
// DEDUP: every {table} this config names -- work_item_user_metrics_daily,
// user_metrics_daily, work_item_cycle_times -- is ReplacingMergeTree
// (computed_at) (migrations 055/096/001), and this package reads every
// one of them FINAL, never a bare LIMIT-1-BY (that shape sorts and
// collapses the whole table with no per-tenant scope of its own). The
// reference services/people.py hands {table} to queries/people.py's
// fetch_person_metric_value/_series/_breakdown, which format it into SQL
// via dedup_from(table) (clickhouse_dedup.py): work_item_user_metrics_daily
// resolves to a FINAL read (it is in RERUN_DEDUPED_DAILY_TABLES), but
// user_metrics_daily resolves to dedup_from's OTHER shape -- an
// `ORDER BY computed_at DESC LIMIT 1 BY <key>` subquery -- and
// work_item_cycle_times is registered in NEITHER dedup_from table set at
// all, so it falls through to `return table`, a RAW, undeduped read: a
// re-run of a partition (a Celery retry, a rate-limit deferral re-enqueue)
// double-counts every sum()/avg() this config computes over it (the
// blocked_work/throughput/cycle_time breakdown metrics, and person_summary_
// work_mix.sql/person_summary_flow_breakdown.sql's direct reads, see
// summary.go). Both are declared Python-plane defects, not data-semantics
// choices: dedupTable below (metric.go/summary.go's only call site for
// building a {table} read) answers every one of these three tables with a
// plain FINAL read instead, the one dedup shape this package holds to
// throughout (matching search.go's searchPeopleQuery and resolve.go's
// resolvePersonIdentity, which already carry the identical fix for the
// same two RMT tables). None of these columns are Nullable (confirmed
// against their CREATE TABLE statements), so this is a plain FINAL
// substitution, not the argMax-over-Nullable-column tuple-wrap case.
//
// The by_repo breakdowns' join_clause additionally joins `repos` (also
// ReplacingMergeTree, migration 000_raw_tables.sql, version last_synced) --
// the reference's static "INNER JOIN repos ON repos.id = ..." never reads
// it FINAL either, a THIRD instance of the same defect class; this
// config's own join_clause strings read `repos FINAL`.
package people

// personMetricDefinition ports MetricDefinition (api/models/schemas.py:
// 424-426): the free-text description/interpretation pair a metric's
// config carries.
type personMetricDefinition struct {
	Description    string
	Interpretation string
}

// personBreakdownConfig ports one entry of a metric's "breakdowns" dict
// (services/people.py:99-118 etc): table/column/aggregator/identity_column/
// group_expr/join_clause/extra_where/transform for one breakdown
// dimension (by_repo, by_work_type or by_stage).
type personBreakdownConfig struct {
	Table          string
	Column         string
	Aggregator     string
	IdentityColumn string
	GroupExpr      string
	JoinClause     string
	ExtraWhere     string
	Transform      func(float64) float64
}

// personMetric ports one entry of _PERSON_METRICS (services/people.py:
// 84-242).
type personMetric struct {
	Metric         string
	Label          string
	Unit           string
	Table          string
	Column         string
	Aggregator     string
	IdentityColumn string
	ExtraWhere     string
	Transform      func(float64) float64
	Definition     personMetricDefinition

	ByRepo     *personBreakdownConfig
	ByWorkType *personBreakdownConfig
	ByStage    *personBreakdownConfig
}

func identityTransform(v float64) float64    { return v }
func hoursToDaysTransform(v float64) float64 { return v / 24.0 }

// personMetrics ports _PERSON_METRICS (services/people.py:84-242)
// verbatim, in the same declaration order (build_person_summary_response
// iterates this order for its deltas list, services/people.py:500).
var personMetrics = []personMetric{
	{
		Metric:         "cycle_time",
		Label:          "Cycle Time",
		Unit:           "days",
		Table:          "work_item_user_metrics_daily",
		Column:         "cycle_time_p50_hours",
		Aggregator:     "avg",
		IdentityColumn: "user_identity",
		ExtraWhere:     "AND cycle_time_p50_hours IS NOT NULL",
		Transform:      hoursToDaysTransform,
		Definition: personMetricDefinition{
			Description:    "Time from work start to completion for your items.",
			Interpretation: "Lower values indicate faster delivery.",
		},
		ByWorkType: &personBreakdownConfig{
			Table:          "work_item_cycle_times",
			Column:         "cycle_time_hours",
			Aggregator:     "avg",
			IdentityColumn: "assignee",
			GroupExpr:      "if(type = '' OR type IS NULL, 'unknown', type)",
			ExtraWhere:     "AND cycle_time_hours IS NOT NULL",
			Transform:      hoursToDaysTransform,
		},
		ByStage: &personBreakdownConfig{
			Table:          "work_item_cycle_times",
			Column:         "cycle_time_hours",
			Aggregator:     "avg",
			IdentityColumn: "assignee",
			GroupExpr:      "if(status = '' OR status IS NULL, 'unknown', status)",
			ExtraWhere:     "AND cycle_time_hours IS NOT NULL",
			Transform:      hoursToDaysTransform,
		},
	},
	{
		Metric:         "review_latency",
		Label:          "Review Latency",
		Unit:           "hours",
		Table:          "user_metrics_daily",
		Column:         "pr_first_review_p50_hours",
		Aggregator:     "avg",
		IdentityColumn: "identity_id",
		ExtraWhere:     "AND pr_first_review_p50_hours IS NOT NULL",
		Transform:      identityTransform,
		Definition: personMetricDefinition{
			Description:    "Time from pull request creation to first review.",
			Interpretation: "Lower values mean reviews start sooner.",
		},
		ByRepo: &personBreakdownConfig{
			Table:          "user_metrics_daily",
			Column:         "pr_first_review_p50_hours",
			Aggregator:     "avg",
			IdentityColumn: "identity_id",
			GroupExpr:      "repos.repo",
			JoinClause:     "INNER JOIN repos FINAL ON repos.id = user_metrics_daily.repo_id",
			ExtraWhere:     "AND pr_first_review_p50_hours IS NOT NULL",
			Transform:      identityTransform,
		},
	},
	{
		Metric:         "throughput",
		Label:          "Throughput",
		Unit:           "items",
		Table:          "work_item_user_metrics_daily",
		Column:         "items_completed",
		Aggregator:     "sum",
		IdentityColumn: "user_identity",
		ExtraWhere:     "",
		Transform:      identityTransform,
		Definition: personMetricDefinition{
			Description:    "Count of completed work items in the period.",
			Interpretation: "Higher counts indicate more delivery throughput.",
		},
		ByWorkType: &personBreakdownConfig{
			Table:          "work_item_cycle_times",
			Column:         "if(completed_at IS NULL, 0, 1)",
			Aggregator:     "sum",
			IdentityColumn: "assignee",
			GroupExpr:      "if(type = '' OR type IS NULL, 'unknown', type)",
			ExtraWhere:     "",
			Transform:      identityTransform,
		},
	},
	{
		Metric:         "churn",
		Label:          "Code Churn",
		Unit:           "loc",
		Table:          "user_metrics_daily",
		Column:         "loc_touched",
		Aggregator:     "sum",
		IdentityColumn: "identity_id",
		ExtraWhere:     "",
		Transform:      identityTransform,
		Definition: personMetricDefinition{
			Description:    "Lines of code touched in the period.",
			Interpretation: "Higher values can reflect refactors or rework.",
		},
		ByRepo: &personBreakdownConfig{
			Table:          "user_metrics_daily",
			Column:         "loc_touched",
			Aggregator:     "sum",
			IdentityColumn: "identity_id",
			GroupExpr:      "repos.repo",
			JoinClause:     "INNER JOIN repos FINAL ON repos.id = user_metrics_daily.repo_id",
			ExtraWhere:     "",
			Transform:      identityTransform,
		},
	},
	{
		Metric:         "wip_overlap",
		Label:          "WIP Overlap",
		Unit:           "items",
		Table:          "work_item_user_metrics_daily",
		Column:         "wip_count_end_of_day",
		Aggregator:     "avg",
		IdentityColumn: "user_identity",
		ExtraWhere:     "",
		Transform:      identityTransform,
		Definition: personMetricDefinition{
			Description:    "Average count of items active in parallel.",
			Interpretation: "Lower values indicate less multitasking.",
		},
	},
	{
		Metric:         "blocked_work",
		Label:          "Blocked Work",
		Unit:           "items",
		Table:          "work_item_cycle_times",
		Column:         "if(status = 'blocked', 1, 0)",
		Aggregator:     "sum",
		IdentityColumn: "assignee",
		ExtraWhere:     "",
		Transform:      identityTransform,
		Definition: personMetricDefinition{
			Description:    "Count of assigned items marked blocked in the period.",
			Interpretation: "Lower values indicate fewer blockers.",
		},
		ByWorkType: &personBreakdownConfig{
			Table:          "work_item_cycle_times",
			Column:         "if(status = 'blocked', 1, 0)",
			Aggregator:     "sum",
			IdentityColumn: "assignee",
			GroupExpr:      "if(type = '' OR type IS NULL, 'unknown', type)",
			ExtraWhere:     "",
			Transform:      identityTransform,
		},
	},
}

// personMetricConfig ports _metric_config (services/people.py:245-246):
// the first (only) entry whose Metric matches, or ok=false when metric
// names none -- build_person_metric_response's `if not config: raise
// ValueError("metric not supported")` (services/people.py:641-643).
func personMetricConfig(metric string) (personMetric, bool) {
	for _, cfg := range personMetrics {
		if cfg.Metric == metric {
			return cfg, true
		}
	}
	return personMetric{}, false
}

// dedupTable answers a {table} read for this package's three
// ReplacingMergeTree sources with a plain FINAL read -- see this file's
// own package doc comment for why this differs from the reference
// dedup_from's per-table split.
func dedupTable(table string) string {
	return table + " FINAL"
}
