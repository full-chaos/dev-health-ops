package home

// metricSpec ports one entry of _METRICS (services/home.py:63-174).
type metricSpec struct {
	Metric     string
	Label      string
	Unit       string
	Table      string
	Column     string
	Aggregator string
	Transform  func(float64) float64
	Scope      string
}

func identityTransform(v float64) float64    { return v }
func hoursToDaysTransform(v float64) float64 { return v / 24.0 }
func percentTransform(v float64) float64     { return v * 100.0 }

// metrics ports _METRICS (services/home.py:63-174) verbatim, same order.
var metrics = []metricSpec{
	{Metric: "cycle_time", Label: "Cycle Time", Unit: "days", Table: "work_item_metrics_daily", Column: "cycle_time_p50_hours", Aggregator: "avg", Transform: hoursToDaysTransform, Scope: "team"},
	{Metric: "review_latency", Label: "Review Latency", Unit: "hours", Table: "repo_metrics_daily", Column: "pr_first_review_p50_hours", Aggregator: "avg", Transform: identityTransform, Scope: "repo"},
	{Metric: "throughput", Label: "Throughput", Unit: "items", Table: "work_item_metrics_daily", Column: "items_completed", Aggregator: "sum", Transform: identityTransform, Scope: "team"},
	{Metric: "deploy_freq", Label: "Deploy Frequency", Unit: "deploys", Table: "deploy_metrics_daily", Column: "deployments_count", Aggregator: "sum", Transform: identityTransform, Scope: "repo"},
	{Metric: "churn", Label: "Code Churn", Unit: "loc", Table: "repo_metrics_daily", Column: "total_loc_touched", Aggregator: "sum", Transform: identityTransform, Scope: "repo"},
	{Metric: "wip_saturation", Label: "WIP Saturation", Unit: "%", Table: "work_item_metrics_daily", Column: "wip_congestion_ratio", Aggregator: "avg", Transform: percentTransform, Scope: "team"},
	{Metric: "blocked_work", Label: "Blocked Work", Unit: "hours", Table: "work_item_state_durations_daily", Column: "duration_hours", Aggregator: "sum", Transform: identityTransform, Scope: "team"},
	{Metric: "change_failure_rate", Label: "Change Failure Rate", Unit: "%", Table: "repo_metrics_daily", Column: "change_failure_rate", Aggregator: "avg", Transform: percentTransform, Scope: "repo"},
	{Metric: "rework_ratio", Label: "Rework Ratio", Unit: "%", Table: "repo_metrics_daily", Column: "rework_churn_ratio_30d", Aggregator: "avg", Transform: percentTransform, Scope: "repo"},
	{Metric: "pr_rework_ratio", Label: "PR Rework Ratio", Unit: "%", Table: "repo_metrics_daily", Column: "pr_rework_ratio", Aggregator: "avg", Transform: percentTransform, Scope: "repo"},
	{Metric: "ci_success", Label: "CI Success Rate", Unit: "%", Table: "cicd_metrics_daily", Column: "success_rate", Aggregator: "avg", Transform: percentTransform, Scope: "repo"},
}

// dedupByComputedAt ports _DEDUP_BY_COMPUTED_AT (api/queries/metrics.py:
// 21-58) -- the natural key each daily-family ReplacingMergeTree(computed_at)
// table's readers dedup on before aggregating.
var dedupByComputedAt = map[string][]string{
	"repo_metrics_daily":              {"day", "repo_id"},
	"work_item_state_durations_daily": {"day", "provider", "work_scope_id", "team_id", "status"},
	"work_item_metrics_daily":         {"day", "provider", "work_scope_id", "team_id"},
	"work_item_user_metrics_daily":    {"day", "provider", "work_scope_id", "user_identity"},
	"cicd_metrics_daily":              {"day", "repo_id"},
	"deploy_metrics_daily":            {"day", "repo_id"},
	"incident_metrics_daily":          {"day", "repo_id"},
	"testops_release_confidence":      {"day", "repo_id"},
	"testops_pipeline_stability":      {"day", "repo_id"},
}

func metricTable(metric string) string {
	for _, m := range metrics {
		if m.Metric == metric {
			return m.Table
		}
	}
	return "repo_metrics_daily"
}

func metricColumn(metric string) string {
	for _, m := range metrics {
		if m.Metric == metric {
			return m.Column
		}
	}
	return "pr_first_review_p50_hours"
}

// metricGroup ports _metric_group (services/home.py:1254-1257).
func metricGroup(metric string) string {
	switch metric {
	case "cycle_time", "throughput", "wip_saturation", "blocked_work":
		return "team_id"
	default:
		return "repo_id"
	}
}

// metricScope ports _metric_scope (services/home.py:1260-1263).
func metricScope(metric string) string {
	switch metric {
	case "cycle_time", "throughput", "wip_saturation", "blocked_work":
		return "team"
	default:
		return "repo"
	}
}
