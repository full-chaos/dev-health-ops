package explain

// metricConfig ports api/services/explain.py's _MetricConfig TypedDict --
// one entry per supported metric key.
type metricConfig struct {
	Label      string
	Unit       string
	Table      string
	Column     string
	GroupBy    string
	Scope      string // "team" or "repo"
	Aggregator string // "avg" or "sum"
	Transform  func(float64) float64
}

func identityTransform(v float64) float64 { return v }

// metricConfigs ports _METRIC_CONFIG (api/services/explain.py:37-118)
// verbatim, key for key, field for field.
var metricConfigs = map[string]metricConfig{
	"cycle_time": {
		Label: "Cycle Time", Unit: "days",
		Table: "work_item_metrics_daily", Column: "cycle_time_p50_hours",
		GroupBy: "team_id", Scope: "team", Aggregator: "avg",
		Transform: func(v float64) float64 { return v / 24.0 },
	},
	"review_latency": {
		Label: "Review Latency", Unit: "hours",
		Table: "repo_metrics_daily", Column: "pr_first_review_p50_hours",
		GroupBy: "repo_id", Scope: "repo", Aggregator: "avg",
		Transform: identityTransform,
	},
	"throughput": {
		Label: "Throughput", Unit: "items",
		Table: "work_item_metrics_daily", Column: "items_completed",
		GroupBy: "team_id", Scope: "team", Aggregator: "sum",
		Transform: identityTransform,
	},
	"deploy_freq": {
		Label: "Deploy Frequency", Unit: "deploys",
		Table: "deploy_metrics_daily", Column: "deployments_count",
		GroupBy: "repo_id", Scope: "repo", Aggregator: "sum",
		Transform: identityTransform,
	},
	"churn": {
		Label: "Code Churn", Unit: "loc",
		Table: "repo_metrics_daily", Column: "total_loc_touched",
		GroupBy: "repo_id", Scope: "repo", Aggregator: "sum",
		Transform: identityTransform,
	},
	"wip_saturation": {
		Label: "WIP Saturation", Unit: "%",
		Table: "work_item_metrics_daily", Column: "wip_congestion_ratio",
		GroupBy: "team_id", Scope: "team", Aggregator: "avg",
		Transform: func(v float64) float64 { return v * 100.0 },
	},
	"blocked_work": {
		Label: "Blocked Work", Unit: "hours",
		// NB: duration_hours is summed across EVERY status this table
		// carries (blocked/in_progress/waiting/done/...), not filtered to
		// status='blocked' -- fetch_metric_value/fetch_metric_contributors/
		// fetch_metric_driver_delta (api/queries/metrics.py,
		// api/queries/explain.py) carry no status predicate at all; only
		// the UNRELATED fetch_blocked_hours (metrics.py:207-248, used by
		// home.py, never by /explain) adds one. Ported verbatim: this
		// route's own "Blocked Work" headline is, today, actually a
		// sum over all statuses, table/column choice included.
		Table: "work_item_state_durations_daily", Column: "duration_hours",
		GroupBy: "team_id", Scope: "team", Aggregator: "sum",
		Transform: identityTransform,
	},
	"change_failure_rate": {
		Label: "Change Failure Rate", Unit: "%",
		Table: "repo_metrics_daily", Column: "change_failure_rate",
		GroupBy: "repo_id", Scope: "repo", Aggregator: "avg",
		Transform: func(v float64) float64 { return v * 100.0 },
	},
}

// defaultMetricKey is the fallback config _METRIC_CONFIG.get(metric,
// _METRIC_CONFIG["cycle_time"]) (api/services/explain.py:146) borrows for
// any metric string this map does not recognise -- the RESPONSE's own
// "metric" field still echoes the ORIGINAL request string, never
// "cycle_time"; only label/unit/table/column/group_by/scope/aggregator/
// transform borrow cycle_time's.
const defaultMetricKey = "cycle_time"

func resolveMetricConfig(metric string) metricConfig {
	if config, ok := metricConfigs[metric]; ok {
		return config
	}
	return metricConfigs[defaultMetricKey]
}
