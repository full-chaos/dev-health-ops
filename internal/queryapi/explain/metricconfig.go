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
	// StatusFilter, when non-empty, is the exact Column-table status value
	// every read of this metric restricts to (a plain `status = '<value>'`
	// alongside the org/scope filter, same subquery, same nesting depth).
	// "" applies no status restriction -- every metricConfigs entry other
	// than blocked_work leaves this empty.
	StatusFilter string
	Transform    func(float64) float64
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
		// duration_hours is summed only over rows whose status is
		// "blocked" -- the sole blocked-shaped entry the status
		// vocabulary carries (backlog/todo/in_progress/in_review/
		// blocked/done/canceled/unknown; see
		// internal/providerfoundation/normalization.go's WorkItemRecord
		// and internal/streamhandlers/external_schema.go's own copy of
		// the same closed set). This matches this table's other,
		// UNRELATED reader over the same column (fetch_blocked_hours,
		// api/queries/metrics.py, used by home.py's own blocked-hours
		// panel, never by /explain) rather than fetch_metric_value/
		// fetch_metric_contributors/fetch_metric_driver_delta
		// (api/queries/metrics.py, api/queries/explain.py), which carry
		// no status predicate at all and so sum/rank across every
		// status the table records.
		Table: "work_item_state_durations_daily", Column: "duration_hours",
		GroupBy: "team_id", Scope: "team", Aggregator: "sum",
		StatusFilter: "blocked",
		Transform:    identityTransform,
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
