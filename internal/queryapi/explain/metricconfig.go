package explain

import (
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
)

// metricConfig ports api/services/explain.py's _MetricConfig TypedDict --
// one entry per supported metric key.
type metricConfig struct {
	Label      string
	Unit       string
	Table      string
	Column     string
	GroupBy    string
	Scope      string // "team" or "repo"
	Aggregator string // "avg", "sum", or a fixed rule: "ratio", "merged_weighted"
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
	// Incident-based (CHAOS-8981): deployments linked to an incident /
	// deployments over the window's summed daily counts, undefined without a
	// deployment or without incident evidence. Aggregator "ratio" is read
	// through changefailure.WindowRateSQL, never an average of daily ratios.
	"change_failure_rate": {
		Label: "Change Failure Rate", Unit: "%",
		Table: changefailure.Table, Column: "change_failure_rate",
		GroupBy: "repo_id", Scope: "repo", Aggregator: "ratio",
		Transform: func(v float64) float64 { return v * 100.0 },
	},
	// Reverted / merged pull requests, weighted by each day's merged pull
	// requests so the window value is total reverted / total merged. No
	// writer stores a revert rate yet (nothing detects a reverted pull
	// request), so the metric has no data: unknown, never 0%.
	"revert_rate": {
		Label: "Revert Rate", Unit: "%",
		Table: "repo_metrics_daily", Column: "revert_rate",
		GroupBy: "repo_id", Scope: "repo", Aggregator: "merged_weighted",
		Transform: func(v float64) float64 { return v * 100.0 },
	},
}

// resolveMetricConfig is the config of a metric the route knows. An unknown
// name has NO config: the route never answers with another metric's config
// (CHAOS-9136, D5869). The Python original borrowed cycle_time's for every name
// outside the map (api/services/explain.py:146); that is a known bug, not a
// behaviour to keep.
func resolveMetricConfig(metric string) (metricConfig, bool) {
	config, ok := metricConfigs[metric]
	return config, ok
}

// IsKnownMetric reports whether the explain route has a config for the metric.
func IsKnownMetric(metric string) bool {
	_, ok := metricConfigs[metric]
	return ok
}

// MetricNames is the metric names the explain route has a config for, sorted.
func MetricNames() []string {
	names := make([]string, 0, len(metricConfigs))
	for name := range metricConfigs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
