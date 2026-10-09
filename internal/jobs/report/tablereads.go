package report

import "github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"

// tableRead declares, for one source table of the metric registry, how the
// chart reader reads it: the date column time ranges and day/week/month
// buckets apply to (empty means "day"), the columns that identify one row,
// and the column that orders the versions of one key. A re-run of a day
// appends a newer version of a key, so the reader takes the newest version of
// each key and never the raw table (dedupFromSource).
//
// For a ReplacingMergeTree table Key is exactly its ORDER BY and Version is the
// engine's version column; for a plain MergeTree table (no engine-level
// replacement) Key is the identity the writers use, which includes every ORDER
// BY column. TestClickHouseQueryAdapterChartsEveryRegistryMetric derives both
// from the migrated schema and fails when this declaration disagrees.
type tableRead struct {
	Day     string
	Key     []string
	Version string
}

// tableReads is the one declaration per registry table. A registry table that
// is not declared here cannot be charted: validateChartMetrics refuses it
// before any query, so a new table is never read raw.
var tableReads = map[string]tableRead{
	changefailure.Table:               {Key: []string{"org_id", "repo_id", "day"}, Version: "computed_at"},
	"cicd_metrics_daily":              {Key: []string{"org_id", "repo_id", "day"}, Version: "computed_at"},
	"commit_metrics":                  {Key: []string{"org_id", "repo_id", "day", "author_email", "commit_hash"}, Version: "computed_at"},
	"deploy_metrics_daily":            {Key: []string{"org_id", "repo_id", "day"}, Version: "computed_at"},
	"dora_metrics_daily":              {Key: []string{"org_id", "repo_id", "day", "metric_name"}, Version: "computed_at"},
	"file_complexity_snapshots":       {Day: "as_of_day", Key: []string{"org_id", "repo_id", "as_of_day", "file_path"}, Version: "computed_at"},
	"file_hotspot_daily":              {Key: []string{"org_id", "repo_id", "day", "file_path"}, Version: "computed_at"},
	"file_metrics_daily":              {Key: []string{"org_id", "repo_id", "day", "path"}, Version: "computed_at"},
	"ic_landscape_rolling_30d":        {Day: "as_of_day", Key: []string{"org_id", "repo_id", "team_id", "map_name", "as_of_day", "identity_id"}, Version: "computed_at"},
	"incident_metrics_daily":          {Key: []string{"org_id", "repo_id", "day"}, Version: "computed_at"},
	"investment_metrics_daily":        {Key: []string{"org_id", "repo_id", "day", "team_id", "investment_area", "project_stream"}, Version: "computed_at"},
	"issue_type_metrics_daily":        {Key: []string{"org_id", "repo_id", "day", "provider", "team_id", "issue_type_norm"}, Version: "computed_at"},
	"repo_complexity_daily":           {Key: []string{"org_id", "repo_id", "day"}, Version: "computed_at"},
	"repo_metrics_daily":              {Key: []string{"org_id", "repo_id", "day"}, Version: "computed_at"},
	"review_edges_daily":              {Key: []string{"org_id", "repo_id", "reviewer", "author", "day"}, Version: "computed_at"},
	"team_metrics_daily":              {Key: []string{"org_id", "team_id", "repo_id", "day"}, Version: "computed_at"},
	"testops_coverage_metrics_daily":  {Key: []string{"org_id", "repo_id", "day"}, Version: "computed_at"},
	"testops_pipeline_metrics_daily":  {Key: []string{"org_id", "repo_id", "day"}, Version: "computed_at"},
	"testops_test_metrics_daily":      {Key: []string{"org_id", "repo_id", "day"}, Version: "computed_at"},
	"user_metrics_daily":              {Key: []string{"org_id", "repo_id", "author_email", "day"}, Version: "computed_at"},
	"work_item_cycle_times":           {Key: []string{"org_id", "provider", "work_item_id"}, Version: "computed_at"},
	"work_item_metrics_daily":         {Key: []string{"org_id", "provider", "day", "work_scope_id", "team_id"}, Version: "computed_at"},
	"work_item_state_durations_daily": {Key: []string{"org_id", "provider", "work_scope_id", "team_id", "status", "day"}, Version: "computed_at"},
}
