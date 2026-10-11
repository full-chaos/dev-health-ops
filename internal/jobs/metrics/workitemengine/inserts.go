package workitemengine

// The INSERT statements, with one definition for both writers: the sync effect
// adapters in internal/providersync and the daily families in
// internal/jobs/metrics/daily. The tables are plain MergeTree (append only): a second write of one key does not replace the first, so every reader
// takes the newest computed_at per key.

const IssueTypeMetricsInsert = `INSERT INTO issue_type_metrics_daily
(repo_id, day, provider, team_id, issue_type_norm, created_count,
completed_count, active_count, cycle_p50_hours, cycle_p90_hours,
lead_p50_hours, computed_at, org_id)`

// WorkItemCompletionInsert is the one definition of the work_item_completion_daily INSERT for both writers.
const WorkItemCompletionInsert = `INSERT INTO work_item_completion_daily
(repo_id, day, team_id, delivery_units, work_items_completed, prs_merged,
churn_loc, cycle_p50_hours, computed_at, org_id)`
