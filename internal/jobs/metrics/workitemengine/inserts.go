package workitemengine

// The three INSERT statements, with one definition for both writers: the sync
// effect adapters in internal/providersync and the daily families in
// internal/jobs/metrics/daily. The three tables are plain MergeTree (append
// only): a second write of one key does not replace the first, so every reader
// takes the newest computed_at per key.

const IssueTypeMetricsInsert = `INSERT INTO issue_type_metrics_daily
(repo_id, day, provider, team_id, issue_type_norm, created_count,
completed_count, active_count, cycle_p50_hours, cycle_p90_hours,
lead_p50_hours, computed_at, org_id)`

const InvestmentClassificationsInsert = `INSERT INTO investment_classifications_daily
(repo_id, day, artifact_type, artifact_id, provider, investment_area,
project_stream, confidence, rule_id, computed_at, org_id)`

const InvestmentMetricsInsert = `INSERT INTO investment_metrics_daily
(repo_id, day, team_id, investment_area, project_stream, delivery_units,
work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)`
