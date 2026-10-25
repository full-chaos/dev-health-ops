package providersync

import "github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"

// The INSERT statements of the nine retired work-item destinations. The daily
// job writes these tables; the statements stay because
// preparedRouteInsertStatements still names each of them for a prepared
// manifest that an earlier binary stored.
const (
	gitHubEstimateCoverageInsert = `INSERT INTO estimate_coverage_metrics_daily
(day, provider, work_scope_id, team_id, team_name, estimated_count,
unestimated_count, backlog_size, ratio, computed_at, org_id)`

	gitHubWorkItemTeamAttributionsInsert = `INSERT INTO work_item_team_attributions
(org_id, repo_id, work_item_id, provider, team_id, team_name, source,
is_primary, confidence, evidence, computed_at, writer, run_id)`

	gitHubWorkItemStateDurationsInsert = `INSERT INTO work_item_state_durations_daily
(day, provider, work_scope_id, team_id, team_name, status, duration_hours,
items_touched, computed_at, avg_wip, org_id)`

	gitHubWorkItemMetricsDailyInsert = `INSERT INTO work_item_metrics_daily (
day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
wip_unassigned_end_of_day, cycle_time_p50_hours, cycle_time_p90_hours,
lead_time_p50_hours, lead_time_p90_hours, wip_age_p50_hours, wip_age_p90_hours,
bug_completed_ratio, story_points_completed, new_bugs_count, new_items_count,
defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at, org_id)`

	gitHubWorkItemUserMetricsDailyInsert = `INSERT INTO work_item_user_metrics_daily (
day, provider, work_scope_id, user_identity, team_id, team_name, items_started,
items_completed, wip_count_end_of_day, cycle_time_p50_hours,
cycle_time_p90_hours, computed_at, org_id)`

	gitHubWorkItemCycleTimesInsert = `INSERT INTO work_item_cycle_times (
work_item_id, provider, day, work_scope_id, team_id, team_name, assignee, type,
status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours,
computed_at, org_id)`

	gitHubIssueTypeMetricsInsert = workitemengine.IssueTypeMetricsInsert
)
