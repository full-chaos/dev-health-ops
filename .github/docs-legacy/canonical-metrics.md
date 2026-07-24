# Canonical Metrics & Data Mapping

This document provides the authoritative mapping between API views, metric keys, and the underlying database schema. It is intended to guide the implementation of the Explain endpoint and other analytical features.

## Metric Registry

The following table defines the canonical metrics exposed via the API, their database source, and interpretation.

| Metric Key | Label | Unit | Table | Column | Aggregation | Scope | Description |
|---|---|---|---|---|---|---|---|
| `cycle_time` | Cycle Time | days | `work_item_metrics_daily` | `cycle_time_p50_hours` | Avg (converted to days) | Team | Time from work start to completion. Lower is better. |
| `review_latency` | Review Latency | hours | `repo_metrics_daily` | `pr_first_review_p50_hours` | Avg | Repo | Time from PR creation to first review. Lower is better. |
| `throughput` | Throughput | items | `work_item_metrics_daily` | `items_completed` | Sum | Team | Count of completed work items. Higher is better. |
| `deploy_freq` | Deploy Frequency | deploys | `deploy_metrics_daily` | `deployments_count` | Sum | Repo | Number of deployments to production. Higher is better. |
| `churn` | Code Churn | loc | `repo_metrics_daily` | `total_loc_touched` | Sum | Repo | Total lines of code modified. High churn may indicate instability or rework. |
| `wip_saturation` | WIP Saturation | % | `work_item_metrics_daily` | `wip_congestion_ratio` | Avg | Team | Ratio of active items to developer capacity. Lower is better. |
| `blocked_work` | Blocked Work | hours | `work_item_state_durations_daily` | `duration_hours` | Sum | Team | Total time items spent in a blocked state. Lower is better. |
| `change_failure_rate` | Change Failure Rate | % | `repo_metrics_daily` | `change_failure_rate` | Avg | Repo | Percentage of deployments causing failure. Lower is better. |
| `rework_ratio` | Rework Ratio | % | `repo_metrics_daily` | `rework_churn_ratio_30d` | Avg | Repo | Ratio of churn in recently modified code. Lower is better. |
| `ci_success` | CI Success Rate | % | `cicd_metrics_daily` | `success_rate` | Avg | Repo | Percentage of successful CI pipeline runs. Higher is better. |

### Person-Level Metrics

Person-level metrics use `identity_id` or `user_identity` to filter and group data.

| Metric Key | Label | Unit | Table | Column | Aggregation | Description |
|---|---|---|---|---|---|---|
| `cycle_time` | Cycle Time | days | `work_item_user_metrics_daily` | `cycle_time_p50_hours` | Avg | Individual cycle time. |
| `review_latency` | Review Latency | hours | `user_metrics_daily` | `pr_first_review_p50_hours` | Avg | Time to review PRs assigned to this user. |
| `throughput` | Throughput | items | `work_item_user_metrics_daily` | `items_completed` | Sum | Items completed by the user. |
| `churn` | Code Churn | loc | `user_metrics_daily` | `loc_touched` | Sum | LOC touched by the user. |
| `wip_overlap` | WIP Overlap | items | `work_item_user_metrics_daily` | `wip_count_end_of_day` | Avg | Average concurrent items in progress. |
| `blocked_work` | Blocked Work | items | `work_item_cycle_times` | `status='blocked'` | Sum | Count of items blocked while assigned to user. |

## View -> API Mapping

Data is exposed through specific API endpoints that consume these metrics.

### `/api/v1/explain`
- **Purpose**: Provides detailed breakdown of a specific metric (drivers, contributors, drill-down).
- **Parameters**: `metric` (key from registry), `scope_type`, `scope_id`, `range_days`, `compare_days`.
- **Response**: Current value, delta %, top drivers (teams/repos contributing), and individual contributors (PRs/Issues).

### `/api/v1/home`
- **Purpose**: Executive dashboard with high-level health signals.
- **Metrics Used**: All metrics in the registry are candidates for "Insight Cards" or "Sparklines".
- **Logic**: Computes deltas for all metrics and highlights the "Constraint of the Week" (metric with worst regression).

### `/api/v1/people/{person_id}/metric`
- **Purpose**: Detailed view for a single person's metric.
- **Parameters**: `person_id`, `metric`.
- **Response**: Timeseries, breakdown by dimension (repo, work_type, stage).

## Filters & Scopes

All metrics support standard filtering patterns.

### Time Window
- `start_day`, `end_day`: Explicit date range (inclusive).
- `range_days`: Lookback period from today (e.g., last 14 days).
- `compare_days`: Comparison period duration (usually matches `range_days`).

### Scope (`scope_type`)
- **`org`**: Aggregates data across the entire organization.
- **`team`**: Filters by `team_id`.
  - Maps to `team_id` column in `work_item_*` tables.
  - Requires joining `repos` on `owner_team_id` for repo-scoped metrics (e.g., `churn`).
- **`repo`**: Filters by `repo_id`.
  - Maps to `repo_id` column in `repo_*` tables.
  - For team-scoped metrics, this filter may not apply directly unless the team is mapped to the repo.

## Database Schema Reference

The metrics are derived from the following core ClickHouse tables:

- **`work_item_metrics_daily`**: Aggregated daily stats for work items (Jira/GitLab Issues).
- **`repo_metrics_daily`**: Aggregated daily stats for git repositories (Commits, PRs).
- **`deploy_metrics_daily`**: Deployment events and frequencies.
- **`work_item_state_durations_daily`**: Time spent in each workflow state (Active, Blocked, etc.).
- **`user_metrics_daily`**: Developer activity stats (Commits, PR reviews).
- **`work_item_user_metrics_daily`**: Developer delivery stats (Assigned items).
