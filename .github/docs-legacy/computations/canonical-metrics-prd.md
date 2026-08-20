# Canonical Metrics Reference

This document defines the authoritative mapping between API views, metric keys, and database schema.

---

## Metric Registry

### Team/Repo Level Metrics

| Metric Key | Label | Unit | Table | Column | Aggregation | Description |
|------------|-------|------|-------|--------|-------------|-------------|
| `cycle_time` | Cycle Time | days | `work_item_metrics_daily` | `cycle_time_p50_hours` | Avg (→ days) | Time from work start to completion |
| `review_latency` | Review Latency | hours | `repo_metrics_daily` | `pr_first_review_p50_hours` | Avg | Time from PR creation to first review |
| `throughput` | Throughput | items | `work_item_metrics_daily` | `items_completed` | Sum | Count of completed work items |
| `deploy_freq` | Deploy Frequency | deploys | `deploy_metrics_daily` | `deployments_count` | Sum | Number of production deployments |
| `churn` | Code Churn | loc | `repo_metrics_daily` | `total_loc_touched` | Sum | Total lines of code modified |
| `wip_saturation` | WIP Saturation | % | `work_item_metrics_daily` | `wip_congestion_ratio` | Avg | Active items / developer capacity |
| `blocked_work` | Blocked Work | hours | `work_item_state_durations_daily` | `duration_hours` | Sum | Time items spent blocked |
| `change_failure_rate` | Change Failure Rate | % | `repo_metrics_daily` | `change_failure_rate` | Avg | Deployments causing failure |
| `rework_ratio` | Rework Ratio | % | `repo_metrics_daily` | `rework_churn_ratio_30d` | Avg | Churn in recently modified code |
| `ci_success` | CI Success Rate | % | `cicd_metrics_daily` | `success_rate` | Avg | Successful CI pipeline runs |

### Person-Level Metrics

Uses `identity_id` or `user_identity` to filter and group.

| Metric Key | Label | Unit | Table | Column | Aggregation | Description |
|------------|-------|------|-------|--------|-------------|-------------|
| `cycle_time` | Cycle Time | days | `work_item_user_metrics_daily` | `cycle_time_p50_hours` | Avg | Individual cycle time |
| `review_latency` | Review Latency | hours | `user_metrics_daily` | `pr_first_review_p50_hours` | Avg | Time to review assigned PRs |
| `throughput` | Throughput | items | `work_item_user_metrics_daily` | `items_completed` | Sum | Items completed by user |
| `churn` | Code Churn | loc | `user_metrics_daily` | `loc_touched` | Sum | LOC touched by user |
| `wip_overlap` | WIP Overlap | items | `work_item_user_metrics_daily` | `wip_count_end_of_day` | Avg | Concurrent items in progress |
| `blocked_work` | Blocked Work | items | `work_item_cycle_times` | `status='blocked'` | Sum | Items blocked while assigned |

---

## Database Schema Reference

### Core Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `repo_metrics_daily` | Repo-level git/PR stats | `repo_id`, `day` |
| `user_metrics_daily` | Developer activity | `repo_id`, `author_email`, `day` |
| `work_item_metrics_daily` | Work tracking aggregates | `day`, `provider`, `work_scope_id`, `team_id` |
| `work_item_user_metrics_daily` | Developer delivery | `day`, `provider`, `user_identity`, `team_id` |
| `team_metrics_daily` | Team well-being | `team_id`, `day` |
| `work_item_cycle_times` | Per-item fact rows | `provider`, `work_item_id` |
| `work_item_state_durations_daily` | Time in state | `day`, `provider`, `work_scope_id`, `status` |
| `deploy_metrics_daily` | Deployment frequency | `repo_id`, `day` |
| `cicd_metrics_daily` | CI/CD pipeline stats | `repo_id`, `day` |

---

## Metric Calculation Details

### Commit Size Bucketing

```
total_loc = additions + deletions
```

| Bucket | Range |
|--------|-------|
| Small | total_loc ≤ 50 |
| Medium | 51–300 |
| Large | > 300 |

### PR Cycle Time

For PRs merged on day D:
```
cycle_time_hours = (merged_at - created_at) / 3600
```

Distribution fields:
- `median_pr_cycle_hours` (p50)
- `pr_cycle_p75_hours`
- `pr_cycle_p90_hours`

### Work Item Cycle Time

```
lead_time_hours = completed_at - created_at
cycle_time_hours = completed_at - started_at
```

- `started_at`: First transition to `in_progress`
- `completed_at`: First transition to `done` or `canceled`
- Items missing `started_at` excluded from cycle-time distributions

### Large Change Thresholds

| Type | Threshold |
|------|-----------|
| Large commit | total_loc > 300 |
| Large PR | additions + deletions ≥ 1000 (default) |

### Bus Factor

Smallest number of developers accounting for ≥ 50% of code churn.

### Code Ownership Gini

Gini coefficient (0–1) of code contribution inequality. Higher = more concentrated ownership.

---

## API Endpoints

### `/api/v1/explain`

Detailed metric breakdown.

**Parameters:**
- `metric`: Key from registry
- `scope_type`: `org`, `team`, or `repo`
- `scope_id`: Identifier for scope
- `range_days`: Lookback period
- `compare_days`: Comparison period

**Response:**
- Current value
- Delta %
- Top drivers (teams/repos contributing)
- Individual contributors (PRs/Issues)

### `/api/v1/home`

Executive dashboard with health signals.

- Computes deltas for all metrics
- Highlights "Constraint of the Week" (worst regression)

### `/api/v1/people/{person_id}/metric`

Single person metric detail.

**Response:**
- Timeseries
- Breakdown by dimension (repo, work_type, stage)

---

## Filter Patterns

### Time Window

| Parameter | Usage |
|-----------|-------|
| `start_day`, `end_day` | Explicit date range (inclusive) |
| `range_days` | Lookback from today |
| `compare_days` | Comparison period duration |

### Scope Types

| Type | Filters On | Notes |
|------|------------|-------|
| `org` | Entire organization | No filtering |
| `team` | `team_id` column | Join `repos` on `owner_team_id` for repo metrics |
| `repo` | `repo_id` column | Direct filter on repo tables |

---

## ClickHouse Query Patterns

### Latest Value per Key

Tables are append-only with `computed_at` versioning:

```sql
SELECT
  day,
  argMax(items_completed, computed_at) AS items_completed
FROM work_item_metrics_daily
WHERE provider = 'jira'
GROUP BY day, provider, work_scope_id, team_id
ORDER BY day;
```

### Daily Totals (Two-Step)

```sql
SELECT
  day,
  sum(items_completed_latest) AS items_completed
FROM (
  SELECT
    day,
    argMax(items_completed, computed_at) AS items_completed_latest
  FROM work_item_metrics_daily
  GROUP BY day, provider, work_scope_id, team_id
)
GROUP BY day
ORDER BY day;
```

---

## Null Behavior

- Null fields indicate data unavailable, **not zero**
- Items without `started_at` excluded from cycle-time distributions
- Items without status history contribute no state duration rows
- Missing review facts leave pickup/review-time fields NULL
