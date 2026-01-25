# Metric Calculation Details

Deep-dive into how metrics are computed, including formulas, edge cases, and implementation notes.

---

## Git & Repository Metrics

### Daily User Metrics (`user_metrics_daily`)

**Key:** `(repo_id, author_email, day)`

Note: `author_email` falls back to `author_name` when email is missing.

| Metric | Calculation |
|--------|-------------|
| `commits_count` | Count of commits on day |
| `loc_added` | Sum of additions |
| `loc_deleted` | Sum of deletions |
| `loc_touched` | `loc_added + loc_deleted` |
| `files_changed` | Distinct files changed (union across day) |
| `large_commits` | Commits where `total_loc > 300` |
| `avg_commit_size` | `loc_touched / commits_count` |

**PR Metrics (for PRs merged that day):**

| Metric | Calculation |
|--------|-------------|
| `prs_authored` | PRs created on day |
| `prs_merged` | PRs merged on day |
| `pr_cycle_p50_hours` | Median of `(merged_at - created_at)` |
| `pr_cycle_p75_hours` | 75th percentile |
| `pr_cycle_p90_hours` | 90th percentile |

**Collaboration Metrics (nullable):**

| Metric | Calculation |
|--------|-------------|
| `pr_pickup_time_p50_hours` | PR created → first interaction |
| `pr_first_review_p50_hours` | PR created → first review |
| `pr_review_time_p50_hours` | First review → merge |
| `reviews_given` | Count of review submissions |
| `changes_requested_given` | Count of change requests |

### Daily Repo Metrics (`repo_metrics_daily`)

**Key:** `(repo_id, day)`

| Metric | Calculation |
|--------|-------------|
| `commits_count` | Total commits |
| `loc_touched` | Sum of all additions + deletions |
| `avg_commit_size` | `loc_touched / commits_count` |
| `large_commit_ratio` | Large commits / total commits |
| `prs_merged` | PRs merged on day |
| `pr_cycle_p50_hours` | Median PR cycle time |
| `pr_cycle_p75_hours` | 75th percentile |
| `pr_cycle_p90_hours` | 90th percentile |

**Quality Metrics (best-effort):**

| Metric | Calculation |
|--------|-------------|
| `large_pr_ratio` | Large PRs / total PRs |
| `pr_rework_ratio` | PRs with multiple review rounds / total |
| `bus_factor` | Minimum developers for 50% of churn |
| `code_ownership_gini` | Gini coefficient of contribution |

---

## Work Item Metrics

### Normalization

Work items from all providers normalize to unified `WorkItem` model:

```python
class WorkItem:
    provider: str          # jira, github, gitlab
    work_item_id: str      # Provider-native ID
    work_scope_id: str     # Project/repo identifier
    team_id: Optional[str] # Assigned team
    created_at: datetime
    started_at: Optional[datetime]   # First in_progress
    completed_at: Optional[datetime] # First done/canceled
```

### Status Normalization

Statuses map to canonical categories:

| Category | Meaning |
|----------|---------|
| `backlog` | Not yet scheduled |
| `todo` | Scheduled but not started |
| `in_progress` | Active work |
| `in_review` | Awaiting review |
| `blocked` | Work cannot proceed |
| `done` | Successfully completed |
| `canceled` | Abandoned |

### Daily Work Item Metrics (`work_item_metrics_daily`)

**Key:** `(day, provider, work_scope_id, team_id)`

| Metric | Calculation |
|--------|-------------|
| `items_started` | Items transitioning to `in_progress` |
| `items_completed` | Items transitioning to `done` or `canceled` |
| `items_completed_unassigned` | Completed items with no assignee |
| `wip_count_end_of_day` | Items in `in_progress`, `in_review`, `blocked` |
| `cycle_time_p50_hours` | Median `completed_at - started_at` |
| `cycle_time_p90_hours` | 90th percentile cycle time |
| `lead_time_p50_hours` | Median `completed_at - created_at` |
| `lead_time_p90_hours` | 90th percentile lead time |
| `wip_age_p50_hours` | Median age of WIP items |
| `wip_age_p90_hours` | 90th percentile WIP age |
| `bug_completed_ratio` | Bugs / total completed |
| `story_points_completed` | Sum of story points (Jira only) |
| `predictability_score` | `items_completed / (items_completed + wip_count)` |

### Work Scope ID Convention

Provider-native identifier for work scope:

| Provider | Format | Example |
|----------|--------|---------|
| Jira | Project key | `ABC` |
| GitHub Issues | Repository full name | `owner/repo` |
| GitHub Projects v2 | `ghprojv2:<org>#<number>` | `ghprojv2:myorg#3` |
| GitLab | Project full path | `group/project` |

---

## Team Well-Being Metrics

### Daily Team Metrics (`team_metrics_daily`)

**Key:** `(team_id, day)`

Computed from commits (deduplicated by hash) with team mapping.

| Metric | Calculation |
|--------|-------------|
| `after_hours_commit_ratio` | Commits outside business hours on weekdays |
| `weekend_commit_ratio` | Commits on weekends |

**Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `BUSINESS_TIMEZONE` | `UTC` | Timezone for hour calculation |
| `BUSINESS_HOURS_START` | `9` | Start of business hours |
| `BUSINESS_HOURS_END` | `17` | End of business hours |

---

## IC Landscape Metrics

### Rolling 30-Day Metrics (`ic_landscape_rolling_30d`)

Normalized by team percentiles (0–1) for visualization.

| Metric | Calculation |
|--------|-------------|
| `churn_loc_30d` | Rolling 30d sum of LOC touched |
| `delivery_units_30d` | `prs_merged + work_items_completed` |
| `cycle_p50_30d_hours` | Median of daily median PR cycle times |
| `wip_max_30d` | Max active work items over 30d |

### Landscape Maps

**Map 1: Churn × Throughput**
- X: `log(churn_loc_30d)`
- Y: `delivery_units_30d`

**Map 2: Cycle Time × Throughput**
- X: `log(cycle_p50_30d_hours)`
- Y: `delivery_units_30d`

**Map 3: WIP × Throughput**
- X: `wip_max_30d`
- Y: `delivery_units_30d`

Each coordinate normalized to team percentile rank.

---

## Code Complexity

### Daily Complexity Metrics (`repo_complexity_daily`)

Computed via `radon` scanning historical git references.

| Metric | Calculation |
|--------|-------------|
| `cyclomatic_total` | Sum of CC for all functions |
| `cyclomatic_avg` | Mean CC per function |
| `high_complexity_functions` | Functions with CC > 15 |
| `very_high_complexity_functions` | Functions with CC > 25 |

---

## Investment Metrics

### Daily Investment (`investment_metrics_daily`)

Categorizes effort into investment areas via rule-based classifier.

**Artifacts classified:**
- Work items: Based on labels, components, title keywords
- Commits (churn): Based on file path patterns

| Metric | Calculation |
|--------|-------------|
| `investment_area` | Assigned category |
| `project_stream` | Secondary grouping |
| `delivery_units` | Story points or item count |
| `churn_loc` | LOC associated with area |

**Configuration:** `config/investment_areas.yaml`

---

## Edge Cases & Null Handling

### Missing Data

| Scenario | Behavior |
|----------|----------|
| No `started_at` | Excluded from cycle-time distributions |
| No status history | No state duration rows |
| No review facts | Pickup/review fields remain NULL |
| No assignee | Counted under `team_id=''` and `user_identity='unassigned'` |

### Recomputation

- All metrics are **append-only** with `computed_at` timestamp
- Use `argMax(..., computed_at)` to get latest value
- Safe to recompute any day (idempotent via compound keys)
