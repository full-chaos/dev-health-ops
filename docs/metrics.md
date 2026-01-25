# Developer Health Metrics (v2)

This repository computes daily “developer health” metrics from:

- synced Git facts (`git_commits`, `git_commit_stats`, `git_pull_requests`)
- optional work tracking data (Jira issues, GitHub issues/Projects v2, GitLab issues)

Derived time series are written to **ClickHouse** (preferred) and/or **MongoDB** (optional) for Grafana.

All timestamps are treated as **UTC** (ClickHouse stores DateTime in UTC; Mongo stores naive datetimes as UTC by convention).

Important: Jira (and other issue trackers) is **not** a replacement for pull request data. Work items are used for planning/throughput/WIP metrics, while PR metrics (cycle time, merge frequency, review metrics when available) come from the Git provider sync.

## Source Data

Git facts must already exist in the backend you point the job at:

- `git_commits`
- `git_commit_stats`
- `git_pull_requests`

Work tracking facts should be synced from provider APIs via `python cli.py sync work-items ...`.
See `docs/task_trackers.md` for configuration. (`metrics daily --provider ...` still exists as a convenience/backward-compatible path.)
`--provider auto` (default) loads work items from the database only and skips provider API calls.

CI/CD pipeline facts are synced from GitHub/GitLab via the `sync cicd` job:

```bash
python cli.py sync cicd --provider github --db "<DB_CONN>" --auth "$GITHUB_TOKEN" --owner "<org>" --repo "<repo>"
python cli.py sync cicd --provider gitlab --db "<DB_CONN>" --auth "$GITLAB_TOKEN" --gitlab-url "<URL>" --project-id <ID>
```

Deployments and incident facts are synced via their own jobs:

```bash
python cli.py sync deployments --provider github --db "<DB_CONN>" --auth "$GITHUB_TOKEN" --owner "<org>" --repo "<repo>"
python cli.py sync incidents --provider github --db "<DB_CONN>" --auth "$GITHUB_TOKEN" --owner "<org>" --repo "<repo>"
python cli.py sync deployments --provider gitlab --db "<DB_CONN>" --auth "$GITLAB_TOKEN" --gitlab-url "<URL>" --project-id <ID>
python cli.py sync incidents --provider gitlab --db "<DB_CONN>" --auth "$GITLAB_TOKEN" --gitlab-url "<URL>" --project-id <ID>
```

Note: Jira Ops/Service Desk incidents are planned once project-to-repo or deployment mapping is defined.

## Derived Tables / Collections

### Git / Repo / User

- `repo_metrics_daily`
- `user_metrics_daily`
- `commit_metrics`

### Work Tracking

- `work_item_metrics_daily` (daily aggregates, by provider/team/repo)
- `work_item_user_metrics_daily` (daily aggregates, by provider/user/team)
- `work_item_cycle_times` (per-work-item fact rows for completed items)

### Team Well-being (team-level only)

- `team_metrics_daily`

## Metric Definitions

### Commit size bucketing

- `total_loc = additions + deletions` (summed from `git_commit_stats`)
- bucket:
  - `small`: `total_loc <= 50`
  - `medium`: `51..300`
  - `large`: `> 300`

### PR cycle time

For PRs with `merged_at` on day **D**:

- `cycle_time_hours = (merged_at - created_at) / 3600`
- Distribution fields (per repo/day and user/day):
  - `median_pr_cycle_hours` (p50)
  - `pr_cycle_p75_hours`
  - `pr_cycle_p90_hours`

### Large change thresholds

- Large commits: `total_loc > 300`
- Large PRs: `additions + deletions >= LARGE_PR_LOC_THRESHOLD` (default: `1000`)
  - Note: PR size facts are **best-effort** and may be missing depending on the connector.

### Daily user metrics (`user_metrics_daily`)

Keyed by `(repo_id, author_email, day)` where `author_email` falls back to `author_name` when email is missing.

- Commits: counts, LOC added/deleted, distinct files changed (union across the day), large commits, avg commit size
- PRs: authored (created that day), merged (merged that day), avg/p50/p75/p90 PR cycle hours (for PRs merged that day)
- Collaboration (nullable when not available):
  - `pr_pickup_time_p50_hours`: PR created → first interaction (comment/review)
  - `pr_first_review_p50_hours` / `p90`: PR created → first review
  - `pr_review_time_p50_hours`: first review → merge
  - `reviews_given`, `changes_requested_given`: counts of review submissions for the day

Null behavior:

- if review/comment facts are unavailable, pickup/first-review/review-time fields remain `NULL`

### Daily repo metrics (`repo_metrics_daily`)

Keyed by `(repo_id, day)`.

- Commits: count, LOC touched, avg size, large commit ratio
- PRs: merged count, p50/p75/p90 PR cycle hours
- Quality (best-effort): `large_pr_ratio`, `pr_rework_ratio` (requires PR size and review facts)
- Knowledge:
  - `bus_factor`: smallest number of developers accounting for ≥ 50% of code churn
  - `code_ownership_gini`: Gini coefficient of code contribution inequality (0..1)

### Optional per-commit metrics (`commit_metrics`)

Keyed by `(repo_id, day, author_email, commit_hash)`.

### Work item normalization + cycle times

Work items are normalized into a unified `WorkItem` abstraction (`models/work_items.py`).

Best-effort fields:

- `started_at`: first transition into normalized `in_progress`
- `completed_at`: first transition into normalized `done` or `canceled`

Time metrics:

- `lead_time_hours = completed_at - created_at` (when completed)
- `cycle_time_hours = completed_at - started_at` (when started + completed)

Null behavior:

- items missing `started_at` are excluded from cycle-time distributions

### Work tracking daily metrics (`work_item_metrics_daily`)

Keyed by `(day, provider, team_id, work_scope_id)`.

`work_scope_id` is provider-native and is used to avoid assuming a repo UUID exists for work items:

- Jira: Jira project key (e.g. `ABC`)
- GitHub: repository full name (e.g. `owner/repo`) when sourced from issues; Projects v2 uses `ghprojv2:<org>#<number>`
- GitLab: project full path (e.g. `group/project`)
- Throughput: `items_started`, `items_completed`
- Ownership gap: `items_completed_unassigned` (subset of completed items that had no assignee at completion time)
- WIP: `wip_count_end_of_day`
- Cycle/lead time distributions (nullable if no samples): p50/p90
- WIP age distributions (nullable if no WIP samples): p50/p90
- `bug_completed_ratio`: completed bugs / total completed
- `story_points_completed`: sum of story points completed (Jira only, when configured)
- `predictability_score`: Completion Rate = `items_completed / (items_completed + wip_count_end_of_day)`

### Work item facts (`work_item_cycle_times`)

Keyed by `(provider, work_item_id)`; stored as ClickHouse `ReplacingMergeTree` by `computed_at` and as Mongo upserts.

### Work item time-in-state (`work_item_state_durations_daily`)

Keyed by `(day, provider, work_scope_id, team_id, status)` and computed from provider status transitions (Jira changelog when available).

Null behavior:

- items without status transition history contribute no rows

### Unassigned work

If a work item has no assignee, daily rollups:

- count it under `team_id=''` (shown as “unassigned” in dashboards)
- emit a `work_item_user_metrics_daily` row with `user_identity='unassigned'` so you can chart “closed unassigned” work over time

### Team well-being (team-level only)

Computed from commits (deduplicated by commit hash) and a team mapping.

- `after_hours_commit_ratio`: commits outside business hours on weekdays
- `weekend_commit_ratio`: commits on weekends

Configuration:

- `BUSINESS_TIMEZONE` (default: `UTC`)
- `BUSINESS_HOURS_START` (default: `9`)
- `BUSINESS_HOURS_END` (default: `17`)

## Storage Targets

### ClickHouse (tables)

Tables are created automatically if missing:

- `repo_metrics_daily`
- `user_metrics_daily`
- `commit_metrics`
- `team_metrics_daily`
- `work_item_metrics_daily`
- `work_item_user_metrics_daily`
- `work_item_cycle_times`

They are `MergeTree` tables partitioned by `toYYYYMM(day)` and ordered by the natural keys for Grafana queries.

## ClickHouse query notes

These derived tables are append-only with a `computed_at` version. To query the latest value per key, use `argMax(<metric>, computed_at)` grouped by the key columns. If you need a daily total, do it in two steps (no nested aggregates):

```sql
SELECT
  day,
  sum(items_completed_unassigned_latest) AS items_completed_unassigned
FROM
(
  SELECT
    day,
    provider,
    work_scope_id,
    team_id,
    argMax(items_completed_unassigned, computed_at) AS items_completed_unassigned_latest
  FROM work_item_metrics_daily
  WHERE provider = 'jira'
  GROUP BY day, provider, work_scope_id, team_id
)
GROUP BY day
ORDER BY day;
```

Re-computations are **append-only** and distinguished by `computed_at`. To query the latest metrics for a key/day, use `argMax(..., computed_at)` in ClickHouse.

### MongoDB (collections)

Collections are created automatically:

- `repo_metrics_daily`
- `user_metrics_daily`
- `commit_metrics`
- `team_metrics_daily`
- `work_item_metrics_daily`
- `work_item_user_metrics_daily`
- `work_item_cycle_times`

Documents use stable compound `_id` keys and are written via upserts, so recomputation is safe.

### SQLite (tables)

Tables are created automatically in the same `.db` file:

- `repo_metrics_daily`
- `user_metrics_daily`
- `commit_metrics`
- `team_metrics_daily`
- `work_item_metrics_daily`
- `work_item_user_metrics_daily`
- `work_item_cycle_times`

## Running The Daily Job

The job reads source data from the **same backend** you point it at (ClickHouse or MongoDB), using the synced tables/collections:

- `git_commits`
- `git_commit_stats`
- `git_pull_requests`
  It also supports SQLite, reading the same tables and writing metrics tables into the same `.db` file.

### Environment variables

- `DATABASE_URI` (or `DATABASE_URL`): ClickHouse, MongoDB, SQLite, or PostgreSQL URI for both reading source data and writing derived metrics.
- `SECONDARY_DATABASE_URI`: Required when running with `--sink both` to write to a secondary backend.

### Examples

- Compute one day (backend inferred from `--db` or `DATABASE_URI`):
  - `python cli.py metrics daily --date 2025-02-01 --db clickhouse://localhost:8123/default`
  - `python cli.py metrics daily --date 2025-02-01 --db mongodb://localhost:27017/mergestat`
  - `python cli.py metrics daily --date 2025-02-01 --db sqlite:///./mergestat.db`
- Compute 7-day backfill ending at a date:
  - `python cli.py metrics daily --date 2025-02-01 --backfill 7 --db clickhouse://localhost:8123/default`
- Filter to one repository:
  - `python cli.py metrics daily --date 2025-02-01 --repo-id <uuid> --db clickhouse://localhost:8123/default`
- Compute git + work item metrics (requires provider credentials; see `docs/task_trackers.md`):
  - `python cli.py sync work-items --provider all --date 2025-02-01 --backfill 30 --db clickhouse://localhost:8123/default`
  - `python cli.py metrics daily --date 2025-02-01 --backfill 30 --db clickhouse://localhost:8123/default`

## Dependencies

- ClickHouse uses `clickhouse-connect` (already in `requirements.txt`).
- MongoDB uses `pymongo` (available via the `motor` dependency in `requirements.txt`).
- SQLite uses `sqlalchemy` (already in `requirements.txt`).

## IC Metrics & Landscape (v3)

We compute canonical Individual Contributor (IC) metrics and "Developer Landscape" maps to visualize patterns in churn, throughput, and work-in-progress.

**Note:** These metrics are designed for identifying signals and patterns, not for ranking individuals.

### Identity Resolution

- Users are mapped to a canonical `identity_id` (preferring email) across providers.
- Configuration: `config/teams.yaml` or `config/team_mapping.yaml`.

### IC Metrics (`user_metrics_daily`)

Extends the daily user metrics with work tracking and unified throughput signals:

- `identity_id`: Canonical user identity.
- `loc_touched`: Sum of additions + deletions.
- `delivery_units`: `prs_merged` + `work_items_completed`.
- `work_items_active`: Number of items in progress/review/blocked at end of day.
- `cycle_p50_hours`: Median cycle time (PRs).

### Landscape Maps (`ic_landscape_rolling_30d`)

Rolling 30-day metrics normalized by team percentiles (0..1).
Stored in ClickHouse table `ic_landscape_rolling_30d`.

#### Map 1: Churn vs Throughput

- **X**: `log(churn_loc_30d)` (rolling 30d sum of LOC touched)
- **Y**: `delivery_units_30d` (rolling 30d sum of delivery units)

#### Map 2: Cycle Time vs Throughput

- **X**: `log(cycle_p50_30d_hours)` (median of daily median PR cycle times over 30d)
- **Y**: `delivery_units_30d`

#### Map 3: WIP vs Throughput

- **X**: `wip_max_30d` (max active work items over 30d)
- **Y**: `delivery_units_30d`

Each coordinate (`x_raw`, `y_raw`) is normalized per team into (`x_norm`, `y_norm`) representing the percentile rank within the team.

## Code Complexity (`repo_complexity_daily`)

Complexity metrics are computed by scanning local git clones at specific historical references using `radon`.

- **Cyclomatic Complexity (CC)**: Measures the number of linearly independent paths through a program's source code.
- **Metric fields**:
  - `cyclomatic_total`: Sum of CC for all functions/classes in the repo.
  - `cyclomatic_avg`: Mean CC per function.
  - `high_complexity_functions`: Count of functions with CC > 15 (configurable).
  - `very_high_complexity_functions`: Count of functions with CC > 25.
- **Backfilling**: Uses `git checkout` (via `GitPython`) to analyze historical state.

## Investment Metrics (`investment_metrics_daily`)

Categorizes engineering effort into investment areas (e.g., "New Value", "Security", "Infrastructure") using a rule-based classifier.

- **Artifacts Classified**:
  - **Work Items**: Based on labels, components, and title keywords.
  - **Commits (Churn)**: Based on file path patterns (e.g., `infra/` or `tests/`).
- **Metric fields**:
  - `investment_area`: The assigned category.
  - `project_stream`: A secondary grouping (e.g., "Project Phoenix").
  - `delivery_units`: Story points or count of work items completed.
  - `churn_loc`: Sum of additions + deletions associated with the area.
- **Configuration**: `config/investment_areas.yaml` defines the matching rules and priorities.

## Views and interpretations

Metrics are intended to be consumed through views that constrain interpretation and provide drill-down.

- Views index: `user-guide/views-index.md`
- Investment View: `user-guide/investment-view.md`
- Work Graph: `user-guide/work-graph.md`
- Computations index: `computations/index.md`
