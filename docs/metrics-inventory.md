# Metrics Inventory

This inventory tracks the implementation status of all metrics defined in the `dev-health-ops` project.
Work item metrics assume provider data has been synced via `dev-hops sync work-items ...` (use `-s` to filter repos; `--auth` to override GitHub/GitLab tokens when needed; tags/settings filtering planned).

## 1. Delivery & Velocity (Flow & DORA)

| Metric                 | Status | Source/Implementation                                                                                                                                                                                            |
| :--------------------- | :----: | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cycle Time (PR)        |  [x]   | [metrics/compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                                                                                                                             |
| Coding Time            |  [x]   | [metrics/compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                                                                                                                             |
| Review Time            |  [x]   | [metrics/compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                                                                                                                             |
| Pickup Time            |  [x]   | [metrics/compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                                                                                                                             |
| Deploy Time            |  [x]   | GitHub/GitLab: Synced via `sync deployments --provider github|gitlab`. Computed in [compute_deployments.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_deployments.py) |
| Deployment Frequency   |  [x]   | Derived from [deployments](file:///Users/chris/projects/dev-health-ops/storage.py#959-965) table + fallback to `prs_merged`                                                                                      |
| Lead Time for Changes  |  [x]   | DORA metric in [compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                                                                                                                      |
| MTTR                   |  [x]   | Calculated from Bug work items + Incident records                                                                                                                                                                |
| Change Failure Rate    |  [x]   | Calculated from "revert" PRs in [compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                                                                                                     |
| Work Item Cycle Time   |  [x]   | [metrics/compute_work_items.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_work_items.py)                                                                                                       |
| Work Item Lead Time    |  [x]   | [metrics/compute_work_items.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_work_items.py)                                                                                                       |
| WIP Count/Age          |  [x]   | [metrics/compute_work_items.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_work_items.py)                                                                                                       |
| Flow Efficiency        |  [x]   | [metrics/compute_work_items.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_work_items.py)                                                                                                       |
| CI/CD Pipeline Metrics |  [x]   | GitHub/GitLab: Synced via `sync cicd --provider github|gitlab`. Computed in [compute_cicd.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_cicd.py)                             |
| Incident MTTR          |  [x]   | GitHub/GitLab: Issues labeled 'incident'. Computed in [compute_incidents.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_incidents.py)                                                           |

## 2. Code Quality & Risk

| Metric                   | Status | Source/Implementation                                                                                                                                                                            |
| :----------------------- | :----: | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Churn                    |  [x]   | Total LOC touched in [compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                                                                                                |
| Rework Rate (30-day)     |  [x]   | Computed in [quality.py](file:///Users/chris/projects/dev-health-ops/metrics/quality.py) and wired in [job_daily.py](file:///Users/chris/projects/dev-health-ops/metrics/job_daily.py)           |
| File Hotspot Score       |  [x]   | [metrics/hotspots.py](file:///Users/chris/projects/dev-health-ops/metrics/hotspots.py) combining churn and contributor count                                                                     |
| PR Size                  |  [x]   | `avg_commit_size_loc` and `large_pr_ratio` in [compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                                                                       |
| PR Rework Ratio          |  [x]   | `pr_rework_ratio` in [compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py) (based on changes requested)                                                                   |
| Defect Introduction Rate |  [x]   | [metrics/compute_work_items.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_work_items.py) (bugs created vs items closed)                                                        |
| Single Owner File Ratio  |  [x]   | Computed in [quality.py](file:///Users/chris/projects/dev-health-ops/metrics/quality.py) and wired in [job_daily.py](file:///Users/chris/projects/dev-health-ops/metrics/job_daily.py)           |
| Cyclomatic Complexity    |  [x]   | Computed via `metrics complexity` using `radon`. Stored in `file_complexity_snapshots` (per-file) and `repo_complexity_daily`; loaded in `metrics daily` via `store.get_complexity_snapshots()`. |

## 3. Investment & Portfolio

| Metric                | Status | Source/Implementation                                                                                        |
| :-------------------- | :----: | :----------------------------------------------------------------------------------------------------------- |
| Investment Allocation |  [x]   | Classified via `InvestmentClassifier` in `analytics/investment.py`. Rules in `config/investment_areas.yaml`. |
| Project Stream Churn  |  [x]   | LOC churn aggregated by project stream and investment area.                                                  |
| Strategic vs KTLO %   |  [x]   | Derived from Investment Allocation daily rollups.                                                            |

## 4. IC Metrics & Landscape (New)

| Metric                         | Status | Source/Implementation                                                                                                                                                             |
| :----------------------------- | :----: | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| IC Throughput                  |  [x]   | `delivery_units` (PRs merged + Items completed) in [metrics/compute_ic.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_ic.py). Relies on `identity_mapping.yaml`. |
| IC Churn                       |  [x]   | `loc_touched` (Additions + Deletions) in [metrics/compute_ic.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_ic.py)                                               |
| IC Cycle Time                  |  [x]   | `cycle_p50_hours` (PR median cycle time) in [metrics/compute_ic.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_ic.py)                                            |
| IC Active WIP                  |  [x]   | `work_items_active` (Items in progress/review/blocked) in [metrics/compute_ic.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_ic.py)                              |
| Landscape: Churn vs Throughput |  [x]   | 2D map with team percentile normalization in [metrics/compute_ic.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_ic.py)                                           |
| Landscape: Cycle vs Throughput |  [x]   | 2D map with team percentile normalization in [metrics/compute_ic.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_ic.py)                                           |
| Landscape: WIP vs Throughput   |  [x]   | 2D map with team percentile normalization in [metrics/compute_ic.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_ic.py)                                           |

## 4. Collaboration & Team Dynamics

| Metric                 | Status | Source/Implementation                                                                                                              |
| :--------------------- | :----: | :--------------------------------------------------------------------------------------------------------------------------------- |
| Review Responsiveness  |  [x]   | `pr_pickup_time_p50_hours` in [compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                         |
| Review Load            |  [x]   | `reviews_given` in [compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                                    |
| Review Reciprocity     |  [x]   | Ratio of reviews given vs received in [compute.py](file:///Users/chris/projects/dev-health-ops/metrics/compute.py)                 |
| Bus Factor             |  [x]   | [metrics/knowledge.py](file:///Users/chris/projects/dev-health-ops/metrics/knowledge.py) (Truck Factor: min authors for 50% churn) |
| Knowledge Distribution |  [x]   | [metrics/knowledge.py](file:///Users/chris/projects/dev-health-ops/metrics/knowledge.py) (Gini Coefficient of churn ownership)     |

## 5. Developer Well-being & Cognitive Load

| Metric              | Status | Source/Implementation                                                                                    |
| :------------------ | :----: | :------------------------------------------------------------------------------------------------------- |
| Late-night Activity |  [x]   | [metrics/compute_wellbeing.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_wellbeing.py) |
| Weekend Activity    |  [x]   | [metrics/compute_wellbeing.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_wellbeing.py) |
| Burnout Risk Score  |  [x]   | Combined after-hours/weekend activity per developer                                                      |
| Flow Score          |  [ ]   | Requires IDE telemetry                                                                                   |
| Cognitive Load      |  [ ]   | Requires IDE telemetry                                                                                   |

## 6. Systemic & Process Health

| Metric                | Status | Source/Implementation                                                                                                                                       |
| :-------------------- | :----: | :---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bottleneck Index      |  [x]   | [metrics/compute_work_item_state_durations.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_work_item_state_durations.py)                    |
| WIP Congestion        |  [x]   | [metrics/compute_work_items.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_work_items.py) (WIP / Weekly Throughput)                        |
| Predictability Index  |  [x]   | [metrics/compute_work_items.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_work_items.py) (Completion Rate: Completed / (Completed + WIP)) |
| Pipeline Success Rate |  [x]   | [metrics/compute_cicd.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_cicd.py) (GitHub only, GitLab pending)                                |
| Deploy Success Rate   |  [x]   | [metrics/compute_deployments.py](file:///Users/chris/projects/dev-health-ops/metrics/compute_deployments.py)                                                |
