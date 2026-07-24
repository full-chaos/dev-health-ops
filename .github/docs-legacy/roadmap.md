# Roadmap & Remaining Tasks

This checklist tracks what is complete and what remains to finalize `dev-health-ops`.

## Completed

- [x] **Core Git + PR Metrics**: Commit size, churn, PR cycle/pickup/review times, review load, PR size stats.
- [x] **Work Item Flow Metrics**: Cycle/lead time p50/p90, WIP count/age, flow efficiency, state duration + avg WIP.
- [x] **Quality + Risk Metrics**: Defect introduction rate, WIP congestion, rework churn ratio, single-owner file ratio proxy.
- [x] **DORA Metrics**: MTTR, change failure rate, deployment/incident daily rollups.
- [x] **Wellbeing Signals**: After-hours and weekend commit ratios; weekend active users (derived from user activity).
- [x] **Connectors + Pipelines**: GitHub/GitLab CI/CD + deployments + incidents ingestion; async batch helpers.
- [x] **Storage + Schema**: ClickHouse migrations and sink support for new metrics tables/columns.
- [x] **SQLite test cleanup**: Dispose SQLAlchemyStore engines to avoid aiosqlite event-loop teardown warnings.
- [x] **CLI Controls**: Dedicated `sync cicd`, `sync deployments`, and `sync incidents` targets.
- [x] **Complexity Metrics (Batch)**: Radon-based cyclomatic complexity scanning with DB-driven batch mode (`-s "*"`).
- [x] **Unified Complexity Snapshot Loading**: `metrics daily` loads complexity snapshots via storage abstraction (`get_complexity_snapshots`).
- [x] **Investment Area Classification**: Automated work item and churn classification based on configurable rules.
- [x] **Work Item Sync Command**: Dedicated `sync work-items` flow to fetch provider work items separately from `metrics daily`.
- [x] **Work Item Auth Override**: `sync work-items --auth` can override GitHub/GitLab tokens for provider sync.
- [x] **Synthetic fixtures**: CI/CD + deployments + incidents with metrics rollups for ClickHouse.
- [x] **IC Metrics + Landscape**: Identity resolution, unified UserMetricsDailyRecord, and landscape rolling stats (Churn/Cycle/WIP vs Throughput).
- [x] **Grafana Dev Health Panels**: Panel plugin with Developer Landscape, Hotspot Explorer, and Investment Flow views.
- [x] **Dev Health panel theming**: Plugin-local Grafana theme with a custom visualization palette.
- [x] **Dev Health panel selector**: Panel settings dropdown to switch between Developer Landscape, Hotspot Explorer, and Investment Flow.
- [x] **Grafana Panel ClickHouse Contracts**: Stats schema views for landscape, hotspot, and investment flow panels (use `WITH ... AS` aliasing).
- [x] **Dashboard team filter normalization**: Include legacy NULL/empty team IDs in Grafana ClickHouse queries via `ifNull(nullIf(team_id, ''), 'unassigned')`.
- [x] **Investment metrics NULL team IDs**: `investment_metrics_daily.team_id` stores NULL for unassigned; investment flow view casts via `toNullable(team_id)`.
- [x] **Hotspot Explorer formatting**: Use table format and order by day to keep Grafana sorting valid.
- [x] **Hotspot ownership concentration**: Derive ownership concentration from git blame line shares.
- [x] **Synthetic blame coverage**: Expand fixture file set to improve hotspot ownership coverage.
- [x] **Blame-only sync**: Add `cli.py sync blame --provider <local|github|gitlab>` for targeted blame backfill.
- [x] **Backfill commit caps**: GitHub/GitLab `--date/--backfill` runs default to unlimited commits unless explicitly capped.
- [x] **Hotspot Explorer frame binding**: Prefer facts frames via `churn_loc_30d` to keep drivers/trends rendering.
- [x] **IC Drilldown churn vs throughput**: Add identity-filtered churn vs throughput panel.

## Remaining

### Data + Fixtures

- [x] **Fixtures validation**: Added Phase 2 metrics checks to `run_fixtures_validation()` - validates cycle_time_hours, flow_efficiency, predictability_score, wip_congestion_ratio, and new_items_count. ([#267](https://github.com/full-chaos/dev-health-ops/issues/267))

### Dashboards

- [x] **Dashboards for CI/CD, deployments, incidents** (panels for success rate, duration, deploy counts, MTTR).
- [x] **Investment Areas team filter**: use regex `match(...)` filters for the team variable in ClickHouse queries.
- [x] **Work tracking dashboards audit**: Validated filters and table joins - all use correct `match()` regex patterns with `ifNull(nullIf(team_id, ''), 'unassigned')` normalization. No problematic joins; dashboards use pre-aggregated tables. ([#266](https://github.com/full-chaos/dev-health-ops/issues/266))
- [ ] **Fix dashboard templating filters**: Ensure variable regex and `match(...)` filters do not return empty results. ([#264](https://github.com/full-chaos/dev-health-ops/issues/264))

### Metrics Enhancements

- [x] **Bus factor (true)**: Top contributors accounting for >50% of churn (Bus Factor) and Gini coefficient (Knowledge Distribution).
- [x] **Predictability index**: Completion Rate (items completed / (completed + wip)).
- [ ] **Capacity planning**: Forecast completion using historical throughput. ([#269](https://github.com/full-chaos/dev-health-ops/issues/269))
- [x] **Identity Linking**: Parse commit messages, PR bodies, and branch names for issue refs (Jira keys, #123). Creates direct issue→commit and issue→PR edges. ([#268](https://github.com/full-chaos/dev-health-ops/issues/268))
- [ ] **Work Item Repo Filtering by Tags/Settings**: Allow `sync work-items` to filter repos using `repos.tags` or `repos.settings` (beyond name glob). ([#270](https://github.com/full-chaos/dev-health-ops/issues/270))

### API Enhancements

- [x] **GraphQL Work Graph API**: Add resolvers and types for work_graph_edges traversal queries. ([#265](https://github.com/full-chaos/dev-health-ops/issues/265), [PR #273](https://github.com/full-chaos/dev-health-ops/pull/273))

### Frontend (dev-health-web)

- [ ] **Work Graph GraphQL types**: Add query definitions and TypeScript types for workGraphEdges API. ([dev-health-web#89](https://github.com/full-chaos/dev-health-web/issues/89))
- [ ] **Related Entities - Issue page**: Show linked PRs, blocking issues on issue detail page. ([dev-health-web#90](https://github.com/full-chaos/dev-health-web/issues/90))
- [ ] **Related Entities - PR page**: Show linked issues and commits on PR detail page. ([dev-health-web#91](https://github.com/full-chaos/dev-health-web/issues/91))
- [ ] **Work Graph Explorer**: Interactive graph visualization for entity relationships. ([dev-health-web#92](https://github.com/full-chaos/dev-health-web/issues/92))

### Testing + Docs

- [x] **Tests for new sinks columns** (ClickHouse + SQLite write paths for Phase 2 + wellbeing).
- [x] **Docs refresh**: Usage examples for new CLI flags and fixture generation steps.
