# Dev Health: Product Requirements Document

_Last updated: 2026-01-29_

## Purpose
Dev Health is built to make operating modes visible using inspectable evidence. It answers:
- **Where effort is invested**
- **Why flow degrades**
- **Where durability risk concentrates**

This system is explicitly designed to avoid turning metrics into scorecards.

## Scope
- Ingest: Git providers + work tracking + optional incident/deploy signals
- Normalize: unified models and timelines
- Persist: sinks (ClickHouse/Postgres/etc.)
- Compute: materialized metrics + investment distributions
- Serve: GraphQL analytics API
- Visualize: dev-health-web (primary) + optional Grafana dashboards (panel plugin lives in `dev-health-panels`)

## Product pillars
1. **Investment View (canonical)**  
   Theme and subcategory distributions for WorkUnits, persisted at compute-time.
2. **Flow & constraints**  
   Cycle decomposition, throughput, WIP, review load/latency.
3. **Durability risk**  
   Churn, hotspots, ownership concentration.
4. **Well-being signals (team-level)**  
   After-hours and weekend ratios; pattern drift.
5. **Work Graph (entity relationships)**  
   Traversable graph of issues → PRs → commits → files with provenance tracking.

## Guardrails (non-negotiable)
- No person-to-person comparisons.
- LLM is allowed at compute-time for categorization only; UX-time explanation must not recategorize.
- WorkUnits are evidence containers, not categories.
- Categories are fixed, canonical keys.

## Success criteria
- A new engineer can explain each view and its backing computation without reading code.
- Every chart is traceable to (query → table/view → evidence).
- Adding a provider or sink follows a documented contract.

## Implementation Status (as of 2026-01-29)

### Fully Implemented
- **Investment View**: Theme/subcategory distributions, LLM categorization pipeline, InvestmentView component
- **Flow & Constraints**: Cycle time, lead time, throughput, WIP metrics, State Flow Sankey
- **Durability Risk**: Churn, hotspots, bus factor, code ownership Gini coefficient
- **Well-being Signals**: After-hours ratio, weekend ratio in team_metrics_daily
- **Quadrants**: All 4 required (Cycle×Throughput, WIP×Throughput, Churn×Throughput, Review Load×Latency)
- **Visualizations**: Heatmaps, Flame diagrams (timeline + hierarchical), Sankey, Treemap, Sunburst
- **Connectors**: GitHub, GitLab, Jira, Local Git, Synthetic
- **DORA Metrics**: MTTR, change failure rate, deployment frequency, lead time
- **Work Graph API**: GraphQL `workGraphEdges` query with filters (PR #273)
- **Identity Linking**: Issue→commit edges via commit message parsing; issue→PR edges via title, body, and branch name parsing

### Remaining (see `docs/roadmap.md`)
- Capacity planning (forecast completion)
- Work Item repo filtering by tags/settings
- Work Graph Frontend: Related entities UI, interactive explorer (dev-health-web#88)

## References
- `dev-health-ops/AGENTS.md` (repo rules)
- `docs/90-appendix/agent-instructions/*` (deep dives)
- GitHub Project: https://github.com/orgs/full-chaos/projects/1
