# Dev Health: Product Requirements Document

_Last updated: 2026-01-25_

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
- Visualize: dev-health-web + (optional) Grafana panels

## Product pillars
1. **Investment View (canonical)**  
   Theme and subcategory distributions for WorkUnits, persisted at compute-time.
2. **Flow & constraints**  
   Cycle decomposition, throughput, WIP, review load/latency.
3. **Durability risk**  
   Churn, hotspots, ownership concentration.
4. **Well-being signals (team-level)**  
   After-hours and weekend ratios; pattern drift.

## Guardrails (non-negotiable)
- No person-to-person comparisons.
- LLM is allowed at compute-time for categorization only; UX-time explanation must not recategorize.
- WorkUnits are evidence containers, not categories.
- Categories are fixed, canonical keys.

## Success criteria
- A new engineer can explain each view and its backing computation without reading code.
- Every chart is traceable to (query → table/view → evidence).
- Adding a provider or sink follows a documented contract.

## References
- `dev-health-ops/AGENTS.md` (repo rules)
- `docs/90-appendix/agent-instructions/*` (deep dives)
