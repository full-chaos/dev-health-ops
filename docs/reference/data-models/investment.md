---
page_id: ref-investment-model
summary: Latest work-unit Investment record, effort value, distributions, evidence, quality, and materialization concepts.
content_type: reference
owner: platform-api
source_of_truth:
  - src/dev_health_ops/investment_taxonomy.py
  - src/dev_health_ops/work_graph/investment/
  - src/dev_health_ops/api/queries/investment.py
applicability: current
lifecycle: active
---

# Investment data model

An Investment work-unit record contains a tenant-scoped work-unit identity, interval, repository or allocation context, effort metric and value, theme and subcategory distributions, structural evidence, evidence quality, categorization status, model/run provenance, and computation time.

The request path uses the latest materialized row for each organization and work unit. Multi-repository allocation can distribute a unit's effort across repositories while preserving the total effort invariant.

Theme and subcategory distributions are probabilistic contributions, not duplicated labels. See [Investment taxonomy](../taxonomies/investment.md) and [Weighting and aggregation](../metrics/weighting-and-aggregation.md).

## Deprecated: `investment_metrics_daily` / `investment_areas.yaml`

`investment_metrics_daily` and its feeder rule set `src/dev_health_ops/config/investment_areas.yaml`
are a **pre-WorkUnit legacy path** (`src/dev_health_ops/analytics/investment.py`). The YAML file's
own header states this: "feeds only the pre-WorkUnit daily `investment_*` metric tables... do not
use this file for canonical WorkUnit categorization." Its `investment_area` values (e.g.
`security`, `infrastructure`) are free-form legacy labels — **not** the fixed five-theme taxonomy
above, and not interchangeable with it.

The canonical theme/subcategory distribution is `latest_work_unit_investments`
(`work_graph/investment/` materialization; `theme_probs`/`subcategory_probs` computed once at
categorization time, deterministic roll-up, never recomputed at read time), team-scoped through a
join to `work_item_team_attributions` — see `api/queries/investment.py`'s
`fetch_investment_breakdown(include_team_id=True)` for the reference query shape, which already
carries ownership precedence (CHAOS-2600).

**Consumers outside this repo must read the canonical join, never `investment_metrics_daily`.**
CHAOS-4398 found that `dev-health-acr`'s `FactInvestment` producer (CHAOS-4363/#308) reads
`investment_metrics_daily` and therefore surfaces the deprecated legacy taxonomy, not the
canonical one — the same class of gap CHAOS-4347 named for repository status and CHAOS-4365 named
for cognitive load: a real, existing, deterministic metric with no producer reading the correct
source. A new acr producer reading the `latest_work_unit_investments` ⋈ `work_item_team_attributions`
join is required before any team-scoped investment-mix consumer (e.g. cohort ranking) can trust its
numbers.
