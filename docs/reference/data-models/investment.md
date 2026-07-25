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
