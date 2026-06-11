# Investment View

This page is the leadership-facing definition of the canonical investment lens.

## What it answers
- Where is engineering effort going?
- Is the system in planned delivery mode, reactive mode, or maintenance mode?
- What is dominating attention (and for how long)?

## Core mechanics
- A WorkUnit receives a compute-time distribution over subcategories.
- Subcategories roll up deterministically to themes.
- Aggregations are **effort-weighted**: each probability is multiplied by the WorkUnit's effort value before summing (see [Investment API](../api/investment-api.md)).

## Taxonomy (canonical keys)
See the [Investment Taxonomy](../product/investment-taxonomy.md).

## Non-negotiables
- No WorkUnit-as-category.
- No user-defined categories.
- No “unknown” output.

## Related docs
- [Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md) — how the distribution is computed
- [Investment Data Model](../architecture/investment-data-model.md) — how it is persisted
- [Investment API](../api/investment-api.md) — how it is aggregated and effort-weighted
- [Investment Materialization](../ops/investment-materialization.md) — the CLI that produces it
- [LLM Categorization Contract](../llm/categorization-contract.md)
- [Work Graph](work-graph.md) — relationships and materialization
