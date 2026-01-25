# Investment View

This page is the leadership-facing definition of the canonical investment lens.

## What it answers
- Where is engineering effort going?
- Is the system in planned delivery mode, reactive mode, or maintenance mode?
- What is dominating attention (and for how long)?

## Core mechanics
- A WorkUnit receives a compute-time distribution over subcategories.
- Subcategories roll up deterministically to themes.
- Aggregations are probability-weighted.

## Taxonomy (canonical keys)
See: `docs/00-overview/04-investment-taxonomy.md`.

## Non-negotiables
- No WorkUnit-as-category.
- No user-defined categories.
- No “unknown” output.

## Related docs
- LLM categorization contract
- Work graph (relationships and materialization)
