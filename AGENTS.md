# AGENTS — Briefing and pointers (dev-health-ops)

This file is intentionally short. The canonical instructions live in the MkDocs site under `docs/`.

## Read-first (in order)
1. **Product intent and guardrails**: `docs/product/prd.md`, `docs/product/concepts.md`
2. **Pipeline boundaries**: `docs/architecture/data-pipeline.md`
3. **Investment model (canonical)**: `docs/user-guide/investment-view.md`, `docs/product/investment-taxonomy.md`
4. **LLM contract (compute-time only)**: `docs/llm/categorization-contract.md`
5. **Views and interpretation**: `docs/user-guide/views-index.md`, `docs/visualizations/patterns.md`
6. **API surface**: `docs/api/graphql-overview.md`, `docs/api/view-mapping.md`
7. **How to run it**: `docs/ops/cli-reference.md`

## Non-negotiables (summary)
- **WorkUnits are evidence containers, not categories.**
- Categorization is **compute-time only** and persisted as distributions.
- Theme roll-up is deterministic from subcategories (taxonomy is fixed).
- UX-time LLM is **explanation only** and must not recompute categories/edges/weights.
- Persistence goes through **sinks** only (no file exports, no debug dumps).

## Change discipline (agents)
- Identify which layer you are changing: connector, processor, metrics, sink, API, UI.
- Make the smallest possible change that achieves the outcome.
- If behavior changes, add/adjust tests.
- Do not blur responsibilities across layers.

## Deprecated repo-root agent docs
The following repo-root files were historical duplicates and are no longer authoritative:
- `AGENTS-INVESTMENT.md`
- `AGENTS-INVESTMENT-CATEGORY.md`
- `AGENTS-WG.md`

They have been moved under `docs/appendix/legacy/agents/` for reference only.
