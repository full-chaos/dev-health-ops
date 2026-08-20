# ADR-002: Investment WorkUnit Component Scope for Period Views

**Status**: ACCEPTED (2026-06-11) — keep current cross-period components; revisit when it becomes an issue  
**Created**: 2026-06-11  
**Parent Issues**: CHAOS-2326

## Context

`dev-hops investment materialize` currently forms connected components from all fetched
`work_graph_edges` for the selected repo/team scope, then filters the resulting WorkUnits
by component time bounds (`from_ts`/`to_ts`). The `--from`/`--to` window therefore decides
which WorkUnits are written, not which edges are allowed to connect into a WorkUnit.

This preserves stable WorkUnit identities across repeated materializations, but long-lived
issue/PR/commit chains can keep accumulating historical edges. For period-scoped
Investment View reads, that can make components historically sticky and larger than the
period a user is inspecting.

## Question

Should period-scoped Investment View materialization build components only from edges in
the requested period, or keep using cross-period components and filter after component
formation?

## Options

| Option | Description | Pros | Cons |
| ------ | ----------- | ---- | ---- |
| A: Cross-period components (current) | Build components from all fetched edges, then filter by component bounds. | Stable WorkUnit IDs; complete provenance for long-lived work; no accidental fragmentation. | Components can grow large; period views may include historical context outside the visible window. |
| B: Period-bounded components | Filter edges/nodes to the period before component construction. | Period views are strictly local; bounds component growth. | Same real-world work can split across periods; WorkUnit IDs churn; explanations lose older evidence. |
| C: Hybrid with explicit lookback | Build from period edges plus a bounded lookback window. | Caps growth while retaining near-period context. | Adds a product/config decision; IDs may still change when lookback changes. |

## Recommendation

Keep **Option A** as the safe default for now and document it as intentional until Product
chooses a stricter period semantics model. Do **not** implement bounding in CHAOS-2326
because the correct behavior is a product decision: stable cross-period WorkUnits and
strict period-local WorkUnits optimize for different user expectations.

If Product wants strictly period-local Investment View semantics, implement **Option C**
first rather than jumping directly to Option B. A bounded lookback should be explicit in
the UI/docs and included in the WorkUnit ID strategy so users understand why a component's
evidence set can differ by reporting window.

## Decision

> **ACCEPTED — Option A (keep current cross-period components).** Product sign-off
> (2026-06-11): preserve stable WorkUnit identity and complete provenance over strict
> period locality for now. Revisit — preferring **Option C** (bounded lookback) over Option
> B — if/when period locality becomes a real user problem. No materializer changes are made.

## Implementation Notes

- No materializer code changes are included with this ADR.
- Existing docs should continue to state that `--from`/`--to` bound materialization output,
  not component formation.
- If bounding is later approved, add tests that compare WorkUnit IDs and evidence contents
  across adjacent windows before changing `materialize.py`.
