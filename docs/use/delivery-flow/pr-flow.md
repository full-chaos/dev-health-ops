---
page_id: use-pr-flow
summary: Understand the current Flow page, its state-transition Sankey, and the evidence needed to interpret waiting or repeated movement.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - current /metrics?tab=flow product surface
  - docs/user-guide/views/pr-flow.md
applicability: current
lifecycle: active
---

# PR Flow

PR Flow helps answer: **Where does work appear to move, wait, or return in the selected delivery system?** In the current **Metrics → Flow** destination, the **State Flow** tab presents a Sankey built from available work-item state transitions for the selected scope and period.
{: .fc-page-lede }

The view summarizes observed transitions between source state labels. It does not promise that every item exposes a complete lifecycle, and it does not treat a missing transition as proof that work skipped a state.

## When to use this page

Use PR Flow when a throughput, cycle-time, or delivery conversation needs more detail about **movement between states**. It is useful for questions such as:

- Where is transition volume concentrated?
- Is work repeatedly returning to an earlier state?
- Does a waiting or blocked state appear in a material share of transitions?
- Has the pattern changed across equivalent periods?

Use [Review pressure](review-pressure.md) when the specific question is review demand or waiting. Use [Quadrants](quadrants.md) when you need to examine two raw measures together rather than a state path.

## What the page shows

1. **Scope and period** determine which work items and transitions can contribute.
2. **State nodes** use the labels available from the current source and product mapping.
3. **Links** summarize observed transitions from one state to another.
4. **Link width** represents the relative amount of observed transition activity shown by the view.
5. **Supporting work or contextual actions**, when available, provide the evidence needed to explain a pattern.

State names and detail can differ by provider. Read the labels shown by the product rather than assuming a universal sequence such as open → review → merged.

## Read the Sankey

Start at a state node and follow its outgoing links. A wide link indicates that the displayed result contains more observed transitions along that path than along a narrower link. Read the whole pattern before focusing on one route:

- A large forward transition can be normal for the selected workflow.
- A visible return to an earlier state can indicate rework, clarification, deliberate iteration, or source-specific state behavior.
- A concentration around a waiting or blocked state can identify an area for evidence review, but the chart cannot explain the dependency or decision by itself.

Compare equivalent scopes, periods, and state mappings. Changing the repository set, provider, or time window can change both the population and the available state vocabulary.

## Worked example

Suppose the view shows a material path from **Active** to **Blocked**, followed by a path back to **Active**. A supported reading is:

> In the selected scope and period, a visible share of observed state transitions moved into Blocked and later returned to Active.

The view does not tell you why. Open representative work and inspect dependencies, review history, linked issues, and dates. The pattern may reflect an external dependency, missing information, a deliberate pause, or another workflow-specific cause.

## What a missing path means

A link can be absent because:

- no matching transition was observed in the selected result;
- the source does not expose that transition detail;
- the selected filters excluded the relevant work;
- synchronization or coverage is incomplete;
- state mapping differs from the workflow you expected.

Check coverage before converting absence into a process conclusion.

## Empty and partial states

| State | Interpretation | Next step |
| --- | --- | --- |
| No transitions | The current context produced no usable transition rows | Confirm scope, period, filters, and source coverage. |
| Partial transition set | Some providers, repositories, or periods are not represented | Avoid comparisons until coverage is understood. |
| Stale result | The latest expected source activity has not arrived | Check synchronization freshness. |
| Unavailable view | The feature, role, or source prerequisite is not available | Check workspace availability and permissions. |

Use [Diagnose no or incomplete data](../troubleshooting/no-or-incomplete-data.md) when the result is empty, stale, or inconsistent with the visible context.

## What not to conclude

- One long-lived or returned item is not a team-level trend.
- A state label has only the meaning defined by the current source and product mapping.
- Transition volume is not a measure of individual performance.
- A wide link identifies a pattern; it does not establish its cause.

## Continue

- [Read Quadrants](quadrants.md)
- [Investigate review pressure](review-pressure.md)
- [Follow relationships in Work Graph](../code-and-relationships/work-graph.md)
