---
page_id: use-pr-flow
summary: Understand the Flow tab of the Metrics page, its metric tiles, its Cycle Time × Throughput quadrant, and the evidence needed to interpret waiting or repeated movement.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - current /metrics?tab=flow product surface
applicability: current
lifecycle: active
---

# PR Flow

PR Flow helps answer: **Where does work appear to wait or slow down in the selected delivery system?** In the current **Diagnose → Flow** destination (`/metrics?tab=flow`), the **Flow** tab shows four metric tiles, a **Cycle Time × Throughput** quadrant, and two cards that list the likely associations and the primary contributors of the tab's highlighted metric.
{: .fc-page-lede }

The page summarizes measured values for the selected scope and period. It does not draw a path between states, and it does not promise that every item exposes a complete lifecycle.

## When to use this page

Use PR Flow when a throughput, cycle-time, or delivery conversation needs more detail about **where time accumulates**. It is useful for questions such as:

- Is cycle time moving with throughput, or against it?
- Is work in progress saturated, or is blocked work visible?
- Which associations and contributors appear with a change in cycle time?
- Has the pattern changed across equivalent periods?

Use [Review pressure](review-pressure.md) when the specific question is review demand or waiting. Use [Quadrants](quadrants.md) when you need to examine two raw measures together in more detail.

## What the page shows

1. **Scope and period** in the scope bar determine which work contributes.
2. **Metric tiles** show Cycle Time, Review Latency, WIP Saturation, and Blocked Work, each with its change from the previous equivalent period. A tile with no data says so; it never shows zero for a missing value.
3. **The quadrant** plots Cycle Time against Throughput. The **Show interpretive overlay** checkbox turns the zone overlay on and off.
4. **Likely associations** lists the percent change of related measures in the selected window. No causal conclusion is added.
5. **Primary contributors** lists where the impact concentrates in the window.
6. **View evidence** in the page header opens the evidence drawer for Cycle Time.

The **DORA** and **Throughput** tabs of the same page use the same layout with other tiles and quadrants (see [Quadrants](quadrants.md)).

## Read the quadrant

Read the position of the plotted points against both axes, then read the whole pattern before focusing on one point:

- Rising cycle time with stable throughput can indicate coordination debt.
- Falling throughput with rising cycle time can indicate a queue that is building.
- A zone is a heuristic label for a region of the chart. It is not a verdict.

Compare equivalent scopes and periods. Changing the repository set, team, or time window changes the population.

## Worked example

Suppose Cycle Time rises across two equivalent windows while Throughput stays flat, and Blocked Work shows a visible value. A supported reading is:

> In the selected scope and period, cycle time appears longer for a similar amount of completed work, and blocked work is visible.

The page does not tell you why. Open the evidence drawer and representative work, and inspect dependencies, review history, linked issues, and dates.

## What a missing value means

A tile or point can be empty because:

- no matching data exists in the selected result;
- the source does not expose that detail;
- the selected filters excluded the relevant work;
- synchronization or coverage is incomplete.

Check coverage before converting absence into a process conclusion.

## Empty and partial states

| State | Interpretation | Next step |
| --- | --- | --- |
| No data on a tile | The current context produced no usable rows for the metric | Confirm scope, period, filters, and source coverage. |
| Quadrant unavailable | The quadrant has no data for this scope | Select a scope with enough data, or widen the window. |
| Stale result | The latest expected source activity has not arrived | Check synchronization freshness. |
| Unavailable view | The feature, role, or source prerequisite is not available | Check workspace availability and permissions. |

Use [Diagnose no or incomplete data](../troubleshooting/no-or-incomplete-data.md) when the result is empty, stale, or inconsistent with the visible context.

## What not to conclude

- One slow or blocked item is not a team-level trend.
- A tile value has only the meaning defined by the metric's definition.
- These measures are not a measure of individual performance.
- An association identifies a pattern; it does not establish its cause.

## Continue

- [Read Quadrants](quadrants.md)
- [Investigate review pressure](review-pressure.md)
- [Follow relationships in Work Graph](../code-and-relationships/work-graph.md)
