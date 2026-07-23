---
page_id: use-quadrants
summary: Understand the current Quadrants page, its raw measures and thresholds, and how to follow a point to evidence.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - current Quadrants product surface and zone calculations
  - docs/user-guide/views/quadrants.md
applicability: current
lifecycle: active
---

# Quadrants

Quadrants place two raw measures on the same chart so you can identify work or periods that deserve a closer look. The page is a **hypothesis starter**: it shows where a point sits for the selected scope and period, but it does not score people or decide why that point is there.
{: .fc-page-lede }

## When to use this page

Use a quadrant when one metric is not enough to frame the question. Common pairings can help ask whether:

| Pair | Question it helps ask |
| --- | --- |
| Churn × Throughput | Is repeated code change moving alongside completed work? |
| Cycle time × Throughput | Is completion volume changing as the time to finish work changes? |
| WIP × Throughput | Is active work accumulating faster than it completes? |
| Review load × Review latency | Is review demand moving with waiting time? |

The exact pairs, labels, units, thresholds, and population are defined by the current product surface. Do not carry the meaning of a zone from one pair into another.

## What the page shows

1. **Horizontal and vertical axes** identify the two raw measures and their units.
2. **Thresholds or zone boundaries** divide the current chart into visual regions.
3. **Points** represent the population and aggregation defined by the current view.
4. **Scope, period, and filters** determine which work can appear.
5. **Point details or supporting actions**, when available, provide the evidence path.

Quadrants use the displayed measures rather than a hidden composite performance score. Read the values and units first; the zone label is secondary.

## Read a point

1. Confirm the selected scope, period, and filters.
2. Read both axis labels and units.
3. Check how the current thresholds are defined.
4. Inspect the point's raw values and population coverage.
5. Compare the point with its own recent equivalent windows.
6. Open representative work before choosing an explanation or response.

A point near a boundary can move zones after a small window or threshold change. Treat boundaries as navigation aids, not hard scientific categories.

## Worked example

Suppose a **Churn × Throughput** point moves toward higher churn while throughput remains similar to an earlier equivalent period. The chart supports a question:

> What changed repeatedly in this period without a corresponding change in completed-work volume?

Possible explanations include a planned refactor, a large coordinated change, repeated repair, generated files, or an unstable area. The quadrant cannot choose between them. Follow the point to the related work, then use [Code Hotspots](../code-and-relationships/code-hotspots.md) when the question becomes code-area specific.

## Compare periods carefully

A valid comparison keeps the following stable or explicitly explains the difference:

- axis definitions and units;
- threshold method;
- team, repository, or other scope;
- time-window length;
- source and coverage population;
- filters and feature availability.

Do not compare two teams with materially different work contexts as though the chart normalizes those differences.

## Empty, sparse, and changed states

| State | Meaning | Response |
| --- | --- | --- |
| No usable points | One or both measures are unavailable for the selected context | Check scope, filters, source coverage, and metric prerequisites. |
| Sparse population | Too few items or periods support a stable pattern | Widen the evidence review, not automatically the analytical window. |
| Threshold changed | Zone membership is not directly comparable with an earlier chart | Compare raw values and document the changed threshold. |
| Axis changed | The chart now asks a different question | Do not reuse the earlier zone interpretation. |

Use [Diagnose no or incomplete data](../troubleshooting/no-or-incomplete-data.md) when a quadrant is empty or inconsistent with the visible context.

## What not to conclude

- A zone is not a diagnosis.
- A point does not establish causation between its two measures.
- A high or low value is not inherently good or bad without the work context.
- A quadrant must not be used to rank contributors.

## Continue

- [Read PR Flow](pr-flow.md)
- [Investigate review pressure](review-pressure.md)
- [Read Code Hotspots](../code-and-relationships/code-hotspots.md)
