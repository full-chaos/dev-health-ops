---
page_id: use-capacity
summary: Understand the Completion Forecast page, the backlog and throughput history behind it, and the meaning of P50, P85, and P95 ranges.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - current /plan/capacity product surface
  - current Monte Carlo throughput forecast implementation
  - docs/user-guide/views/capacity-planning.md
applicability: current
lifecycle: active
---

# Completion Forecast

Completion Forecast explores when a selected backlog **might** complete if recent delivery history remains representative. The current page uses a Monte Carlo forecast over the selected work set and historical throughput. It is a planning aid, not a promise of a delivery date.
{: .fc-page-lede }

## When to use this page

Use Completion Forecast when a team has a defined backlog and needs to discuss a range of possible completion outcomes. It is useful for:

- comparing a planned work set with recent delivery history;
- exposing uncertainty rather than hiding it in one date;
- discussing scope, sequencing, and risk before making a commitment;
- revisiting a scenario after the backlog or delivery system changes.

Do not use the forecast as an individual productivity target or as evidence that a team must increase output to meet one percentile.

## What the page contains

The available controls depend on the current workspace, but the scenario is built from these parts:

1. **Backlog or work set** defines what remains to be completed.
2. **Team, repository, or other scope** identifies the delivery system whose history is used.
3. **Historical period** supplies completed-work observations for the simulation.
4. **Backlog size and completion projection** summarize the selected scenario.
5. **P50, P85, and P95 ranges** show increasingly cautious portions of the simulated outcomes.
6. **Refresh Forecast** recalculates the visible scenario from the current selections.

A refresh updates the simulation. It does not make the inputs more representative or turn a percentile into a commitment.

## Understand the percentiles

- **P50** is the middle of the simulated completion outcomes. Roughly half of the simulated outcomes complete by that point and half complete later.
- **P85** is a more cautious planning range. More of the simulated outcomes complete by that point.
- **P95** is a conservative range that includes most simulated outcomes, but it is still not a guarantee.

Read the ranges together. The distance between them communicates uncertainty: a wide spread means the recent throughput history produces a broader set of plausible outcomes.

## Check whether the scenario is representative

Before relying on the range, compare the planned work with the history used by the forecast:

- Is the backlog defined consistently and free of obviously excluded work?
- Is the historical period long enough to include normal variation?
- Did staffing, workflow, source coverage, or work type change materially?
- Is recent throughput dominated by unusually small, large, blocked, or batched work?
- Are backlog items being added, removed, or redefined while the forecast is discussed?

A mathematically valid simulation can still be a poor planning model when its inputs do not represent the future work.

## Worked example

Suppose a backlog has a P50 completion in late September, a P85 range in mid-October, and a P95 range in early November. A useful interpretation is:

> Recent delivery history produces a middle outcome around late September, but a cautious planning range extends into October and a conservative range into early November.

That spread should lead to questions about scope stability, dependencies, work size, and sequencing. It should not be collapsed into “the team will finish on September 28.”

## Compare scenarios

Change one assumption at a time and state it explicitly. For example:

- remove a clearly deferred work set;
- compare two backlog definitions;
- use an equivalent historical window after a documented workflow change;
- separate a dependency-bound group from independently deliverable work.

Record the changed input with the resulting range. Two screenshots with different scopes or histories are not a meaningful scenario comparison unless those differences are visible.

## Empty and error states

| State | Meaning | Next step |
| --- | --- | --- |
| No forecast | The backlog or completed-work history is unavailable or insufficient | Confirm scope, backlog definition, and source coverage. |
| Very wide range | Historical throughput varies substantially or the sample is small | Treat the result as high uncertainty and inspect the history. |
| Stale scenario | The backlog or history has changed since the forecast was computed | Refresh after confirming the intended inputs. |
| Unexpectedly narrow range | The history may be too short, uniform, or filtered | Verify the period and completed-work population. |

Use [No or incomplete data](../troubleshooting/no-or-incomplete-data.md) when the page cannot produce a usable scenario.

## What not to conclude

- A percentile is not a promised date.
- The forecast does not account for every future staffing, dependency, or scope change.
- A later range is not evidence of poor individual performance.
- A refreshed simulation does not correct an unrepresentative backlog or history.

## Continue

- [Compare trends responsibly](compare-trends.md)
- [Prepare a team conversation](team-conversations.md)
- [Read PR Flow](../delivery-flow/pr-flow.md)
