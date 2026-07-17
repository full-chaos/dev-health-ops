---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views/pr-flow/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Plan capacity
  url: user-guide/views/capacity-planning/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# PR Flow

PR Flow helps a team understand how pull requests move through their visible stages and where **review latency** adds waiting time. It describes work movement in the selected scope and period, not individual assessment.

## Purpose
Use this guide to ask where work is waiting, moving, or returning for more work before it is complete.

## When to use
Use PR Flow when a delivery trend needs more detail about stages, review waiting, or merge timing. Use a [flame diagram](flame-diagrams.md) when one pull request needs a closer single-item diagnosis.

## Current behavior
In **Metrics → Flow**, the current **State Flow** tab shows available work-item state
transitions and flow paths for the selected scope and period. In this guide, **PR stages**
means the visible stage labels and timing that connected sources make available; it does not
promise a complete lifecycle for every pull request.

**Review latency** is the time from PR creation to first review when those timestamps
are available. Keep the visible period and filters with any interpretation, and use a
missing review timestamp as a coverage limitation rather than a zero-duration stage.

## Planned behavior
Additional aggregate paths and drill-downs may be added as source coverage grows. Treat only controls and stages visible in the current workspace as available now.

## How to read
Start with stage labels and durations. Compare waiting and review with the surrounding period; one long pull request is not a system trend.

## Worked example
An illustrative pull request can show a short active stage and a longer review stage. Inspect linked review, dependency, and timing evidence before choosing an explanation.

## Evidence path
Open related pull requests, reviews, issues, and time context when an evidence path is available. The [evidence model](../../product/concepts.md) keeps a stage reading tied to artifacts.

## Empty and error states
No available stages or review dates mean there is not enough usable source information for the selected context. Check filters, connection coverage, and page help; do not read an empty result as instant flow.

## Caveats
Sources can expose different stage detail, and one pull request can contain an unusual pause. Keep the view focused on work context and use longer trends before choosing a team response.

## Next step
- [Diagnose one item with a flame diagram](flame-diagrams.md).
- [Read the glossary](../glossary.md).
- [Plan capacity](capacity-planning.md).
