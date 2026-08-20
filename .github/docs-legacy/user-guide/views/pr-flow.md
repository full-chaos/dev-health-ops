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

PR Flow helps a team understand how work moves between available states in the selected scope and period. It describes aggregated work-item movement, not individual assessment.

## Purpose
Use this guide to ask where work is waiting, moving, or returning for more work before it is complete.

## When to use
Use PR Flow when a delivery trend needs more detail about state movement or returns to an earlier state. Use a [flame diagram](flame-diagrams.md) when one item needs a closer single-item diagnosis.

## Current behavior
In **Metrics → Flow**, the current **State Flow** tab is a **work-item state-transition Sankey**
for the selected scope and period. Its nodes are available state labels and its
links summarize observed state transitions between those labels. It does not promise a complete
lifecycle for every item.

Keep the visible period and filters with any interpretation. A missing transition can be a
coverage limitation, not evidence that work moved directly from one state to another.

## Planned behavior
Additional transition detail and drill-downs may be added as source coverage grows. Treat
only controls and states visible in the current workspace as available now.

## How to read
Start with state labels and the relative size of their transition links. Compare a pattern
with the surrounding period; one item moving back is not a system trend.

## Worked example
An illustrative flow can show work moving from active to blocked and then back to active.
Inspect linked work, dependencies, and time context before choosing an explanation.

## Evidence path
Open related work items, dependencies, and time context when an evidence path is available.
The [evidence model](../../product/concepts.md) keeps a state-transition reading tied to artifacts.

## Empty and error states
No available transitions mean there is not enough usable source information for the selected
context. Check filters, connection coverage, and page help; do not read an empty result as
instant flow.

## Caveats
Sources can expose different state detail, and one item can have an unusual transition.
Keep the view focused on work context and use longer trends before choosing a team response.

## Next step
- [Diagnose one item with a flame diagram](flame-diagrams.md).
- [Read the glossary](../glossary.md).
- [Plan capacity](capacity-planning.md).
