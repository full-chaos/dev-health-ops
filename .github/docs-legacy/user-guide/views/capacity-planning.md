---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views/capacity-planning/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Follow work relationships
  url: user-guide/views/work-graph/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# Capacity Planning View

Capacity Planning helps a team explore how its selected **backlog** may move given recent delivery history. It is a planning aid, **not a promise** of a completion date.

## Purpose
Make a scenario explicit: which backlog, scope, and historical throughput are considered, and what uncertainty comes with that history.

## When to use
Use it when a team needs to discuss planned work against recent delivery, not when it needs a guarantee.

## Current behavior
The current **Capacity Planning** view uses a selected scope and date range to run a
**Monte Carlo** completion forecast from the selected **backlog** and **historical
throughput**. Its Completion Forecast can show backlog size, a completion projection,
and **P50**, **P85**, and **P95** ranges. The **Refresh Forecast** action recalculates
the visible scenario; it does not turn the range into a commitment.

## Planned behavior
Additional planning controls or scenario detail may be added over time. Treat features as planned until they are visible in the current workspace.

## How to read
Check backlog definition, time window, and historical throughput first. Read P50 as a
middle simulation outcome, P85 as a more cautious planning range, and P95 as a
conservative range. They are possible outcomes, not a single deadline.

## Worked example
An illustrative backlog can look achievable in a middle scenario while a cautious edge extends later because recent throughput varies. That suggests a scope and sequencing conversation, not an instruction to increase individual output.

## Evidence path
Open selected work and recent completion history when an evidence path is available. The [evidence model](../../product/concepts.md) keeps a planning range connected to its work and period.

## Empty and error states
An unavailable forecast can mean there is not enough completed-work history or the selected backlog is outside available context. Check scope, filters, source coverage, and page help.

## Caveats
Backlog changes, uneven work size, blocked work, and shifting scope can move a scenario. Revisit it as the plan changes; do not turn a range into a personal target or fixed commitment.

## Next step
- [Read PR Flow](pr-flow.md).
- [Read the glossary](../glossary.md).
- [Follow work relationships](work-graph.md).
