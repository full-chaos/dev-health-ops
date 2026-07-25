---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views/code-hotspots/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Inspect the work graph
  url: user-guide/views/work-graph/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# Code Hotspots

Code Hotspots highlights where code **churn** and **complexity** persist in the selected
scope and period. It helps a team decide where to inspect work, tests, and dependencies;
it is not blame for the people who changed the code.

## Purpose

Use this view to locate areas that repeatedly change or remain hard to understand. It turns
a broad durability question into a path for inspecting evidence and context.

## When to use

Use Code Hotspots when a flow, quality, or maintenance conversation points to a code area.
It works well alongside a time-based heatmap or a tree of repository, path, and file. Do
not use it to compare contributors or assign ownership of a problem.

## How to read

Read the selected time window, repository scope, and measure first. A concentrated area can
reflect active delivery, a deliberate refactor, an unstable interface, or recurring fixes.
Look for persistence across windows before treating a one-period change as a durable signal.

## Worked example

An illustrative path can show repeated churn over several windows together with rising
complexity. That combination suggests a useful question: is the area receiving planned
change, repeated repair, or an accumulating design cost? Read the linked work before choosing
a response such as splitting work, improving tests, or scheduling maintenance.

## Evidence path

Open the relevant heatmap cell or tree branch when an evidence path is available, then inspect
the linked pull requests, commits, issues, and time window. The [evidence model](../../product/concepts.md)
keeps the path from a visual pattern to its supporting artifacts visible.

## Empty and error states

An empty hotspot view can mean there is no usable activity for the selected filters or that
coverage is incomplete. Check the scope, window, and source connection before reading an
empty result as low churn or low complexity.

## Caveats

Hotspots describe code-area patterns, not people. Churn and complexity need the surrounding
delivery context, and an area with visible change is not automatically unhealthy. Keep this
view out of performance conversations: it is a guide for inspection, **not blame**.

## Next step

- [Use Quadrants](quadrants.md) to ask how the broader system mode relates to the pattern.
- [Read the glossary](../glossary.md).
- [Follow work relationships](work-graph.md) when linked artifacts need more context.
