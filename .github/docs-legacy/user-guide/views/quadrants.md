---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views/quadrants/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Diagnose a single item
  url: user-guide/views/flame-diagrams/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# Quadrants

Quadrants place two raw measures together so a team can ask which operating mode needs a
closer look. They show context for a selected scope and period; they do not score, rank,
or compare people.

## Purpose

Use a quadrant to start a system conversation when two measures may be moving together.
It is a hypothesis starter, not a conclusion about why the pattern exists.

## When to use

Use a quadrant after choosing a team or repository scope and a time window. It is useful
when a single trend cannot explain whether work is flowing, accumulating, or changing
repeatedly. Use a single-item [flame diagram](flame-diagrams.md) after the quadrant points
to a particular work item or delay.

## How to read

Read the axis labels, units, scope, and period first. Quadrants show **raw values only**:
the point is placed from the displayed measures, not from a hidden score. Compare a point
with its own recent windows and then follow evidence for the underlying work.

| Pair | Question it helps ask |
| --- | --- |
| Churn × Throughput | Is repeated code change moving alongside completed work? |
| Cycle Time × Throughput | Is work completing at a different pace as its cycle length changes? |
| WIP × Throughput | Is active work accumulating faster than it completes? |
| Review Load × Review Latency | Is review demand moving with waiting time? |

## Worked example

An illustrative Churn × Throughput point can show more changed lines in the selected window
while completed work stays similar. That combination may suggest rework, a large planned
change, or a difficult area. Open the related work before choosing between those
explanations; the quadrant itself does not choose one.

## Evidence path

From a point, inspect the selected window and scope, then open the linked work artifacts
when an evidence path is available. Read the issue, pull request, commit, or review context
with its date before making a team decision. The [evidence model](../../product/concepts.md)
explains why a chart is not enough on its own.

## Empty and error states

If a pair has no usable values for the selected scope or period, treat it as unavailable
information—not a zero point. Check the selected filters, source coverage, and page help
before widening the time window or choosing another view.

## Caveats

Quadrant boundaries are aids for discussion. A point near a boundary can move with a small
window change, and two teams with different work context should not be compared. Keep the
raw values, units, and evidence visible throughout the discussion.

## Next step

- [Diagnose a single item with a flame diagram](flame-diagrams.md).
- [Read the glossary](../glossary.md).
- [Find the right view](../views-index.md).
