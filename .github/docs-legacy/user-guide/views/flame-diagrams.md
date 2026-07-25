---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views/flame-diagrams/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Find the right view
  url: user-guide/views-index/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# Flame diagrams

Flame diagrams explain a **single item** by decomposing where its time, waiting, rework, or
other contribution came from. They diagnose one item; they do not compare a fleet of items.

## Purpose

Use a flame diagram to ask why one selected pull request, work item, or delivery took the
shape it did. Each segment is part of an evidence trail, not a verdict about the people who
worked on it.

## When to use

Start here after a broader view, such as [Quadrants](quadrants.md), identifies a period or
item worth understanding. Do not use a flame diagram to prove a portfolio-wide trend; use a
trend view for that question.

## How to read

Start at the whole selected item, then read its visible segments from the broadest duration
or contribution toward the more specific parts. A wide waiting or review segment is a prompt
to inspect its supporting work and timestamps. It does not, by itself, explain the cause.

## Worked example

An illustrative pull request can show a short active change segment followed by a longer
waiting-for-review segment. That shape suggests a question about review flow in that window.
Open the review and linked work evidence before deciding whether the delay was availability,
batching, a dependency, or something else.

## Evidence path

Follow the selected segment to its linked work artifacts when an evidence path is available.
Keep the pull request, issue, commit, or delivery context beside the segment so the
explanation remains inspectable. The [evidence model](../../product/concepts.md) describes
the shared source-to-interpretation path.

## Empty and error states

If there is no timeline or contribution data for the selected item, the diagram cannot
explain it. Check that the item and time window are correct, then use the page help or return
to a broader view. Do not treat an empty diagram as proof that no work happened.

## Caveats

Segments can overlap, and available sources can be incomplete. A flame diagram shows the
recorded shape of one item, not the quality of a person or a team. Preserve the item scope
and evidence links when sharing the interpretation.

## Next step

- [Inspect code hotspots](code-hotspots.md) for a broader code-area question.
- [Read the glossary](../glossary.md).
- [Return to views and charts](../views-index.md).
