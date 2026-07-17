---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views/work-graph/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Return to views and charts
  url: user-guide/views-index/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# Work Graph: follow relationships

## Canonical role
This page is the **canonical user journey** for following relationships from work to supporting evidence. The separate [Work Graph reference](../work-graph.md) keeps concise reference context without duplicating this journey.

## Purpose
Use the Work Graph to understand how an **issue**, **pull request**, **commit**, and **file** relate in a body of work. It supports navigation and evidence inspection, not scoring.

## When to use
Use it when PR Flow, Code Hotspots, or Investment needs relationship context across linked artifacts.

## Current behavior
The current **Work Graph Explorer** presents available relationships and their supporting
context when connected sources can provide them. Choose a **connection type** to inspect a
relationship slice, then use the visible Theme and Subcategory scope when it is available.
Missing links can also reflect incomplete coverage.

## Planned behavior
Richer exploration may be added over time. Treat it as planned until it is visible in the current workspace; this guide does not promise a relationship absent from the product.

## How to read
Start from work, follow the available relationship, then inspect support. In an Investment question, the path is **Theme → Subcategory → Evidence**; use this view when evidence needs relationship context.

## Worked example
An illustrative issue can lead to a pull request, its commits, and the files those commits changed. Read the linked work and dates before interpreting that path.

## Evidence path
Open a relationship's linked artifact and its context when an evidence path is available. The [evidence model](../../product/concepts.md) keeps the chain inspectable.

## Empty and error states
An unavailable or sparse relationship view can mean the source has not supplied a link, scope is narrow, or coverage is incomplete. Check filters and source context before treating an empty path as proof.

## Caveats
Relationship context can vary by source and link quality. Use it to navigate and discuss work as a team, not to assess individuals.

## Next step
- [Read PR Flow](pr-flow.md).
- [Read the glossary](../glossary.md).
- [Read the Work Graph reference](../work-graph.md).
