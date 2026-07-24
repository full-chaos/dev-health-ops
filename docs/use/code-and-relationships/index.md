---
page_id: use-code
summary: Locate persistent code-area patterns and follow relationships from a result to its supporting work.
content_type: landing
owner: product-analytics
source_of_truth:
  - current Code and Complexity product surfaces
  - current /diagnose/work-graph product surface
  - docs/user-guide/views/code-hotspots.md
  - docs/user-guide/views/work-graph.md
applicability: current
lifecycle: active
hide:
  - toc
---

# Code and relationships

Use these views when a product signal needs source-level or relationship context. Code Hotspots helps locate areas with persistent change or complexity. Work Graph follows the supported links between issues, pull requests, commits, files, repositories, and other evidence.
{: .fc-page-lede }

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

Code-area patterns
{: .fc-topic-card__label }

### [Code Hotspots](code-hotspots.md)

Find files, paths, or modules where churn and complexity remain concentrated, then inspect the work and dependencies behind the pattern.

</article>

<article class="fc-topic-card" markdown>

Evidence relationships
{: .fc-topic-card__label }

### [Work Graph](work-graph.md)

Follow supported relationships across work artifacts without inferring ownership, intent, or causation from graph proximity.

</article>

<article class="fc-topic-card" markdown>

Exact model
{: .fc-topic-card__label }

### [Work Graph data model](../../reference/data-models/work-graph.md)

Look up the canonical node, relationship, identity, and attribution contracts used by the graph and related product surfaces.

</article>

</div>

## Choose the starting point

Start with **Code Hotspots** when the question is “Which code areas deserve inspection?” Start with **Work Graph** when you already have an issue, pull request, commit, file, Investment segment, or other artifact and need to understand its supported relationships.

A visible hotspot or graph path is not a verdict. Code concentration can reflect active delivery, a deliberate refactor, generated output, repeated repair, or another context. A relationship can be missing because the source does not expose it or synchronization is incomplete. Keep the selected scope and period, then open the underlying work before deciding what the pattern means.

The former Flame diagrams material remains source evidence only until a current canonical route and interaction are verified. It is not published as a supported v2 workflow.
