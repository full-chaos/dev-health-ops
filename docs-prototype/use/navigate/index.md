---
page_id: use-navigate
summary: Define the scope, time window, filters, comparison, and data state that give every Dev Health result its meaning.
content_type: landing
owner: product-analytics
source_of_truth:
  - docs/user-guide/how-to-read-dev-health.md
  - current product navigation and filter controls
applicability: current
lifecycle: active
hide:
  - toc
---

# Context and filters

Every Dev Health result is calculated for a particular workspace, scope, period, filter set, and available source population. Preserve that context as you move from a summary to evidence or compare one period with another; otherwise the question changes while the number appears to stay the same.
{: .fc-page-lede }

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

### [Scope and time](scope-and-time.md)

Choose the workspace, team, repository, and period that define which work can contribute to a result.

</article>

<article class="fc-topic-card" markdown>

### [Filters and comparisons](filters-and-comparisons.md)

Understand which controls narrow the source population and which ones create a comparison with a different period or scope.

</article>

<article class="fc-topic-card" markdown>

### [Data states](data-states.md)

Distinguish loading, measured zero, unavailable, empty, stale, delayed, and partially covered results before interpreting them.

</article>

</div>

## Read the context before the result

Before comparing a chart, metric, or report, confirm:

- the active workspace;
- team, repository, or other scope;
- time-zone and date boundaries;
- active filters and exclusions;
- comparison period, when present;
- source freshness and coverage;
- units and aggregation level.

A value is meaningful only beside those choices. An empty or unavailable value is not automatically zero, and a difference between two views can come from changed scope or coverage rather than changed work.

## Keep context through the evidence path

When you open a segment, point, report, or Work Graph relationship, keep the originating context visible or record it. The supporting artifact may have its own dates and provider labels; verify that it belongs to the selected analytical window before using it to explain the result.
