---
page_id: use-delivery
summary: Understand how work moves, waits, returns, and accumulates in the selected delivery system.
content_type: landing
owner: product-analytics
source_of_truth:
  - current /metrics?tab=flow product surface
  - docs/user-guide/views/pr-flow.md
  - docs/user-guide/views/quadrants.md
applicability: current
lifecycle: active
hide:
  - toc
---

# Delivery flow

Delivery-flow views describe how work moves through the states and review activity available for the selected scope and period. They help a team locate waiting, repeated movement, and pressure in the system. They do not score people or choose the cause of a delay.
{: .fc-page-lede }

## Choose the question

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

State movement
{: .fc-topic-card__label }

### [PR Flow](pr-flow.md)

Read the current state-transition Sankey to see which available states work moved between and where transition volume concentrates.

</article>

<article class="fc-topic-card" markdown>

Paired measures
{: .fc-topic-card__label }

### [Quadrants](quadrants.md)

Place two raw measures together to identify work or periods that deserve a closer evidence review.

</article>

<article class="fc-topic-card" markdown>

Review demand
{: .fc-topic-card__label }

### [Review pressure](review-pressure.md)

Examine aggregate review demand, waiting, and availability without turning the result into a reviewer ranking.

</article>

</div>

## Read a flow result

1. Confirm the repository or team, time window, and filters.
2. Read the state names, axis labels, measures, and units shown by the current view.
3. Look for a pattern across several items or equivalent periods rather than one exceptional item.
4. Open representative work and its dates, dependencies, reviews, or state history.
5. Check coverage before interpreting a missing transition or empty zone as evidence.

A flow pattern can support several explanations. Work moving back to an earlier state, for example, can reflect rework, deliberate iteration, an external dependency, or source-specific state mapping. The visualization locates the question; the linked work and its context distinguish the explanations.

## Related views

Use [Investment](../investment/index.md) when the question is about the composition of work. Use [Code and relationships](../code-and-relationships/index.md) when a flow pattern points to a code area or requires a relationship path across artifacts.
