---
page_id: use-investment
summary: Understand where effort appears to be going, how the Investment views differ, and how to follow a result to evidence.
content_type: landing
owner: product-analytics
source_of_truth:
  - src/dev_health_ops/api/services/investment.py
  - src/dev_health_ops/api/queries/investment.py
  - docs/user-guide/views/investment-mix.md
  - docs/user-guide/journeys/investment-view.md
applicability: current
lifecycle: active
hide:
  - toc
---

# Investment

Investment describes how effort appears to be distributed across a fixed set of themes and subcategories for the selected scope and period. Provider labels, issue types, and ticket categories are inputs to the model; they are not presented as the product's final explanation.
{: .fc-page-lede }

Use these views when the question is about **work mix and its supporting evidence**. Use [Delivery flow](../delivery-flow/index.md) when the question is primarily about movement, waiting, or review pressure, and use [Code and relationships](../code-and-relationships/index.md) when the question begins with a code area or linked artifacts.

## Start with the investigation

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

Guided workflow
{: .fc-topic-card__label }

### [Investigate where effort appears to be going](investigate-effort.md)

Move from the selected scope and period to the theme mix, evidence quality, subcategories, and supporting WorkUnits.

</article>

<article class="fc-topic-card" markdown>

Composition
{: .fc-topic-card__label }

### [Investment Mix](investment-mix.md)

Read the treemap or sunburst as an effort-weighted distribution, not a ticket count or permanent label.

</article>

<article class="fc-topic-card" markdown>

Evidence
{: .fc-topic-card__label }

### [Follow investment evidence](follow-evidence.md)

Inspect the issues, pull requests, commits, incidents, and other WorkUnit evidence behind a selected theme or subcategory.

</article>

</div>

## Compare different Investment surfaces

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

### [Investment Flows](investment-flows.md)

Use the Sankey view to examine how effort is distributed between themes and available team or repository scopes.

</article>

<article class="fc-topic-card" markdown>

### [Investment Expense](investment-expense.md)

Use the stacked time series to distinguish short-lived spikes from gradual changes in the composition of work.

</article>

<article class="fc-topic-card" markdown>

### [Weighting and aggregation](../../reference/metrics/weighting-and-aggregation.md)

Look up the exact calculation contract when you need to distinguish probability, effort value, contribution, and displayed share.

</article>

</div>

## How the model is organized

```text
WorkUnit evidence
      ↓
Subcategory — the specific kind of investment
      ↓
Theme — the broad investment category
```

A WorkUnit is an evidence container, not a category. One WorkUnit can contribute to more than one subcategory. Evidence quality describes how strongly independent signals corroborate the categorization; it is not a separate correctness score.

The fixed themes and subcategories are defined in the [Investment taxonomy](../../reference/taxonomies/investment.md). Investment does not rank people, reproduce provider-native labels as truth, or establish why a pattern exists without the linked work and its context.
