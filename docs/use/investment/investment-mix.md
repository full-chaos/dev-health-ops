---
page_id: use-investment-mix
summary: Understand the Investment Mix page, its treemap and sunburst, its units, and the evidence behind a displayed share.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - src/components/work/investment/charts/InvestmentMixSection.tsx
  - src/lib/investmentMix.ts
  - src/dev_health_ops/api/queries/investment.py
  - docs/user-guide/views/investment-mix.md
  - docs/user-guide/journeys/investment-view.md
applicability: current
lifecycle: active
---

# Investment Mix

Investment Mix answers: **How does the selected body of work appear to be distributed across the canonical Investment themes and subcategories?** It presents an effort-weighted distribution for the current workspace scope, filters, and period. It does not count tickets, copy provider labels, or assign one permanent category to a person or WorkUnit.
{: .fc-page-lede }

## When to use this page

Use Investment Mix when you need a composition view: which kinds of work make up the selected result, how concentrated that mix is, and which subcategories deserve a closer evidence review.

Use a different surface when the question changes:

- [Investment Flows](investment-flows.md) shows how effort is distributed between themes and available scopes.
- [Investment Expense](investment-expense.md) shows how the composition changes over time.
- [Delivery flow](../delivery-flow/index.md) focuses on movement, waiting, and review rather than work mix.

## What the page contains

The available controls and labels depend on the current workspace, but the reading order is stable:

1. **Scope and period** define which work can contribute to the result.
2. **Filters** narrow the source set before the Investment aggregation is displayed.
3. **Treemap** compares the relative size of positive theme or subcategory contributions.
4. **Sunburst or hierarchical mix** shows the theme-to-subcategory relationship.
5. **Evidence quality** provides corroboration context when the product has quality data for the result.
6. **Selection and supporting-work actions** move from a visible segment to its subcategories or evidence.

## Understand the hierarchy

Investment uses a fixed vocabulary:

- **Theme** is the broad kind of organizational investment, such as Feature Delivery or Operational / Support.
- **Subcategory** is a more specific kind of work inside a theme.
- **WorkUnit** is the evidence container that connects the categorization to issues, pull requests, commits, incidents, and other supported artifacts.

```text
Theme
└── Subcategory
    └── supporting WorkUnits and source artifacts
```

WorkUnits are never peers to themes or subcategories. A WorkUnit can contribute probabilistically to more than one subcategory, so the chart is a distribution rather than a set of mutually exclusive ticket labels.

## Read the treemap

In the treemap, **area represents contribution to the current positive total**. A larger rectangle means the theme or subcategory contributes more of the effort-weighted result for the selected context.

The displayed share is based on the contribution returned by the Investment aggregation, not the number of records:

```text
displayed share = segment contribution ÷ total positive contribution
```

The contribution combines a WorkUnit's subcategory probability with its effort value before results are aggregated. Two source items therefore do not necessarily carry equal weight. See [Weighting and aggregation](../../reference/metrics/weighting-and-aggregation.md) for the exact contract.

Select a theme to inspect its subcategories. Clear or repeat the selection to return to the full distribution. Keep the original scope, period, and filters visible while drilling down so the meaning of the percentages does not change unnoticed.

## Read the sunburst or hierarchical mix

The hierarchical view makes the fixed relationship between themes and subcategories visible. Read from the center or parent segment outward:

1. Start with the theme distribution.
2. Open a theme whose share is material to the question.
3. Compare the subcategories inside that theme.
4. Follow a selected subcategory to the available supporting work.

Do not compare a subcategory percentage with a theme percentage as though they use different denominators unless the interface explicitly says so. The hierarchy explains how the selected result is composed.

## Read evidence quality

Evidence quality indicates how strongly independent signals corroborate the displayed categorization. When the interface uses opacity or a quality label:

- stronger corroboration can make a result more suitable for a direct evidence review;
- lower quality means the available evidence is sparse or weakly corroborated;
- lower quality does **not** mean the segment should be hidden or that the product has proven it wrong.

A low-quality result is a prompt to inspect the linked work and use more cautious language.

## Worked example

Suppose **Maintenance / Tech Debt** occupies a larger share than in an earlier equivalent period, and **maintenance.refactor** is the largest subcategory inside it.

A supported reading is:

> In this scope and period, the effort-weighted mix appears to lean more toward maintenance work, with refactoring contributing a material share.

That result does not explain why. Open the supporting WorkUnits and inspect the related pull requests, issues, commits, dates, and evidence quality. The underlying work may reflect an intentional platform migration, repeated repair, an unstable boundary, or another context that the chart alone cannot choose between.

## Distinguish result states

| Visible state | What it means | What to do |
| --- | --- | --- |
| Positive distribution | Supported inputs produced one or more positive contributions | Read the shares, quality, and evidence. |
| Measured zero | The supported calculation produced zero for the selected context | Confirm the exact metric contract and source set. |
| Empty result | No usable rows or required inputs were returned | Check scope, filters, availability, and coverage. |
| Partial result | Some expected sources or periods are missing | Review coverage before comparing the mix. |
| Stale result | A value exists but is older than the question requires | Check synchronization freshness. |
| Unavailable view | The feature, role, or prerequisite is not available | Check workspace availability and permissions. |

Use [Diagnose no or incomplete data](../troubleshooting/no-or-incomplete-data.md) when the chart is empty, unexpectedly small, stale, or inconsistent with the visible context.

## What not to conclude

- A share is not the percentage of tickets, pull requests, commits, or people.
- A theme is not a provider-native label or a user-configurable category.
- A larger segment is not automatically better or worse.
- Missing or partially covered work is not measured zero.
- A team-level distribution is not a person-level assessment.
- A visible association does not establish cause.

## Continue

- [Investigate where effort appears to be going](investigate-effort.md)
- [Follow investment evidence](follow-evidence.md)
- [Read the Investment taxonomy](../../reference/taxonomies/investment.md)
- [Look up weighting and aggregation](../../reference/metrics/weighting-and-aggregation.md)
