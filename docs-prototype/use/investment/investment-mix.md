---
page_id: use-investment-mix
summary: Read the Investment Mix treemap or sunburst without confusing size, share, and evidence quality.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - src/components/work/investment/charts/InvestmentMixSection.tsx
  - src/lib/investmentMix.ts
  - src/dev_health_ops/api/queries/investment.py
applicability: current
lifecycle: active
---

# Read Investment Mix

Investment Mix presents the positive effort-weighted contributions returned for the selected scope and period.

## Choose the view

- **Treemap:** compares relative size and supports theme or subcategory selection.
- **Investment mix / sunburst:** shows the theme-to-subcategory hierarchy.

The current interface describes treemap size as effort and uses evidence quality as opacity when quality data is available.

## Interpret the chart

1. Confirm the scope, period, and filters shown in the product.
2. Read the largest shapes as the largest contributions to the current total.
3. Use the displayed percentage as `contribution ÷ total positive contribution` for the current result.
4. Select a theme to focus its subcategories; select again or clear the focus to return to the full mix.
5. Treat opacity or quality labels as corroboration context, not a correctness score.
6. Follow the selected theme or subcategory to Work Graph or the available supporting-work action.

## Keep the units straight

The backend aggregates `subcategory probability × effort value`. The chart then compares those positive contributions. It does not count work items and it does not imply that every source item has equal weight.

## When the chart looks wrong

Use [Diagnose no or incomplete data](../troubleshooting/no-or-incomplete-data.md) when the result is empty, unexpectedly small, stale, or inconsistent with the selected scope. Use [Weighting and aggregation](../../reference/metrics/weighting-and-aggregation.md) for the exact contract.
