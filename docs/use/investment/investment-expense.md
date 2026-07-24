---
page_id: use-investment-expense
summary: Read how Investment composition changes over time without treating the view as accounting expense.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - current Investment time-series and analytics contracts
applicability: current
lifecycle: active
---

# Read Investment Expense

This time-series view asks how the effort-weighted Investment composition changes across the selected period. “Expense” describes sustained effort pressure; it is not a financial ledger.

1. Confirm the interval and aggregation grain.
2. Read abrupt changes as prompts to inspect incidents, releases, or coverage—not as causes by themselves.
3. Read gradual changes across several points before describing a structural shift.
4. Keep the same scope and source coverage when comparing periods.
5. Follow the relevant theme to supporting work.

Use [Weighting and aggregation](../../reference/metrics/weighting-and-aggregation.md) for the contribution contract and [Stale or delayed results](../troubleshooting/stale-or-delayed-results.md) when the series stops advancing.
