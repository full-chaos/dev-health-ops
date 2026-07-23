---
page_id: use-investment-flows
summary: Read how effort-weighted Investment contributions connect categories with repositories or teams.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - src/dev_health_ops/api/services/investment_flow.py
  - src/dev_health_ops/api/queries/investment.py
applicability: current
lifecycle: active
---

# Read Investment Flows

Use Investment Flows when the question is how effort-weighted categories connect with repositories, teams, or other supported scope nodes.

1. Preserve the same scope, period, and category filters used in Investment Mix.
2. Read link width as an aggregated contribution, not a count of work items.
3. Check unassigned nodes before concluding that a team or repository owns the flow.
4. Select a path and follow it to supporting work where the product offers that action.
5. Compare equivalent periods only after checking coverage and attribution changes.

Multi-repository work can be allocated across repositories by the current allocation model. A flow into `unassigned` is an attribution or mapping state, not a new Investment category.
