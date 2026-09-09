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

## Repository coverage is two claims, not one

The repository coverage figure beside a flow reports two different things
added together, so the product shows them apart as well.

- **Direct coverage** is work whose repository is known: the work item's own
  code evidence, or a repository inherited from its parent or child issue.
- **Team-fallback coverage** is work whose repository is not known. Only the
  owning team is known, so its effort is spread evenly across every repository
  that team owns.
- **Fan-out width** is how many distinct repositories each of those units was spread across. It counts repositories, not rows, so it reports the same width whether or not you have filters applied.

Read the fan-out width before you trust a high coverage number. A team that
owns nine repositories turns one unresolved work item into nine repository
rows, and the combined figure counts all nine as covered. That is why coverage
can read near 100 percent while the work is no better attributed than it was:
the number grew because the fallback is broad, not because the evidence
improved.

Act on the split, not the total. High direct coverage means the repository
picture can carry a decision. High team-fallback coverage with a wide fan-out
means the opposite -- treat those repository totals as an even spread across a
team's estate, and go improve issue-to-code linking before comparing
repositories against each other.

Either figure can be blank rather than zero. Blank means the view could not
measure the split; zero means it measured it and found none.
