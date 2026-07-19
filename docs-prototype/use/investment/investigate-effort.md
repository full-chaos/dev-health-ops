---
page_id: use-investment-investigate-effort
summary: Determine where effort appears to be going and follow the result to supporting work.
content_type: task-guide
owner: product-analytics
source_of_truth:
  - src/dev_health_ops/api/services/investment.py
  - src/dev_health_ops/api/queries/investment.py
  - src/components/work/investment/charts/InvestmentMixSection.tsx
applicability: current
lifecycle: active
---

# Investigate where effort appears to be going

Use this workflow to inspect an effort-weighted distribution for one scope and period, then follow a selected theme or subcategory to supporting work.

## Before you begin

Confirm the workspace, repository or team, time window, and data coverage. Preserve that context while moving between the Mix view and evidence.

## Read the result

1. Open the Investment area and start with **Investment Mix**.
2. Keep the full distribution visible before focusing on a single theme.
3. Read size as an effort-weighted contribution—not as a count of tickets, commits, or people.
4. Select a theme or subcategory that materially affects the question.
5. Check evidence quality and coverage before treating a difference as meaningful.
6. Follow the selection to supporting work before choosing an action.
7. For comparisons, use equivalent scopes and periods and state any coverage difference.

## Distinguish the states

| State | Interpretation |
| --- | --- |
| Measured positive contribution | Supported source data produced a positive effort-weighted contribution. |
| Measured zero | The supported calculation produced zero for the selected context. |
| Empty response | Required tables, fields, scope, or matching rows may be absent; diagnose before interpreting. |
| Incomplete or stale | Some expected input has not arrived or is older than the question requires. |
| Low or unknown evidence quality | The categorization has weaker or missing corroboration; inspect supporting work. |

## What not to conclude

- A percentage is not the percentage of tickets.
- A theme is not a provider-native label or a manual tag.
- Missing or incomplete evidence is not zero.
- A team-level signal is not a person-level judgment.
- An association or concentration does not establish cause.

## Continue

- [Read Investment Mix](investment-mix.md)
- [Follow investment evidence](follow-evidence.md)
- [Diagnose no or incomplete data](../troubleshooting/no-or-incomplete-data.md)
- [Look up weighting and aggregation](../../reference/metrics/weighting-and-aggregation.md)
