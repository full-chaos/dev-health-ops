---
page_id: use-investment-evidence
summary: Inspect the work and source artifacts that support an Investment selection.
content_type: task-guide
owner: product-analytics
source_of_truth:
  - src/components/work/investment/charts/InvestmentMixSection.tsx
  - src/lib/workGraphDrilldownUrl.ts
  - src/dev_health_ops/api/queries/work_unit_investments.py
applicability: current
lifecycle: active
---

# Follow investment evidence

Use evidence to understand why a theme or subcategory appears in the selected scope. Do this before recommending a change.

## Preserve the question

Keep the same workspace, repository or team, time window, and filters when opening the supporting-work view. Changing context silently changes the question.

## Inspect the supporting work

1. Select a theme or subcategory in Investment Mix.
2. Open the available Work Graph or supporting-work action.
3. Review the work units and linked source artifacts available for that selection.
4. Check whether issues, pull requests, commits, or relationships are missing for the period.
5. Compare the visible evidence with the evidence-quality and coverage state.
6. Return to the distribution without changing scope when you need to compare another theme.

## Use calibrated language

Say that the evidence **supports**, **suggests**, or **is consistent with** the categorization. Do not claim that the view proves intent, cause, performance, or an individual’s contribution.

## If evidence is absent

Absence can be a coverage, permission, source, processing, or applicability problem. Use [Diagnose no or incomplete data](../troubleshooting/no-or-incomplete-data.md) and retain the scope, period, selected category, and visible status for escalation.
