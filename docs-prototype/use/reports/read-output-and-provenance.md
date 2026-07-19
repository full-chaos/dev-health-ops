---
page_id: use-report-read
summary: Read rendered report output beside the definition and run that produced it, and distinguish narrative from measured source values.
content_type: task-guide
owner: product-analytics
source_of_truth:
  - current SavedReport and ReportRun GraphQL contracts
  - docs/user-guide/reports.md
applicability: current
lifecycle: active
---

# Read report output and provenance

A completed Report Center run can present **Rendered Markdown** together with its saved definition and run history. Read those pieces as one result. The narrative is meaningful only for the scope, period, measures, coverage, and execution that produced it.
{: .fc-page-lede }

## Identify the exact output

Before reading the narrative, confirm:

- report name and description;
- saved scope and date range;
- comparison period, when used;
- measures or report plan;
- run status and trigger;
- run start and completion time;
- whether the visible output belongs to the latest successful run or an older one.

A newer failed run can coexist with older successful output. Do not assume the first visible narrative was produced by the most recent execution.

## Distinguish narrative from source values

The report can combine measured or derived values with explanatory prose. Some narrative can be **AI-generated**. That label means the product generated a summary from the available report context; it does not make the prose a measured fact or a conclusion about a person.

Use this reading order:

1. Confirm the saved question and execution.
2. Identify the measured values, periods, units, and comparison.
3. Read the generated or authored narrative as an interpretation of that context.
4. Follow linked work or source views for material claims.
5. Keep coverage and freshness limitations with any shared excerpt.

Use calibrated language such as **appears**, **leans**, and **suggests** where the report summarizes model-derived or associative signals.

## Understand provenance

The current reading surface uses the saved definition and run history as its primary provenance. It does not guarantee a separate downloadable artifact or complete provenance panel in every deployment.

A useful provenance record includes:

| Element | Why it matters |
| --- | --- |
| Definition | States the analytical question and population |
| Run ID and trigger | Identifies the exact execution and whether it was manual or scheduled |
| Completion time | Shows when the output became current |
| Scope and period | Defines what work can contribute |
| Measures and plan | Explain which calculations and narrative structure were requested |
| Linked work or views | Provide the evidence path for an explanation |

A future artifact URL field or preview route does not guarantee that export is currently supported.

## Worked example

Suppose a completed monthly report says that maintenance-related effort appears higher than in the equivalent prior period. Before repeating that statement:

1. Confirm the two periods use the same team and repository scope.
2. Check whether source coverage and freshness are comparable.
3. Identify the measured Investment values behind the narrative.
4. Open the contributing subcategories and supporting work.
5. Add the relevant context—for example, a planned migration—when sharing the result.

The report helps preserve and repeat the question. It does not eliminate the evidence review.

## Empty, missing, or failed output

| State | Meaning | Next step |
| --- | --- | --- |
| No successful output | The report has not completed successfully | Review run history and trigger a run only after confirming the definition. |
| Failed latest run | The latest execution produced no usable result | Preserve the run context and troubleshoot the failure. |
| Empty narrative | The selected context may have no usable information | Check scope, measures, coverage, and the run status; do not read it as all-zero data. |
| Stale output | A successful result exists, but a newer run has not completed | Identify the exact completion time before sharing it as current. |

Use [Report problems](../troubleshooting/reports.md) when output is missing, failed, or inconsistent with the saved definition.

## Share safely

Report output can contain customer-sensitive context. Share it only with the approved audience and preserve its scope, period, completion time, and caveats. Do not copy generated narrative into a broader channel without the context needed to understand it.
