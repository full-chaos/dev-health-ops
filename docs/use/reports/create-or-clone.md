---
page_id: use-report-create
summary: Create a saved analytical question or clone an existing definition while keeping definitions, schedules, runs, and output distinct.
content_type: task-guide
owner: product-analytics
source_of_truth:
  - current Report Center create, update, and clone mutations
  - docs/user-guide/reports.md
applicability: current
lifecycle: active
---

# Create or clone a report

A saved report is a **definition of a repeatable question**. It stores the name, description, scope, date range, measures, and other supported plan fields shown by Report Center. It does not become a result until a run completes successfully.
{: .fc-page-lede }

## Decide whether to create or clone

Create a report when the question and context are new. Clone one when an existing definition is close enough to be a useful starting point.

A clone copies the source definition into a new report with a new identity. Review every copied field before running it. The current clone behavior does not copy the source report's schedule or run history.

## Write the question into the definition

Open **Report Center** and choose **New report**. Use the **Name** for a concise, recognizable question and the **Description** to explain what the group intends to learn.

A useful description records the context that a future reader might otherwise miss:

> Monthly platform-team delivery review for the `platform/*` repository scope. Compare the current calendar month with the equivalent prior month and follow material changes to source work before selecting an action.

Avoid names such as “Team score” or “Productivity report.” Reports preserve analytical context; they do not rate people.

## Configure the saved context

Set the fields exposed by the current form, including the supported:

- workspace, team, repository, or other scope;
- date range and comparison period;
- measures or report plan;
- description and intended audience;
- schedule, where the current deployment exposes it.

The definition determines what later runs ask. Changing the scope or period changes the question even when the report name stays the same.

## Save and verify the definition

After saving, open the report detail page and read the stored settings back. Confirm that:

- the intended scope is present;
- the date range and comparison are correct;
- the measures match the question;
- the description states the purpose and limits;
- no copied or default field silently changes the population.

Run the report before sharing it. A saved definition with no successful run has no current output to interpret.

## Clone an existing report

1. Open the report whose definition is the closest match.
2. Choose **Clone**.
3. Give the copy a name that distinguishes its question or audience.
4. Review the copied scope, range, measures, and description.
5. Save the copy and run it independently.

Do not assume the clone is comparable with the source merely because it began with the same fields. Record every changed assumption and compare only equivalent run contexts.

## Definition versus run

| Object | What it contains |
| --- | --- |
| Report definition | Name, description, scope, period, measures, plan, and supported schedule settings |
| Report run | Trigger, status, timestamps, duration, and execution result for one invocation |
| Rendered output | The latest successful narrative or result associated with a run |

Use [Run and schedule](run-and-schedule.md) to create an execution and [Output and provenance](read-output-and-provenance.md) to read its result.
