---
page_id: use-reports
summary: Understand Report Center definitions, runs, schedules, rendered output, and provenance.
content_type: landing
owner: product-analytics
source_of_truth:
  - full-chaos/dev-health-web docs/reports.md
  - current report GraphQL schema and workers
  - docs/user-guide/reports.md
applicability: current
lifecycle: active
hide:
  - toc
---

# Reports

Report Center turns a saved analytical question into a repeatable report. A report definition preserves the selected scope, period, measures, and plan; each run applies that definition and records its own status and output. The definition, the run, and the rendered result are different objects.
{: .fc-page-lede }

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

Definition
{: .fc-topic-card__label }

### [Create or clone a report](create-or-clone.md)

Name the question, save its scope and period, or copy an existing definition without confusing the copy with its source run history.

</article>

<article class="fc-topic-card" markdown>

Execution
{: .fc-topic-card__label }

### [Run and schedule](run-and-schedule.md)

Trigger a manual run, understand the available schedule choices, and monitor queued, running, completed, or failed executions.

</article>

<article class="fc-topic-card" markdown>

Result
{: .fc-topic-card__label }

### [Output and provenance](read-output-and-provenance.md)

Read rendered Markdown beside the definition and run that produced it, and distinguish generated narrative from measured source values.

</article>

</div>

## How Report Center is organized

- `/reports` lists saved reports and their current status.
- `/reports/new` creates a new definition.
- `/reports/[id]` shows the saved definition, run history, and latest successful output where available.

A report is useful when a group needs the same question and context for a recurring review. It is not a person-level scorecard, and an AI-generated narrative is not a replacement for the linked work or measured source values.

Weekly Review, Executive Summary, and Export History remain preview routes and are not documented as current supported destinations.
