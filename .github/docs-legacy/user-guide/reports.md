---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/reports/
owner: Dev Health documentation
last-reviewed: 2026-07-17
template: guide.html
next:
  label: Interpret shared metrics
  url: user-guide/metrics-interpretation/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# Report Center

Report Center turns a saved question about a selected scope and period into a repeatable
report. Use it to preserve the context, compare trends carefully, and return to the
source material before acting.

## Purpose

Create a repeatable report when a team needs the same scope, time window, and metric
context for a regular conversation. A report supports a shared reading; it is not a
scorecard for people.

## Create a report

In **Report Center**, choose **New report**. Give the report a **Name** and, when useful,
a **Description** that states the question it should help the group discuss. Set the
visible report settings for its scope, date range, and measures before saving.

Start with one question, such as “How has work moved through the platform team this
month?” Keep the scope and date range in the description so a later reader can tell what
the report covers.

## Clone a report

Choose **Clone** when an existing report is close to the question you need. The copy keeps
the source report's saved settings and receives a new name, so update the scope, period,
or description before using it for a different conversation. A clone does not bring over
the source report's schedule or run history.

## Schedule a report

Choose one fixed schedule: **None**, **Weekly**, or **Monthly**. **None** keeps the report
manual; **Weekly** and **Monthly** request recurring runs at the product's fixed cadence.
These are the complete scheduling choices in the current Report Center.

Choose a fixed schedule that lets the trend accumulate. A weekly run can support a team
review; a monthly run can suit a longer-term question.

## Run Now

Choose **Run Now** to request a new run immediately. The current run appears in **Run
history** while it is pending or in progress. Wait for a completed run before treating its
output as the report for the selected context.

## Read a completed report

A completed run presents **Rendered Markdown**. Read the report's scope, period, and
measures together before relying on a statement in the narrative. The current reading
surface does not show a separate provenance panel, so return to the linked work and source
context when a trend needs explanation.

Some narrative text can be **AI-generated**. That label means the text is a generated
summary of the available report context, not a conclusion about a person or a replacement
for the linked evidence. Keep calibrated language—appears, leans, and suggests—beside any
AI-derived interpretation.

## Empty and error states

No completed output can mean the report has not run successfully yet, its selected context
has no usable information, or the run needs attention. It does not mean that every measure
is zero. Check the report settings and Run history first. If a run needs operational
follow-up, use the [report failures operator details](../ops/runbook-report-failures.md).

## Caveats

Keep the same scope and date range when comparing report runs. A change in either can make
a difference appear larger or smaller than the underlying trend. Use the report to choose
a team question and an evidence path, not to rate contributors.

## Next step

- [Interpret shared metrics](metrics-interpretation.md) before comparing a measure.
- [How to read Dev Health](how-to-read-dev-health.md) explains the shared evidence model.
- [Glossary](glossary.md) defines the terms used across the manual.
