---
page_id: use-report-run
summary: Trigger a report, understand manual and scheduled executions, and monitor its run history without confusing a pending run with a result.
content_type: task-guide
owner: product-analytics
source_of_truth:
  - current Report Center Run Now action and ReportRun contract
  - docs/user-guide/reports.md
applicability: current
lifecycle: active
---

# Run and schedule a report

A report run applies one saved report definition at a particular time. The run records its trigger, state, timing, and output. Report Center keeps run history separate from the definition so you can tell whether the saved question changed or only its latest execution did.
{: .fc-page-lede }

## Review the definition before running

Open the report detail page and confirm the saved:

- scope;
- date range and comparison;
- measures and plan;
- description;
- schedule, where available.

A new run does not freeze a poorly defined question into a good one. Correct the definition before requesting an execution when the scope or period is wrong.

## Run the report now

Choose **Run Now** to request an immediate manual execution. The new run appears in **Run history**. Do not treat the previous successful output as the result of the new run while the latest execution is still pending or in progress.

The run contract distinguishes manual and scheduler-triggered executions. The visible trigger helps explain why a run occurred and which workflow should be investigated when it fails.

## Understand run states

| State | Meaning | Reader action |
| --- | --- | --- |
| Queued or pending | The run was accepted but has not started | Wait for processing; repeated clicks can create additional runs. |
| In progress | A worker is producing the report | Keep the definition stable and monitor the run rather than editing around it. |
| Completed | The run produced usable output | Open the output and confirm its definition and timestamps. |
| Failed | The run ended without usable output | Inspect the visible error or run context and use report troubleshooting. |
| No run | The definition has never executed | Run it before sharing or interpreting a result. |

Run history can include older successful output beside a newer failed run. Identify the exact run you are reading.

## Schedule recurring runs

Where the current Report Center exposes schedule controls, the supported choices are **None**, **Weekly**, and **Monthly**:

- **None** keeps execution manual.
- **Weekly** requests a recurring run at the product's fixed weekly cadence.
- **Monthly** requests a recurring run at the product's fixed monthly cadence.

Choose a cadence that lets the underlying trend accumulate and matches the review cycle. A schedule automates execution; it does not validate that the scope, measures, or comparison remain appropriate over time.

The public guide does not promise schedule controls in every deployment or role. Treat them as available only when the current workspace exposes them.

## Monitor the result

For a completed run, record:

- trigger type;
- start and completion time;
- duration;
- saved definition;
- latest successful output;
- any visible warnings or coverage limitations.

Use [Output and provenance](read-output-and-provenance.md) to distinguish the rendered narrative from measured or derived source values.

## When a run does not complete

A long queue can indicate worker demand, a stalled job, or a platform issue. A completed run with empty output can indicate an unusable selected context rather than a worker failure. Preserve the report ID, run ID, definition, status, and timestamps, then use [Report problems](../troubleshooting/reports.md).

Do not expose customer-sensitive output, credentials, or unrestricted logs while collecting evidence.
