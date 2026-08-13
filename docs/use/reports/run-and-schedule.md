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

`ReportRun` is the authority for an execution. The API, scheduler, and workers use the same durable states in both execution runtimes.

| Visible state | Durable state | Owner and schedule effect | Reader action |
| --- | --- | --- | --- |
| Queued or pending | `pending` | The API or scheduler creates the run and handoff. This state does not advance a schedule. | Wait for processing; repeated manual clicks can create additional runs. |
| In progress | `running` | A worker holds a renewable execution lease. This state does not advance a schedule. | Monitor the same run ID. Do not create a replacement run for a stalled worker. |
| Completed | `success` | The worker stores the artifact and advances the saved report. A scheduled run also invalidates its next-due marker so the scheduler can compute the next cron time. | Open the output and confirm its definition and timestamps. |
| Failed | `failed` | The worker records the failure and advances the saved report. A scheduled run also invalidates its next-due marker. A bounded task retry or an explicit retry can reuse the same run ID. | Inspect the visible error or run context and use report troubleshooting. |
| Canceled | `canceled` | The cancellation path advances a scheduler-triggered report to the canceled occurrence and invalidates its next-due marker. A manual cancellation does not change a schedule marker. | Confirm that the intended occurrence was canceled. The next cron occurrence can still run. |
| No run | No row | The definition has never executed. | Run it before sharing or interpreting a result. |

Run history can include older successful output beside a newer failed run. Identify the exact run you are reading.

Workers renew a running execution lease while they query, render, and store the report. If a worker stops after it sets the run to `running`, another worker can reclaim the same run after the lease expires. The replacement gets a new fencing token, so the stopped worker cannot later store an artifact or failure over the new attempt. The system allows two expired-lease reclaims. A third expiry changes the run to `failed` with `report_run_execution_reclaim_exhausted` instead of retrying forever.

Operators can monitor `worker_report_run_lease_expired_total`. The `result="retrying"` label counts durable reclaims. The `result="failed"` label counts runs that reached the reclaim limit. A growing failed count needs investigation of worker health or report duration.

## Schedule recurring runs

Where the current Report Center exposes schedule controls, the supported choices are **None**, **Weekly**, and **Monthly**:

- **None** keeps execution manual.
- **Weekly** requests a recurring run at the product's fixed weekly cadence.
- **Monthly** requests a recurring run at the product's fixed monthly cadence.

Choose a cadence that lets the underlying trend accumulate and matches the review cycle. A schedule automates execution; it does not validate that the scope, measures, or comparison remain appropriate over time.

One recurring schedule can belong to only one saved report. A report can remain
unscheduled, and any number of reports can remain manual. To move a cadence to a
different report, detach it from the first report before you attach it to the
second. Cloning a report does not copy its recurring schedule.

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
