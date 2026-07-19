---
page_id: use-no-data
summary: Distinguish measured zero, unavailable, incomplete, stale, delayed, and empty results, then escalate at the correct boundary.
content_type: troubleshooting
owner: product-analytics
source_of_truth:
  - current product state behavior
  - src/dev_health_ops/api/services/investment.py
  - docs/user-guide/how-to-read-dev-health.md
applicability: current
lifecycle: active
---

# No or incomplete data

Use this guide when a view is empty, partially populated, missing an expected source, or does not include recent work. The first task is to identify **which data state the page is showing**. Measured zero, unavailable, incomplete, stale, delayed, and empty are different conditions and require different responses.
{: .fc-page-lede }

## Preserve the original question

Before changing anything, record the current:

- workspace;
- team, repository, or other scope;
- time window and comparison window;
- filters;
- page or report;
- visible status, message, or loading state;
- source/provider involved;
- approximate time.

Keep this context so that a wider period or removed filter remains a diagnostic test rather than silently becoming a different analytical question.

## Identify the visible state

| State | What it means | What it does not mean |
| --- | --- | --- |
| **Measured zero** | The supported calculation ran and produced zero for the selected context | It does not mean source coverage is complete unless the page confirms it. |
| **Unavailable** | The current workspace, role, source, or prerequisite cannot provide a supported result | It is not a numeric zero. |
| **Incomplete** | Some expected source data or processed input is missing | The visible partial result is not the full selected population. |
| **Stale** | A value exists, but its source or computation is older than the question requires | It is not evidence that recent work had no effect. |
| **Delayed** | Ingestion, computation, or report processing has not completed | Repeated refreshes do not necessarily accelerate the job. |
| **Empty response** | The query returned no usable rows or a required data surface is unavailable | It does not describe the work mix or performance of the selected scope. |

If the page exposes freshness, coverage, source, or status details, keep them with the result. A blank panel without a confirmed state should be treated as unknown until the checks below narrow it.

## Check the selected context

1. Confirm that the intended workspace is active.
2. Confirm the team, repository, or other scope.
3. Confirm the start and end of the time window.
4. Review every active filter and comparison choice.
5. Verify that the current role can access the route and underlying source.

Then run controlled tests:

- Remove one optional filter to see whether it excluded all matching work.
- Use a wider period only to test whether any historical data is present.
- Compare with another known-good repository or team in the same workspace.
- Return to the original scope and period after the test.

A result that appears only after widening the period usually points to coverage or freshness, not proof that the original period should be changed.

## Check source coverage and freshness

When the selected context is correct, continue to [Check synchronization status and freshness](../../admin/sync-and-coverage/status-and-freshness.md). The workspace administrator should verify:

- the expected provider connection exists and is authorized;
- the repository or project is included in the synchronized scope;
- the latest successful synchronization time is recent enough;
- coverage includes the entities and fields required by the view;
- a partial or failed sync is not being presented as complete.

For Investment, a result also depends on supported WorkUnits, effort values, categorization output, and the selected scope. Missing source material can reduce the result or leave no usable rows; it must not be interpreted as a deliberate zero distribution.

## Escalate runtime failures separately

If workspace configuration and coverage are correct but synchronization, ingestion, workers, queues, or stores are failing, continue to the operator runbooks:

- [Ingestion failure](../../operate/runbooks/ingestion-failure.md)
- [Worker or queue failure](../../operate/runbooks/worker-or-queue-failure.md)
- [Provider authentication failure](../../operate/runbooks/provider-authentication-failure.md)

The user-facing symptom and its preserved context should accompany the escalation. Do not send a product user to database or worker commands without an operator confirming that boundary.

## Collect safe evidence

Include:

- the preserved scope, period, filters, and page;
- the visible state or exact sanitized message;
- expected source and last known good result;
- whether a controlled filter or period test changed the result;
- synchronization freshness and coverage status, when an administrator checked them;
- timestamps and correlation or run identifiers exposed by the product.

Do not include credentials, tokens, customer-sensitive source text, full environment dumps, or unredacted logs in screenshots, issues, or support evidence.

## After the result returns

Before comparing the recovered result with an earlier one, confirm that the scope, period, filters, source coverage, and calculation version are equivalent. A result that became available after a backfill or coverage repair may not be directly comparable with a previously partial result.
