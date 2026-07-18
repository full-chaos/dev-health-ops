# Diagnose no or incomplete data

Use this page when a workflow is empty, partially populated, unexpectedly stale, or missing a source.

## Check the selected context

1. Confirm the workspace, repository or team, and time window.
2. Remove optional filters that could exclude all matching work.
3. Compare the same scope with a wider period only to test coverage—not to change the question silently.

## Check source and processing state

Ask a workspace administrator to confirm that the source is connected, credentials are valid, synchronization has completed, and the selected scope is covered.

## Distinguish the state

| State | Meaning | Next step |
| --- | --- | --- |
| Measured zero | The supported calculation returned zero | Review the exact metric definition |
| Unavailable | The product has no supported value | Check prerequisites or applicability |
| Incomplete | Required input is missing | Review coverage and source status |
| Stale | A value exists but is older than expected | Check synchronization freshness |
| Delayed | Processing has not completed | Check job or report status |

## Escalate

Retain the workspace, scope, time window, source, visible status, and approximate time of the problem. Do not include credentials or customer-sensitive data.
